"""Tests for the bootstrap job that creates UC connections from a secret scope.

The script runs on Databricks serverless, where databricks-sdk is preinstalled.
It is not a dependency of this repo, so the three names the script imports are
registered as minimal stand-ins before it is loaded.
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from types import SimpleNamespace

import pytest

TEMPLATE = (
    Path(__file__).resolve().parents[1]
    / "skills/fivetran-to-lakeflow-migration/scripts/templates/bootstrap_connections.py"
)


class _DatabricksError(Exception):
    pass


class _NotFound(_DatabricksError):
    pass


def _load_bootstrap() -> types.ModuleType:
    sdk = types.ModuleType("databricks.sdk")
    sdk.WorkspaceClient = object
    errors = types.ModuleType("databricks.sdk.errors")
    errors.DatabricksError = _DatabricksError
    errors.NotFound = _NotFound
    stubs = {
        "databricks": types.ModuleType("databricks"),
        "databricks.sdk": sdk,
        "databricks.sdk.errors": errors,
    }
    saved = {name: sys.modules.get(name) for name in stubs}
    sys.modules.update(stubs)
    try:
        spec = importlib.util.spec_from_file_location("bootstrap_connections", TEMPLATE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        for name, previous in saved.items():
            if previous is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous
    return module


bootstrap = _load_bootstrap()

SECRET = "s3cr3t-value-that-must-not-leak"


class FakeClient:
    def __init__(self, secrets: dict[str, str], existing: set[str] | None = None) -> None:
        self._secrets = secrets
        self._existing = set(existing or ())
        self.created: list[dict] = []
        self.updated: list[tuple[str, dict]] = []
        self.fail_on: set[str] = set()

        self.dbutils = SimpleNamespace(
            secrets=SimpleNamespace(
                list=lambda scope: [SimpleNamespace(key=k) for k in self._secrets],
                get=lambda scope, key: self._secrets[key],
            )
        )
        self.connections = SimpleNamespace(get=self._get, update=self._update)
        self.api_client = SimpleNamespace(do=self._do)

    def _get(self, name: str) -> dict:
        if name not in self._existing:
            raise _NotFound(name)
        return {"name": name}

    def _update(self, name: str, options: dict) -> None:
        self.updated.append((name, options))

    def _do(self, method: str, path: str, body: dict) -> dict:
        assert method == "POST"
        assert path == "/api/2.1/unity-catalog/connections"
        if body["name"] in self.fail_on:
            raise _DatabricksError("INVALID_PARAMETER_VALUE: missing option client_certificate")
        self.created.append(body)
        return body


def _spec(name: str = "sf_conn", **overrides) -> dict:
    spec = {
        "name": name,
        "connection_type": "SALESFORCE",
        "fivetran_service": "salesforce",
        "options": {"is_sandbox": "false"},
        "from_secrets": ["client_id", "client_secret"],
        "options_json_secret": False,
    }
    spec.update(overrides)
    return spec


def _secrets(name: str = "sf_conn") -> dict[str, str]:
    return {f"{name}.client_id": "abc", f"{name}.client_secret": SECRET}


def test_creates_a_missing_connection_read_only_with_resolved_options() -> None:
    client = FakeClient(_secrets())
    assert bootstrap.run(client, {"connections": [_spec()]}, "scope") == 0
    (body,) = client.created
    assert body["connection_type"] == "SALESFORCE"
    assert body["read_only"] is True
    assert body["options"] == {"is_sandbox": "false", "client_id": "abc", "client_secret": SECRET}


def test_existing_connection_gets_its_full_options_replaced() -> None:
    client = FakeClient(_secrets(), existing={"sf_conn"})
    assert bootstrap.run(client, {"connections": [_spec()]}, "scope") == 0
    assert client.created == []
    assert client.updated == [
        ("sf_conn", {"is_sandbox": "false", "client_id": "abc", "client_secret": SECRET})
    ]


def test_missing_secrets_skip_the_connection_and_name_every_key(capsys) -> None:
    client = FakeClient({"sf_conn.client_id": "abc"})
    assert bootstrap.run(client, {"connections": [_spec()]}, "scope") == 1
    assert client.created == [] and client.updated == []
    err = capsys.readouterr().err
    assert "sf_conn.client_secret" in err
    assert "put_secrets.sh" in err


def test_one_failure_does_not_stop_the_others() -> None:
    secrets = {**_secrets("a"), **_secrets("b")}
    client = FakeClient(secrets)
    client.fail_on = {"a"}
    manifest = {"connections": [_spec("a"), _spec("b")]}
    assert bootstrap.run(client, manifest, "scope") == 1
    assert [c["name"] for c in client.created] == ["b"]


def test_options_json_secret_is_merged_over_fixed_options() -> None:
    spec = _spec(from_secrets=[], options_json_secret=True, options={"region": "us"})
    client = FakeClient({"sf_conn.options_json": '{"client_id": "x", "port": 443}'})
    assert bootstrap.run(client, {"connections": [spec]}, "scope") == 0
    assert client.created[0]["options"] == {"region": "us", "client_id": "x", "port": "443"}


def test_invalid_options_json_fails_without_echoing_the_value(capsys) -> None:
    spec = _spec(from_secrets=[], options_json_secret=True)
    client = FakeClient({"sf_conn.options_json": f'{{"password": "{SECRET}"'})
    assert bootstrap.run(client, {"connections": [spec]}, "scope") == 1
    out = capsys.readouterr()
    assert "not valid JSON" in out.err
    assert SECRET not in out.err + out.out


@pytest.mark.parametrize("existing", [set(), {"sf_conn"}])
def test_secret_values_are_never_printed(capsys, existing) -> None:
    client = FakeClient(_secrets(), existing=existing)
    bootstrap.run(client, {"connections": [_spec()]}, "scope")
    out = capsys.readouterr()
    assert SECRET not in out.out + out.err
