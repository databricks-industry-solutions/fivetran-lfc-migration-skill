"""Tests for reading Fivetran credentials from Databricks secrets."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "skills/fivetran-to-lakeflow-migration/scripts"
sys.path.insert(0, str(SCRIPTS))

import fivetran_discover
import preflight
from ftlfc import credentials
from ftlfc.credentials import (
    STOP_ON_READ_FAILURE,
    CliBackend,
    RuntimeBackend,
    SecretError,
    check_uc_secrets,
    load_credentials,
    parse_ref,
    read_secret,
    resolve_source,
)
from ftlfc.fivetran import FivetranAuthError, FivetranClient

FAKE_KEY = "fake-key-value"
FAKE_SECRET = "fake-secret-value"


class _Secrets:
    """Stands in for dbutils.secrets; records every lookup."""

    def __init__(self, values: dict, error: Exception | None = None) -> None:
        self.values = values
        self.error = error
        self.calls: list[dict] = []

    def get(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.values[tuple(kwargs.values())]


class _DbUtils:
    def __init__(self, secrets: _Secrets) -> None:
        self.secrets = secrets


class _MapBackend:
    def __init__(
        self,
        values: dict[str, str] | None = None,
        error: Exception | None = None,
        check_errors: dict[str, Exception] | None = None,
    ):
        self.values = values or {}
        self.error = error
        self.check_errors = check_errors or {}
        self.reads: list[str] = []
        self.checks: list[str] = []

    def read(self, ref):
        self.reads.append(ref.ref)
        if self.error is not None:
            raise self.error
        return self.values[ref.ref]

    def check_exists(self, ref):
        self.checks.append(ref.ref)
        if ref.ref in self.check_errors:
            raise self.check_errors[ref.ref]


def _runner(returncode: int, stdout: str = "", stderr: str = ""):
    calls: list[list[str]] = []

    def run(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, returncode, stdout, stderr)

    run.calls = calls
    return run


class TestRefs:
    @pytest.mark.parametrize(
        ("store", "ref", "parts"),
        [
            ("uc", "main.fivetran.api_key", ("main", "fivetran", "api_key")),
            ("unity_catalog", "main.fivetran.api_key", ("main", "fivetran", "api_key")),
            ("scope", "fivetran/api-key", ("fivetran", "api-key")),
            ("scope", "team.prod/api-key", ("team.prod", "api-key")),
        ],
    )
    def test_valid_refs_parse(self, store, ref, parts) -> None:
        parsed = parse_ref(store, ref)
        assert parsed.parts == parts
        assert parsed.store == ("scope" if store == "scope" else "uc")

    @pytest.mark.parametrize(
        ("store", "ref"),
        [
            ("uc", "fivetran.api_key"),
            ("uc", "main..api_key"),
            ("uc", "main.fivetran/api_key.x"),
            ("uc", "a.b.c.d"),
            ("scope", "fivetran"),
            ("scope", "fivetran/"),
            ("scope", "a/b/c"),
        ],
    )
    def test_malformed_refs_name_the_ref_and_the_expected_shape(self, store, ref) -> None:
        with pytest.raises(SecretError, match="malformed") as info:
            parse_ref(store, ref)
        assert ref in str(info.value)

    def test_unknown_store_is_rejected(self) -> None:
        with pytest.raises(SecretError, match="unknown secret store 'vault'"):
            parse_ref("vault", "a/b")


class TestResolveSource:
    def test_nothing_configured_off_compute_uses_env_vars(self) -> None:
        assert resolve_source(env={}) is None
        env = {"FIVETRAN_API_KEY": "k", "FIVETRAN_API_SECRET": "s"}
        assert resolve_source(env=env) is None

    def test_env_vars_win_over_the_default_scope_on_compute(self) -> None:
        env = {
            "DATABRICKS_RUNTIME_VERSION": "17.3",
            "FIVETRAN_API_KEY": "k",
            "FIVETRAN_API_SECRET": "s",
        }
        assert resolve_source(env=env) is None

    def test_on_compute_with_nothing_configured_uses_the_default_scope(self) -> None:
        source = resolve_source(env={"DATABRICKS_RUNTIME_VERSION": "17.3"})
        assert source.store == "scope"
        assert (source.key_ref.ref, source.secret_ref.ref) == (
            "fivetran/api-key",
            "fivetran/api-secret",
        )

    def test_scope_store_fills_in_default_refs(self) -> None:
        source = resolve_source(store="scope", env={})
        assert source.key_ref.parts == ("fivetran", "api-key")
        assert source.secret_ref.parts == ("fivetran", "api-secret")

    def test_uc_store_from_env_settings(self) -> None:
        env = {
            "FIVETRAN_SECRET_STORE": "unity_catalog",
            "FIVETRAN_API_KEY_REF": "main.fivetran.api_key",
            "FIVETRAN_API_SECRET_REF": "main.fivetran.api_secret",
            "DATABRICKS_CONFIG_PROFILE": "prod",
        }
        source = resolve_source(env=env)
        assert source.store == "uc"
        assert source.secret_ref.parts == ("main", "fivetran", "api_secret")
        assert source.profile == "prod"

    def test_flags_override_env_settings(self) -> None:
        env = {"FIVETRAN_SECRET_STORE": "scope"}
        source = resolve_source(store="uc", key_ref="c.s.key", secret_ref="c.s.secret", env=env)
        assert source.store == "uc"

    def test_store_is_inferred_from_refs(self) -> None:
        uc = resolve_source(key_ref="c.s.key", secret_ref="c.s.secret", env={})
        scope = resolve_source(key_ref="ft/key", secret_ref="ft/secret", env={})
        assert (uc.store, scope.store) == ("uc", "scope")

    def test_mixed_refs_without_a_store_are_rejected(self) -> None:
        with pytest.raises(SecretError, match="set --secret-store"):
            resolve_source(key_ref="c.s.key", secret_ref="ft/secret", env={})

    def test_uc_needs_both_refs(self) -> None:
        with pytest.raises(SecretError, match="both --api-key-ref and --api-secret-ref"):
            resolve_source(store="uc", key_ref="c.s.key", env={})

    def test_malformed_ref_fails_at_resolution(self) -> None:
        with pytest.raises(SecretError, match=re.escape("'c.key' is malformed")):
            resolve_source(store="uc", key_ref="c.key", secret_ref="c.s.secret", env={})


class TestReadSecret:
    def test_not_found_names_ref_store_and_reason(self) -> None:
        ref = parse_ref("uc", "main.fivetran.api_key")
        backend = _MapBackend(error=RuntimeError("RESOURCE_DOES_NOT_EXIST: Secret missing"))
        with pytest.raises(SecretError) as info:
            read_secret(ref, backend)
        assert str(info.value) == (
            "could not read uc secret 'main.fivetran.api_key': not found\n"
            "Stop and ask the user to fix this ref; do not look for the secret anywhere else."
        )

    def test_permission_denied_lists_the_grants(self) -> None:
        ref = parse_ref("uc", "main.fivetran.api_key")
        backend = _MapBackend(error=RuntimeError("PERMISSION_DENIED: User lacks READ SECRET"))
        with pytest.raises(SecretError, match="permission denied") as info:
            read_secret(ref, backend)
        assert "READ SECRET" in str(info.value) and "USE SCHEMA" in str(info.value)
        assert str(info.value).endswith(STOP_ON_READ_FAILURE)

    def test_empty_value_is_an_error(self) -> None:
        ref = parse_ref("scope", "fivetran/api-key")
        with pytest.raises(SecretError, match="is empty") as info:
            read_secret(ref, _MapBackend({"fivetran/api-key": "  "}))
        assert str(info.value).endswith(STOP_ON_READ_FAILURE)

    def test_backend_secret_errors_also_say_stop(self) -> None:
        ref = parse_ref("scope", "fivetran/api-key")
        with pytest.raises(SecretError, match="only be read on Databricks compute") as info:
            read_secret(ref, CliBackend(runner=pytest.fail))
        assert str(info.value).endswith(STOP_ON_READ_FAILURE)
        assert str(info.value).count(STOP_ON_READ_FAILURE) == 1

    def test_failure_does_not_try_other_locations(self) -> None:
        source = resolve_source(store="scope", env={})
        backend = _MapBackend(error=RuntimeError("Secret does not exist"))
        with pytest.raises(SecretError, match="'fivetran/api-key'"):
            load_credentials(source, backend)
        assert backend.reads == ["fivetran/api-key"]

    def test_values_never_appear_in_errors(self) -> None:
        source = resolve_source(store="scope", env={})
        backend = _MapBackend({"fivetran/api-key": FAKE_KEY})
        with pytest.raises(SecretError) as info:
            load_credentials(source, backend)
        assert FAKE_KEY not in str(info.value)

    def test_load_returns_both_values(self) -> None:
        source = resolve_source(store="uc", key_ref="c.s.k", secret_ref="c.s.v", env={})
        backend = _MapBackend({"c.s.k": FAKE_KEY, "c.s.v": FAKE_SECRET})
        assert load_credentials(source, backend) == (FAKE_KEY, FAKE_SECRET)


class TestRuntimeBackend:
    def test_scope_reads_through_dbutils(self) -> None:
        secrets = _Secrets({("fivetran", "api-key"): FAKE_KEY})
        backend = RuntimeBackend(dbutils=_DbUtils(secrets))
        assert backend.read(parse_ref("scope", "fivetran/api-key")) == FAKE_KEY
        assert secrets.calls == [{"scope": "fivetran", "key": "api-key"}]

    def test_uc_reads_through_dbutils_first(self) -> None:
        secrets = _Secrets({("main", "fivetran", "api_key"): FAKE_KEY})
        backend = RuntimeBackend(dbutils=_DbUtils(secrets), api_get=pytest.fail)
        assert backend.read(parse_ref("uc", "main.fivetran.api_key")) == FAKE_KEY
        assert secrets.calls == [{"catalog": "main", "schema": "fivetran", "key": "api_key"}]

    def test_uc_falls_back_to_the_rest_api_on_older_runtimes(self) -> None:
        secrets = _Secrets({}, error=TypeError("get() got an unexpected keyword 'catalog'"))
        paths: list[str] = []

        def api_get(path):
            paths.append(path)
            return {"full_name": "main.fivetran.api_key", "effective_value": FAKE_KEY}

        backend = RuntimeBackend(dbutils=_DbUtils(secrets), api_get=api_get)
        assert backend.read(parse_ref("uc", "main.fivetran.api_key")) == FAKE_KEY
        assert paths == ["/api/2.1/unity-catalog/secrets/main.fivetran.api_key"]

    def test_uc_existence_check_uses_the_metadata_path(self) -> None:
        paths: list[str] = []
        backend = RuntimeBackend(dbutils=pytest.fail, metadata_get=paths.append)
        backend.check_exists(parse_ref("uc", "main.fivetran.api_key"))
        assert paths == ["/api/2.1/unity-catalog/secrets/main.fivetran.api_key"]

    def test_rest_error_surfaces_with_its_reason(self) -> None:
        secrets = _Secrets({}, error=TypeError("old runtime"))

        def api_get(path):
            raise RuntimeError("PERMISSION_DENIED")

        backend = RuntimeBackend(dbutils=_DbUtils(secrets), api_get=api_get)
        with pytest.raises(SecretError, match=re.escape("uc secret 'c.s.k': permission denied")):
            read_secret(parse_ref("uc", "c.s.k"), backend)


class TestCliBackend:
    def test_uc_secret_is_read_with_include_value_and_the_profile(self) -> None:
        body = {"full_name": "main.fivetran.api_key", "effective_value": FAKE_KEY}
        run = _runner(0, stdout=json.dumps(body))
        value = CliBackend("prod", runner=run).read(parse_ref("uc", "main.fivetran.api_key"))
        assert value == FAKE_KEY
        command = run.calls[0]
        assert command[:3] == ["databricks", "api", "get"]
        assert (
            command[3] == "/api/2.1/unity-catalog/secrets/main.fivetran.api_key?include_value=true"
        )
        assert command[-2:] == ["--profile", "prod"]

    def test_cli_not_found_maps_to_not_found(self) -> None:
        run = _runner(1, stderr="Error: Secret 'main.fivetran.api_key' does not exist.")
        backend = CliBackend("prod", runner=run)
        with pytest.raises(SecretError, match="not found"):
            read_secret(parse_ref("uc", "main.fivetran.api_key"), backend)

    def test_missing_value_field_is_an_error(self) -> None:
        run = _runner(0, stdout=json.dumps({"full_name": "c.s.k"}))
        with pytest.raises(SecretError, match="returned no value"):
            CliBackend(runner=run).read(parse_ref("uc", "c.s.k"))

    def test_existence_check_never_requests_the_value(self) -> None:
        run = _runner(0, stdout=json.dumps({"full_name": "main.fivetran.api_key"}))
        CliBackend("prod", runner=run).check_exists(parse_ref("uc", "main.fivetran.api_key"))
        command = run.calls[0]
        assert command[:4] == [
            "databricks",
            "api",
            "get",
            "/api/2.1/unity-catalog/secrets/main.fivetran.api_key",
        ]
        assert not any("include_value" in part for part in command)

    def test_existence_check_surfaces_not_found(self) -> None:
        run = _runner(1, stderr="Error: Secret 'c.s.k' does not exist.")
        source = resolve_source(store="uc", key_ref="c.s.k", secret_ref="c.s.v", env={})
        problems = check_uc_secrets(source, CliBackend(runner=run))
        assert problems == ["uc secret 'c.s.k': not found", "uc secret 'c.s.v': not found"]

    def test_scope_secrets_need_databricks_compute(self) -> None:
        run = _runner(0)
        with pytest.raises(SecretError, match="only be read on Databricks compute"):
            CliBackend(runner=run).read(parse_ref("scope", "fivetran/api-key"))
        assert run.calls == []


class TestWiring:
    def test_client_repr_hides_credentials(self) -> None:
        client = FivetranClient(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
        assert FAKE_KEY not in repr(client) and FAKE_SECRET not in repr(client)

    def test_missing_credentials_error_explains_every_option(self, monkeypatch) -> None:
        monkeypatch.delenv("FIVETRAN_API_KEY", raising=False)
        monkeypatch.delenv("FIVETRAN_API_SECRET", raising=False)
        with pytest.raises(FivetranAuthError) as info:
            FivetranClient.from_env()
        message = str(info.value)
        assert "--secret-store uc" in message and "--secret-store scope" in message

    def test_discovery_reports_a_secret_error_and_exits_nonzero(
        self, monkeypatch, capsys, tmp_path
    ) -> None:
        monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
        code = fivetran_discover.main(
            [
                "--secret-store",
                "uc",
                "--api-key-ref",
                "main.fivetran",
                "--api-secret-ref",
                "main.fivetran.api_secret",
                "-o",
                str(tmp_path / "inventory.json"),
            ]
        )
        assert code == 1
        assert "'main.fivetran' is malformed" in capsys.readouterr().err

    def test_discovery_read_failure_tells_the_agent_to_stop(
        self, monkeypatch, capsys, tmp_path
    ) -> None:
        backend = _MapBackend(error=RuntimeError("RESOURCE_DOES_NOT_EXIST"))
        monkeypatch.setattr(credentials, "default_backend", lambda profile: backend)
        code = fivetran_discover.main(
            [
                "--secret-store",
                "uc",
                "--api-key-ref",
                "main.fivetran.api_key",
                "--api-secret-ref",
                "main.fivetran.api_secret",
                "-o",
                str(tmp_path / "inventory.json"),
            ]
        )
        err = capsys.readouterr().err
        assert code == 1
        assert "could not read uc secret 'main.fivetran.api_key': not found" in err
        assert STOP_ON_READ_FAILURE in err
        assert backend.reads == ["main.fivetran.api_key"]

    def test_preflight_checks_uc_refs_exist_without_reading_them(self) -> None:
        env = {
            "FIVETRAN_SECRET_STORE": "uc",
            "FIVETRAN_API_KEY_REF": "main.fivetran.api_key",
            "FIVETRAN_API_SECRET_REF": "main.fivetran.api_secret",
        }
        backend = _MapBackend()
        ok, detail = preflight.check_fivetran_creds(env, backend=backend)
        assert ok
        assert "uc secrets main.fivetran.api_key and main.fivetran.api_secret exist" in detail
        assert backend.checks == ["main.fivetran.api_key", "main.fivetran.api_secret"]
        assert backend.reads == []

    def test_preflight_reports_each_missing_or_denied_uc_ref(self) -> None:
        env = {
            "FIVETRAN_SECRET_STORE": "uc",
            "FIVETRAN_API_KEY_REF": "main.fivetran.api_key",
            "FIVETRAN_API_SECRET_REF": "main.fivetran.api_secret",
        }
        backend = _MapBackend(
            check_errors={
                "main.fivetran.api_key": RuntimeError("Secret does not exist"),
                "main.fivetran.api_secret": RuntimeError("PERMISSION_DENIED"),
            }
        )
        ok, detail = preflight.check_fivetran_creds(env, backend=backend)
        assert not ok
        assert "uc secret 'main.fivetran.api_key': not found" in detail
        assert "uc secret 'main.fivetran.api_secret': permission denied" in detail
        assert detail.endswith(STOP_ON_READ_FAILURE)
        assert backend.reads == []

    def test_existence_check_runs_against_the_given_profile(self) -> None:
        env = {"FIVETRAN_API_KEY_REF": "c.s.k", "FIVETRAN_API_SECRET_REF": "c.s.v"}
        source = resolve_source(env=env, profile="staging")
        run = _runner(0, stdout="{}")
        assert check_uc_secrets(source, CliBackend(source.profile, runner=run)) == []
        assert all(command[-2:] == ["--profile", "staging"] for command in run.calls)

    def test_preflight_leaves_scope_refs_unchecked_off_compute(self, monkeypatch) -> None:
        monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
        backend = _MapBackend()
        ok, detail = preflight.check_fivetran_creds(
            {"FIVETRAN_SECRET_STORE": "scope"}, backend=backend
        )
        assert ok
        assert "not checked" in detail and "only be read on Databricks compute" in detail
        assert backend.checks == [] and backend.reads == []

    def test_preflight_flags_a_malformed_ref(self) -> None:
        ok, detail = preflight.check_fivetran_creds(
            {
                "FIVETRAN_SECRET_STORE": "uc",
                "FIVETRAN_API_KEY_REF": "x",
                "FIVETRAN_API_SECRET_REF": "c.s.k",
            }
        )
        assert not ok and "malformed" in detail

    def test_preflight_still_accepts_env_vars(self) -> None:
        ok, detail = preflight.check_fivetran_creds(
            {"FIVETRAN_API_KEY": "k", "FIVETRAN_API_SECRET": "s"}
        )
        assert (ok, detail) == (True, "env vars set")
