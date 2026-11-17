"""Fivetran API credentials from Databricks secrets.

Two stores hold the key and secret:

- ``scope``: a workspace secret scope, ref ``scope/key``. Only readable on
  Databricks compute through ``dbutils``; the CLI cannot return scope values.
- ``uc``: a Unity Catalog secret, ref ``catalog.schema.secret``. The reader
  needs READ SECRET on the secret plus USE CATALOG and USE SCHEMA on its parents.

A failed read names the ref, the store, and the reason, and never tries another
location: quietly picking up a different secret is how a stale key or another
account's key ends up in a migration. Secret values never appear in errors.
"""

from __future__ import annotations

import importlib
import json
import os
import subprocess
import urllib.parse
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

try:
    from databricks.sdk import WorkspaceClient
except ImportError:
    WorkspaceClient = None

STORE_ALIASES = {"scope": "scope", "uc": "uc", "unity_catalog": "uc"}
DEFAULT_SCOPE_REFS = ("fivetran/api-key", "fivetran/api-secret")

_UC_SECRETS_PATH = "/api/2.1/unity-catalog/secrets"
_NOT_FOUND_MARKERS = ("RESOURCE_DOES_NOT_EXIST", "NOT_FOUND", "does not exist", "404")
_PERMISSION_MARKERS = ("PERMISSION_DENIED", "403", "not authorized", "does not have")

Runner = Callable[..., subprocess.CompletedProcess]


class SecretError(RuntimeError):
    """A configured secret could not be read; the message never holds a value."""


@dataclass(frozen=True)
class SecretRef:
    store: str
    ref: str
    parts: tuple[str, ...]


@dataclass(frozen=True)
class CredentialSource:
    store: str
    key_ref: SecretRef
    secret_ref: SecretRef
    profile: str | None = None

    def describe(self) -> str:
        return f"{self.store} secrets {self.key_ref.ref} and {self.secret_ref.ref}"


class Backend(Protocol):
    def read(self, ref: SecretRef) -> str: ...

    def check_exists(self, ref: SecretRef) -> None:
        """Raise if a uc secret is missing or unreadable, without requesting its value."""
        ...


def normalise_store(store: str) -> str:
    try:
        return STORE_ALIASES[store.strip().lower()]
    except KeyError:
        raise SecretError(
            f"unknown secret store '{store}'; use 'scope' or 'uc' (unity_catalog)"
        ) from None


def infer_store(ref: str) -> str | None:
    if "/" in ref:
        return "scope"
    if len(ref.split(".")) == 3:
        return "uc"
    return None


def parse_ref(store: str, ref: str) -> SecretRef:
    store = normalise_store(store)
    ref = ref.strip()
    if store == "scope":
        parts, expected = tuple(ref.split("/")), "scope/key"
        valid = len(parts) == 2
    else:
        parts, expected = tuple(ref.split(".")), "catalog.schema.secret"
        valid = len(parts) == 3 and "/" not in ref
    if not valid or not all(parts):
        raise SecretError(f"{store} secret ref '{ref}' is malformed; expected {expected}")
    return SecretRef(store=store, ref=ref, parts=parts)


def failure_reason(exc: BaseException) -> str:
    text = str(exc)
    if any(marker in text for marker in _PERMISSION_MARKERS):
        return "permission denied (needs READ SECRET, plus USE CATALOG and USE SCHEMA for uc)"
    if any(marker in text for marker in _NOT_FOUND_MARKERS):
        return "not found"
    detail = text.strip().splitlines()[0][:200] if text.strip() else ""
    return f"{type(exc).__name__}: {detail}" if detail else type(exc).__name__


def read_secret(ref: SecretRef, backend: Backend) -> str:
    try:
        value = backend.read(ref)
    except SecretError:
        raise
    except Exception as exc:
        raise SecretError(
            f"could not read {ref.store} secret '{ref.ref}': {failure_reason(exc)}"
        ) from None
    if not value or not value.strip():
        raise SecretError(f"{ref.store} secret '{ref.ref}' is empty")
    return value.strip()


def _uc_path(ref: SecretRef) -> str:
    return f"{_UC_SECRETS_PATH}/{urllib.parse.quote(ref.ref, safe='.')}"


def _effective_value(body: Mapping[str, Any], ref: SecretRef) -> str:
    value = body.get("effective_value")
    if value is None:
        raise SecretError(f"uc secret '{ref.ref}' returned no value")
    return value


def _require_uc(ref: SecretRef) -> None:
    if ref.store != "uc":
        raise SecretError(f"only uc secrets can be checked without reading them, not '{ref.ref}'")


def on_databricks(env: Mapping[str, str] | None = None) -> bool:
    env = os.environ if env is None else env
    return bool(env.get("DATABRICKS_RUNTIME_VERSION"))


class RuntimeBackend:
    """Reads secrets on Databricks compute (Genie Code, notebooks, jobs)."""

    def __init__(
        self,
        dbutils: Any = None,
        api_get: Callable[[str], Mapping[str, Any]] | None = None,
        metadata_get: Callable[[str], Mapping[str, Any]] | None = None,
    ) -> None:
        self._dbutils = dbutils
        self._api_get = api_get
        self._metadata_get = metadata_get

    def _secrets(self) -> Any:
        if self._dbutils is None:
            # Imported here because importing databricks.sdk.runtime off Databricks
            # compute tries to open a remote session.
            self._dbutils = importlib.import_module("databricks.sdk.runtime").dbutils
        return self._dbutils.secrets

    def _rest_get(self, path: str) -> Mapping[str, Any]:
        if self._api_get is not None:
            return self._api_get(path)
        if WorkspaceClient is None:
            raise SecretError("databricks-sdk is not installed, so uc secrets cannot be read")
        return WorkspaceClient().api_client.do("GET", path, query={"include_value": "true"})

    def read(self, ref: SecretRef) -> str:
        if ref.store == "scope":
            scope, key = ref.parts
            return self._secrets().get(scope=scope, key=key)
        catalog, schema, key = ref.parts
        try:
            # Needs serverless environment 4+ or DBR 17.3 LTS+.
            return self._secrets().get(catalog=catalog, schema=schema, key=key)
        except Exception:
            return _effective_value(self._rest_get(_uc_path(ref)), ref)

    def check_exists(self, ref: SecretRef) -> None:
        _require_uc(ref)
        if self._metadata_get is not None:
            self._metadata_get(_uc_path(ref))
            return
        if WorkspaceClient is None:
            raise SecretError("databricks-sdk is not installed, so uc secrets cannot be checked")
        WorkspaceClient().api_client.do("GET", _uc_path(ref))


class CliBackend:
    """Reads Unity Catalog secrets off Databricks compute through the CLI."""

    def __init__(self, profile: str | None = None, runner: Runner = subprocess.run) -> None:
        self._profile = profile
        self._runner = runner

    def read(self, ref: SecretRef) -> str:
        if ref.store == "scope":
            raise SecretError(
                f"scope secret '{ref.ref}' can only be read on Databricks compute; run "
                "discovery in Genie Code or a notebook, or store the credentials as Unity "
                "Catalog secrets and pass --databricks-profile"
            )
        body = self._get(f"{_uc_path(ref)}?include_value=true", ref)
        return _effective_value(json.loads(body), ref)

    def check_exists(self, ref: SecretRef) -> None:
        _require_uc(ref)
        # Without include_value the response is metadata only; it is discarded anyway.
        self._get(_uc_path(ref), ref)

    def _get(self, path: str, ref: SecretRef) -> str:
        command = ["databricks", "api", "get", path, "-o", "json"]
        if self._profile:
            command.extend(["--profile", self._profile])
        try:
            proc = self._runner(command, capture_output=True, text=True, timeout=60, check=False)
        except FileNotFoundError:
            raise SecretError(
                f"could not read uc secret '{ref.ref}': the databricks CLI is not on PATH"
            ) from None
        except subprocess.TimeoutExpired:
            raise SecretError(f"could not read uc secret '{ref.ref}': timed out") from None
        if proc.returncode != 0:
            raise RuntimeError((proc.stderr or proc.stdout or "").strip())
        return proc.stdout


def default_backend(profile: str | None) -> Backend:
    return RuntimeBackend() if on_databricks() else CliBackend(profile)


def resolve_source(
    store: str | None = None,
    key_ref: str | None = None,
    secret_ref: str | None = None,
    profile: str | None = None,
    env: Mapping[str, str] | None = None,
) -> CredentialSource | None:
    """Work out where the credentials live; None means use FIVETRAN_API_KEY/SECRET.

    Flags win over FIVETRAN_SECRET_STORE / FIVETRAN_API_KEY_REF /
    FIVETRAN_API_SECRET_REF. With nothing configured, the env vars are used if
    set, and on Databricks compute the default scope refs otherwise.
    """
    env = os.environ if env is None else env
    store = store or env.get("FIVETRAN_SECRET_STORE") or None
    key_ref = key_ref or env.get("FIVETRAN_API_KEY_REF") or None
    secret_ref = secret_ref or env.get("FIVETRAN_API_SECRET_REF") or None
    profile = profile or env.get("DATABRICKS_CONFIG_PROFILE") or None

    if not (store or key_ref or secret_ref):
        if env.get("FIVETRAN_API_KEY") and env.get("FIVETRAN_API_SECRET"):
            return None
        if not on_databricks(env):
            return None
        store = "scope"

    if store is None:
        inferred = {infer_store(r) for r in (key_ref, secret_ref) if r}
        if len(inferred) != 1 or None in inferred:
            raise SecretError(
                "cannot tell which secret store the refs belong to; set --secret-store "
                "(scope or uc)"
            )
        store = inferred.pop()
    store = normalise_store(store)

    if store == "scope":
        key_ref = key_ref or DEFAULT_SCOPE_REFS[0]
        secret_ref = secret_ref or DEFAULT_SCOPE_REFS[1]
    elif not (key_ref and secret_ref):
        raise SecretError(
            "uc secrets need both --api-key-ref and --api-secret-ref (catalog.schema.secret)"
        )

    return CredentialSource(
        store=store,
        key_ref=parse_ref(store, key_ref),
        secret_ref=parse_ref(store, secret_ref),
        profile=profile,
    )


def check_uc_secrets(source: CredentialSource, backend: Backend | None = None) -> list[str]:
    """Return one problem per uc ref that is missing or unreadable; empty means both exist.

    Metadata only: never requests a value, so it is safe for preflight. Both refs
    are checked so one run reports everything that needs fixing.
    """
    backend = backend or default_backend(source.profile)
    problems: list[str] = []
    for ref in (source.key_ref, source.secret_ref):
        try:
            backend.check_exists(ref)
        except SecretError as exc:
            problems.append(str(exc))
        except Exception as exc:
            problems.append(f"uc secret '{ref.ref}': {failure_reason(exc)}")
    return problems


def load_credentials(source: CredentialSource, backend: Backend | None = None) -> tuple[str, str]:
    backend = backend or default_backend(source.profile)
    return read_secret(source.key_ref, backend), read_secret(source.secret_ref, backend)
