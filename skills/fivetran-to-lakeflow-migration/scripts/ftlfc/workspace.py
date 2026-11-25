"""Read-only lookups against the target Databricks workspace, via the CLI.

Used only to confirm that a UC connection the customer wants to reuse exists and
has the right type, that the bundle's destination catalogs exist, and that the
bundle's pipeline names and name are not already taken by another bundle.
Shelling out to the ``databricks`` CLI keeps the scripts free of an SDK
dependency and reuses whatever profile the customer already has.
"""

from __future__ import annotations

import json
import subprocess
from collections.abc import Callable
from typing import Any

_NOT_FOUND_MARKERS = ("RESOURCE_DOES_NOT_EXIST", "does not exist", "doesn't exist", "NOT_FOUND")

Runner = Callable[..., subprocess.CompletedProcess]


class WorkspaceError(RuntimeError):
    """The workspace could not be queried; distinct from 'connection not found'."""


def _run_cli(
    args: list[str], profile: str, runner: Runner, what: str
) -> subprocess.CompletedProcess:
    command = ["databricks", *args, "--profile", profile, "-o", "json"]
    try:
        return runner(command, capture_output=True, text=True, timeout=60, check=False)
    except FileNotFoundError as exc:
        raise WorkspaceError("the databricks CLI is not on PATH") from exc
    except subprocess.TimeoutExpired as exc:
        raise WorkspaceError(f"timed out looking up {what}") from exc


def _cli_lookup(
    kind: str, command: list[str], profile: str, runner: Runner
) -> Callable[[str], dict[str, Any] | None]:
    def lookup(name: str) -> dict[str, Any] | None:
        proc = _run_cli([*command, name], profile, runner, f"{kind} '{name}'")
        if proc.returncode == 0:
            return json.loads(proc.stdout)
        stderr = (proc.stderr or "").strip()
        if any(marker in stderr for marker in _NOT_FOUND_MARKERS):
            return None
        # Auth expiry and permission errors must not read as "does not exist".
        raise WorkspaceError(f"could not look up {kind} '{name}': {stderr[:300]}")

    return lookup


def cli_connection_lookup(
    profile: str, runner: Runner = subprocess.run
) -> Callable[[str], dict[str, Any] | None]:
    """Return a lookup that fetches a UC connection by name, or None if absent."""
    return _cli_lookup("connection", ["connections", "get"], profile, runner)


def cli_catalog_lookup(
    profile: str, runner: Runner = subprocess.run
) -> Callable[[str], dict[str, Any] | None]:
    """Return a lookup that fetches a UC catalog by name, or None if absent."""
    return _cli_lookup("catalog", ["catalogs", "get"], profile, runner)


def cli_pipeline_lookup(
    profile: str, runner: Runner = subprocess.run
) -> Callable[[str], dict[str, Any] | None]:
    """Return a lookup that fetches a pipeline by id, or None if absent."""
    return _cli_lookup("pipeline", ["pipelines", "get"], profile, runner)


def cli_workspace_path_lookup(
    profile: str, runner: Runner = subprocess.run
) -> Callable[[str], dict[str, Any] | None]:
    """Return a lookup that fetches a workspace path's status, or None if absent."""
    return _cli_lookup("workspace path", ["workspace", "get-status"], profile, runner)


def cli_pipeline_search(
    profile: str, runner: Runner = subprocess.run
) -> Callable[[str], list[dict[str, Any]]]:
    """Return a search listing pipelines whose name contains ``fragment``.

    ``_`` and ``%`` are LIKE wildcards, so callers re-check names exactly.
    """

    def search(fragment: str) -> list[dict[str, Any]]:
        what = f"pipelines named like '{fragment}'"
        proc = _run_cli(
            ["pipelines", "list-pipelines", "--filter", f"name LIKE '%{fragment}%'"],
            profile,
            runner,
            what,
        )
        if proc.returncode != 0:
            raise WorkspaceError(f"could not list {what}: {(proc.stderr or '').strip()[:300]}")
        return json.loads(proc.stdout or "[]") or []

    return search


def cli_table_list(
    profile: str, runner: Runner = subprocess.run
) -> Callable[[str, str], list[dict[str, Any]]]:
    """Return a listing of the tables in ``catalog.schema``; empty when the schema
    does not exist."""

    def list_tables(catalog: str, schema: str) -> list[dict[str, Any]]:
        what = f"tables in '{catalog}.{schema}'"
        proc = _run_cli(
            ["tables", "list", catalog, schema, "--omit-columns"], profile, runner, what
        )
        if proc.returncode == 0:
            return json.loads(proc.stdout or "[]") or []
        stderr = (proc.stderr or "").strip()
        if any(marker in stderr for marker in _NOT_FOUND_MARKERS):
            return []
        raise WorkspaceError(f"could not list {what}: {stderr[:300]}")

    return list_tables


def cli_current_user(profile: str, runner: Runner = subprocess.run) -> str:
    """Return the userName of the profile's identity."""
    proc = _run_cli(["current-user", "me"], profile, runner, "the current user")
    if proc.returncode != 0:
        raise WorkspaceError(
            f"could not look up the current user: {(proc.stderr or '').strip()[:300]}"
        )
    return json.loads(proc.stdout)["userName"]
