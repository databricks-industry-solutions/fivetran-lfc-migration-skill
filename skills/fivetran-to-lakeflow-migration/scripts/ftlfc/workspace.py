"""Read-only lookups against the target Databricks workspace, via the CLI.

Used only to confirm that a UC connection the customer wants to reuse exists and
has the right type, and that the bundle's destination catalogs exist. Shelling
out to the ``databricks`` CLI keeps the scripts free of an SDK dependency and
reuses whatever profile the customer already has.
"""

from __future__ import annotations

import json
import subprocess
from collections.abc import Callable
from typing import Any

_NOT_FOUND_MARKERS = ("RESOURCE_DOES_NOT_EXIST", "does not exist", "NOT_FOUND")

Runner = Callable[..., subprocess.CompletedProcess]


class WorkspaceError(RuntimeError):
    """The workspace could not be queried; distinct from 'connection not found'."""


def _cli_lookup(
    kind: str, command_group: str, profile: str, runner: Runner
) -> Callable[[str], dict[str, Any] | None]:
    def lookup(name: str) -> dict[str, Any] | None:
        command = ["databricks", command_group, "get", name, "--profile", profile, "-o", "json"]
        try:
            proc = runner(command, capture_output=True, text=True, timeout=60, check=False)
        except FileNotFoundError as exc:
            raise WorkspaceError("the databricks CLI is not on PATH") from exc
        except subprocess.TimeoutExpired as exc:
            raise WorkspaceError(f"timed out looking up {kind} '{name}'") from exc

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
    return _cli_lookup("connection", "connections", profile, runner)


def cli_catalog_lookup(
    profile: str, runner: Runner = subprocess.run
) -> Callable[[str], dict[str, Any] | None]:
    """Return a lookup that fetches a UC catalog by name, or None if absent."""
    return _cli_lookup("catalog", "catalogs", profile, runner)
