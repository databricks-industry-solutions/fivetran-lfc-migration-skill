"""Read-only lookups against the target Databricks workspace, via the CLI.

Used only to confirm that a UC connection the customer wants to reuse exists and
has the right type. Shelling out to the ``databricks`` CLI keeps the scripts free
of an SDK dependency and reuses whatever profile the customer already has.
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


def cli_connection_lookup(
    profile: str, runner: Runner = subprocess.run
) -> Callable[[str], dict[str, Any] | None]:
    """Return a lookup that fetches a UC connection by name, or None if absent."""

    def lookup(name: str) -> dict[str, Any] | None:
        command = ["databricks", "connections", "get", name, "--profile", profile, "-o", "json"]
        try:
            proc = runner(command, capture_output=True, text=True, timeout=60, check=False)
        except FileNotFoundError as exc:
            raise WorkspaceError("the databricks CLI is not on PATH") from exc
        except subprocess.TimeoutExpired as exc:
            raise WorkspaceError(f"timed out looking up connection '{name}'") from exc

        if proc.returncode == 0:
            return json.loads(proc.stdout)
        stderr = (proc.stderr or "").strip()
        if any(marker in stderr for marker in _NOT_FOUND_MARKERS):
            return None
        # Auth expiry and permission errors must not read as "does not exist".
        raise WorkspaceError(f"could not look up connection '{name}': {stderr[:300]}")

    return lookup
