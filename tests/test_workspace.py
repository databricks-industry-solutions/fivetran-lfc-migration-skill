"""Tests for the CLI-backed UC connection lookup."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "skills/fivetran-to-lakeflow-migration/scripts"
sys.path.insert(0, str(SCRIPTS))

from ftlfc.workspace import WorkspaceError, cli_connection_lookup


def _runner(returncode: int, stdout: str = "", stderr: str = ""):
    calls: list[list[str]] = []

    def run(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, returncode, stdout, stderr)

    run.calls = calls
    return run


def test_found_connection_is_parsed_and_uses_the_profile() -> None:
    run = _runner(0, stdout='{"name": "pd", "connection_type": "PAGERDUTY"}')
    info = cli_connection_lookup("prod", runner=run)("pd")
    assert info["connection_type"] == "PAGERDUTY"
    assert run.calls[0][:4] == ["databricks", "connections", "get", "pd"]
    assert "--profile" in run.calls[0] and "prod" in run.calls[0]


def test_missing_connection_returns_none() -> None:
    run = _runner(1, stderr="Error: Connection 'pd' does not exist.")
    assert cli_connection_lookup("prod", runner=run)("pd") is None


def test_auth_failure_is_an_error_not_a_missing_connection() -> None:
    run = _runner(1, stderr="Error: refresh token is invalid")
    with pytest.raises(WorkspaceError, match="refresh token"):
        cli_connection_lookup("prod", runner=run)("pd")


def test_missing_cli_is_an_error() -> None:
    def run(command, **kwargs):
        raise FileNotFoundError("databricks")

    with pytest.raises(WorkspaceError, match="not on PATH"):
        cli_connection_lookup("prod", runner=run)("pd")
