"""Tests for Genie Code preflight."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

SKILL_ROOT = Path(__file__).resolve().parents[1] / "skills" / "fivetran-to-lakeflow-migration"
PREFLIGHT = SKILL_ROOT / "scripts" / "preflight.py"


def run_preflight(*args: str) -> subprocess.CompletedProcess[str]:
    cmd = [sys.executable, str(PREFLIGHT), *args]
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


def test_preflight_json_ok():
    proc = run_preflight("--skill-dir", str(SKILL_ROOT), "--json")
    assert proc.returncode == 0
    data = json.loads(proc.stdout)
    assert data["ok"] is True
    assert data["skill_dir"] == str(SKILL_ROOT.resolve())
    assert Path(data["scripts"]).name == "scripts"
    assert "checks" in data
    assert data["checks"]["skill_md"]["ok"] is True


def test_preflight_export_shell():
    proc = run_preflight("--skill-dir", str(SKILL_ROOT), "--export")
    assert proc.returncode == 0
    assert "export CLAUDE_SKILL_DIR=" in proc.stdout
    assert "export SCRIPTS=" in proc.stdout
    assert "export FTLFC_OUT=" in proc.stdout


def test_preflight_resolves_from_script_location():
    proc = run_preflight("--json")
    assert proc.returncode == 0
    data = json.loads(proc.stdout)
    assert Path(data["skill_dir"]).name == "fivetran-to-lakeflow-migration"
