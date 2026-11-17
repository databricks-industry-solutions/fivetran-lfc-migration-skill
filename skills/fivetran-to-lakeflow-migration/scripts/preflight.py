#!/usr/bin/env python3
"""Preflight for Claude Code and Genie Code runtimes.

Resolves skill paths (Genie Code does not set CLAUDE_SKILL_DIR), checks
dependencies, and verifies credentials the migration stages need.

Exit 0 prints JSON with exportable paths; exit 1 on hard blockers.

    python3 scripts/preflight.py --json
    python3 scripts/preflight.py --export   # shell assignments for bash
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ftlfc.credentials import (
    STOP_ON_READ_FAILURE,
    Backend,
    SecretError,
    check_uc_secrets,
    on_databricks,
    resolve_source,
)


def resolve_skill_dir(explicit: str | None) -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()
    env = os.environ.get("CLAUDE_SKILL_DIR")
    if env:
        return Path(env).expanduser().resolve()
    # This file lives at <skill>/scripts/preflight.py
    return Path(__file__).resolve().parent.parent


def check_pyyaml() -> tuple[bool, str]:
    if importlib.util.find_spec("yaml") is not None:
        return True, "present"
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-q", "PyYAML>=6.0.1"],
            check=True,
            capture_output=True,
            text=True,
        )
        return True, "installed"
    except subprocess.CalledProcessError as exc:
        tail = (exc.stderr or exc.stdout or "").strip()[-400:]
        return False, f"pip install failed: {tail}"


def check_fivetran_creds(
    env: Mapping[str, str] | None = None,
    profile: str | None = None,
    backend: Backend | None = None,
) -> tuple[bool, str]:
    """Check where credentials are configured without reading any secret value.

    For uc secrets this also confirms each one exists and is readable, through a
    metadata-only lookup. Scope secrets are checked for configuration only.
    """
    env = os.environ if env is None else env
    try:
        source = resolve_source(env=env, profile=profile)
    except SecretError as exc:
        return False, str(exc)
    if source is not None and source.store == "uc":
        problems = check_uc_secrets(source, backend)
        if problems:
            return False, f"{'; '.join(problems)}\n{STOP_ON_READ_FAILURE}"
        return True, f"{source.describe()} exist (values read at discovery)"
    if source is not None:
        note = "" if on_databricks(env) else "; scopes can only be read on Databricks compute"
        return True, f"{source.describe()} configured, not checked (read at discovery{note})"
    if env.get("FIVETRAN_API_KEY") and env.get("FIVETRAN_API_SECRET"):
        return True, "env vars set"
    return False, (
        "set FIVETRAN_SECRET_STORE with FIVETRAN_API_KEY_REF and FIVETRAN_API_SECRET_REF "
        "(or pass --secret-store and the ref flags to discovery), set FIVETRAN_API_KEY "
        "and FIVETRAN_API_SECRET, or use Fivetran MCP"
    )


def check_databricks_cli(profile: str | None) -> tuple[bool, str]:
    if shutil.which("databricks") is None:
        return False, "databricks CLI not on PATH"
    cmd = ["databricks", "version"]
    if profile:
        cmd.extend(["--profile", profile])
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as exc:
        return False, (exc.stderr or exc.stdout or "databricks version failed").strip()[:200]
    return True, f"cli present (profile={profile or 'default'})"


def build_result(
    skill_dir: Path,
    out_dir: Path,
    profile: str | None,
    install_deps: bool,
) -> dict:
    scripts = skill_dir / "scripts"
    checks: dict[str, dict] = {}

    if install_deps:
        ok, detail = check_pyyaml()
        checks["pyyaml"] = {"ok": ok, "detail": detail}
    else:
        ok = importlib.util.find_spec("yaml") is not None
        checks["pyyaml"] = {
            "ok": ok,
            "detail": "present" if ok else "missing — run preflight with --install-deps",
        }

    ft_ok, ft_detail = check_fivetran_creds(profile=profile)
    checks["fivetran_credentials"] = {"ok": ft_ok, "detail": ft_detail}

    db_ok, db_detail = check_databricks_cli(profile)
    checks["databricks_cli"] = {"ok": db_ok, "detail": db_detail}

    skill_md = skill_dir / "SKILL.md"
    checks["skill_md"] = {
        "ok": skill_md.is_file(),
        "detail": str(skill_md),
    }

    hard_blockers = [
        name
        for name, c in checks.items()
        if not c["ok"] and name in ("pyyaml", "skill_md")
    ]
    # Fivetran creds and databricks CLI are stage-specific — warn, do not hard-fail preflight.

    return {
        "ok": len(hard_blockers) == 0,
        "runtime": "genie-code" if "DATABRICKS_RUNTIME_VERSION" in os.environ else "local",
        "skill_dir": str(skill_dir),
        "scripts": str(scripts),
        "references": str(skill_dir / "references"),
        "fixtures": str(skill_dir / "fixtures"),
        "out_dir": str(out_dir),
        "profile": profile,
        "checks": checks,
        "stages": {
            "discover": {"needs": ["fivetran_credentials"], "optional": []},
            "measure": {"needs": ["databricks_cli"], "optional": ["fivetran_credentials"]},
            "map": {"needs": [], "optional": []},
            "consumption": {"needs": [], "optional": ["databricks_cli"]},
            "generate": {"needs": [], "optional": []},
            "deploy": {"needs": ["databricks_cli"], "optional": []},
        },
        "export": {
            "CLAUDE_SKILL_DIR": str(skill_dir),
            "SCRIPTS": str(scripts),
            "FTLFC_OUT": str(out_dir),
        },
    }


def print_export(result: dict) -> None:
    for key, value in result["export"].items():
        print(f"export {key}='{value}'")
    if result.get("profile"):
        print(f"export DATABRICKS_PROFILE='{result['profile']}'")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--skill-dir",
        help=(
            "absolute path to the skill folder "
            "(default: CLAUDE_SKILL_DIR or infer from script location)"
        ),
    )
    parser.add_argument(
        "-o",
        "--out-dir",
        default=None,
        help="working directory for stage artifacts (default: <skill>/out)",
    )
    parser.add_argument("--profile", help="Databricks CLI profile for stage 2/6 checks")
    parser.add_argument(
        "--install-deps",
        action="store_true",
        help="pip install PyYAML from skill requirements.txt when missing",
    )
    parser.add_argument("--json", action="store_true", help="print JSON result (default)")
    parser.add_argument(
        "--export",
        action="store_true",
        help="print shell export statements for CLAUDE_SKILL_DIR, SCRIPTS, FTLFC_OUT",
    )
    args = parser.parse_args(argv)

    skill_dir = resolve_skill_dir(args.skill_dir)
    out_dir = Path(args.out_dir or skill_dir / "out").expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.install_deps:
        req = skill_dir / "requirements.txt"
        if req.is_file():
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "-q", "-r", str(req)],
                check=False,
            )

    result = build_result(skill_dir, out_dir, args.profile, args.install_deps)

    if args.export:
        print_export(result)
    else:
        print(json.dumps(result, indent=2))

    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
