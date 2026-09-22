"""Emit the connections bundle: a secret scope plus a job that creates UC connections.

Bundles have no ``resources.connections`` type, so a Unity Catalog connection
cannot be declared next to the pipeline that uses it. What a bundle *can*
declare is a secret scope and a job. The generated connections bundle uses
both: the customer stores each credential in the scope, and the bootstrap job
reads them and creates (or updates) every connection that has a
non-interactive auth path. Credentials therefore never appear in a file.

It is a separate bundle, deployed and run before the ingestion bundle, because
the ingestion pipelines reference connections by name and so must not be
deployed until those connections exist.

Browser-OAuth-only connections are absent from the manifest: nothing can
create them without a human, so the ingestion bundle README lists them instead.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from .yaml_out import dump_yaml

#: Directory, relative to the ingestion bundle root, holding the connections bundle.
CONNECTIONS_DIR = "connections"
BOOTSTRAP_JOB_KEY = "bootstrap_connections"
SERVERLESS_ENVIRONMENT_VERSION = "5"

_TEMPLATE = Path(__file__).resolve().parent.parent / "templates" / "bootstrap_connections.py"
_INVALID_SCOPE_CHARS = re.compile(r"[^A-Za-z0-9._-]+")

# Option keys per connection type. A string is a fixed, non-secret default; None
# means the customer supplies the value through the secret scope. Secret key
# names are redacted by the API and could not be observed, so they follow the
# best available evidence in references/lakeflow-connect-api.md.
_OPTION_HINTS: dict[str, dict[str, str | None]] = {
    "SQLSERVER": {"host": None, "port": "1433", "user": None, "password": None},
    "POSTGRESQL": {"host": None, "port": "5432", "user": None, "password": None},
    "MYSQL": {"host": None, "port": "3306", "user": None, "password": None},
    "ORACLE": {
        "host": None,
        "port": "1521",
        "service_name": None,
        "user": None,
        "password": None,
    },
    "TERADATA": {"host": None, "port": "1025", "user": None, "password": None},
    "SALESFORCE": {
        "instance_url": None,
        "is_sandbox": "false",
        "client_id": None,
        "client_secret": None,
    },
    "SERVICENOW": {
        "instance_url": None,
        "client_id": None,
        "client_secret": None,
        "oauth_scope": "useraccount",
        "user": None,
        "password": None,
    },
    "WORKDAY_HCM": {"instance_url": None, "tenant_name": None, "user": None, "password": None},
    "WORKDAY_RAAS": {"user": None, "password": None},
    "NETSUITE": {
        "host": None,
        "port": "1708",
        "account_id": None,
        "role_id": None,
        "data_source": "NetSuite2.com",
    },
}

# Option shapes for a specific auth path, keyed by (connection type, credential
# type). Used in preference to _OPTION_HINTS when the plan names a preferred auth.
_AUTH_OPTION_HINTS: dict[tuple[str, str], dict[str, str | None]] = {
    ("SALESFORCE", "OAUTH_MTLS"): {
        "instance_url": None,
        "is_sandbox": "false",
        "client_id": None,
        "client_secret": None,
        "client_private_key": None,
        "client_certificate": None,
    },
}

_UNVERIFIED_SECRET_KEYS: frozenset[tuple[str, str]] = frozenset({("SALESFORCE", "OAUTH_MTLS")})

#: Options whose values span several lines, so they are read from a file.
MULTILINE_OPTIONS = frozenset({"client_private_key", "client_certificate", "options_json"})

#: Secret holding a JSON object of every option, for types whose keys are unknown.
OPTIONS_JSON = "options_json"


def secret_scope_name(bundle_name: str) -> str:
    """Secret scope names allow alphanumerics, dashes, underscores, and periods."""
    cleaned = _INVALID_SCOPE_CHARS.sub("-", f"{bundle_name}-credentials").strip("-")
    return cleaned[:128]


def secret_key(connection_name: str, option: str) -> str:
    return f"{connection_name}.{option}"


def scriptable_connections(plan: dict[str, Any]) -> list[dict[str, Any]]:
    """One entry per distinct connection the bootstrap job can create."""
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for item in plan["items"]:
        target = item["target"]
        name = target["connection_name"]
        if item["blockers"] or not target["connection_type"] or target["scriptable"] == "no":
            continue
        if name in seen:
            continue
        seen.add(name)
        out.append(_manifest_entry(item))
    return out


def _manifest_entry(item: dict[str, Any]) -> dict[str, Any]:
    target = item["target"]
    connection_type = target["connection_type"]
    preferred_auth = target.get("preferred_auth")
    hints = _option_hints(connection_type, preferred_auth)

    entry: dict[str, Any] = {
        "name": target["connection_name"],
        "connection_type": connection_type,
        "preferred_auth": preferred_auth,
        "fivetran_service": item["fivetran"]["service"],
        "options": {k: v for k, v in (hints or {}).items() if v is not None},
        "from_secrets": [k for k, v in (hints or {}).items() if v is None],
        "options_json_secret": hints is None,
        "unverified_secret_keys": (connection_type, preferred_auth) in _UNVERIFIED_SECRET_KEYS,
    }
    return entry


def _option_hints(connection_type: str, preferred_auth: str | None) -> dict[str, str | None] | None:
    if preferred_auth and (connection_type, preferred_auth) in _AUTH_OPTION_HINTS:
        return _AUTH_OPTION_HINTS[(connection_type, preferred_auth)]
    return _OPTION_HINTS.get(connection_type)


def required_secrets(entry: dict[str, Any]) -> list[str]:
    keys = [secret_key(entry["name"], option) for option in entry["from_secrets"]]
    if entry["options_json_secret"]:
        keys.append(secret_key(entry["name"], OPTIONS_JSON))
    return keys


def build_connections_bundle(
    plan: dict[str, Any], bundle_name: str, host: str | None
) -> dict[str, str]:
    """Files for the connections bundle, keyed by path relative to the ingestion bundle.

    Returns an empty mapping when no connection can be created by automation.
    """
    entries = scriptable_connections(plan)
    if not entries:
        return {}

    scope = secret_scope_name(bundle_name)
    root = f"{CONNECTIONS_DIR}/"
    target: dict[str, Any] = {"default": True}
    if host:
        target["workspace"] = {"host": host}

    return {
        f"{root}databricks.yml": dump_yaml(
            {
                "bundle": {"name": f"{bundle_name}-connections"},
                "include": ["resources/*.yml"],
                # Connections are metastore-level, so one deployment serves every
                # workspace attached to the metastore; there is no dev/prod split.
                "targets": {"default": target},
            }
        ),
        f"{root}resources/credentials.secret_scope.yml": dump_yaml(
            {"resources": {"secret_scopes": {"credentials": {"name": scope}}}}
        ),
        f"{root}resources/{BOOTSTRAP_JOB_KEY}.job.yml": dump_yaml(_bootstrap_job(bundle_name)),
        f"{root}connections.json": json.dumps({"connections": entries}, indent=2) + "\n",
        f"{root}src/bootstrap_connections.py": _TEMPLATE.read_text(),
        f"{root}scripts/put_secrets.sh": _put_secrets_script(entries, scope),
    }


def _bootstrap_job(bundle_name: str) -> dict[str, Any]:
    job = {
        "name": f"{bundle_name}-bootstrap-connections",
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "create_connections",
                "environment_key": "default",
                "spark_python_task": {
                    "python_file": "../src/bootstrap_connections.py",
                    "parameters": [
                        "--scope",
                        "${resources.secret_scopes.credentials.name}",
                        "--manifest",
                        "${workspace.file_path}/connections.json",
                    ],
                },
            }
        ],
        "environments": [
            {
                "environment_key": "default",
                "spec": {"environment_version": SERVERLESS_ENVIRONMENT_VERSION},
            }
        ],
    }
    return {"resources": {"jobs": {BOOTSTRAP_JOB_KEY: job}}}


def _put_secrets_script(entries: list[dict[str, Any]], scope: str) -> str:
    lines = [
        "#!/usr/bin/env bash",
        "# Store the credentials the bootstrap job reads to create each connection.",
        "#",
        "# Single-line values are prompted for; multi-line values (PEM keys, certificate",
        "# chains, JSON) are read from a file path you enter. Nothing is written to this",
        "# file or to shell history. Re-run to rotate a value, then re-run the job.",
        "#",
        "# Usage: ./scripts/put_secrets.sh [profile] [scope]",
        "",
        "set -euo pipefail",
        "",
        'PROFILE="${1:-DEFAULT}"',
        f'SCOPE="${{2:-{scope}}}"',
        "",
        "put() {",
        '  echo "Value for $1:"',
        '  databricks secrets put-secret "$SCOPE" "$1" --profile "$PROFILE"',
        "}",
        "",
        "put_file() {",
        "  local path",
        '  read -r -p "Path to the file holding $1: " path',
        '  path="${path/#\\~/$HOME}"',
        '  databricks secrets put-secret "$SCOPE" "$1" --profile "$PROFILE" < "$path"',
        "}",
        "",
    ]
    for entry in entries:
        auth = f", {entry['preferred_auth']}" if entry["preferred_auth"] else ""
        lines.append(f"echo '-- {entry['name']} ({entry['connection_type']}{auth})'")
        if entry["unverified_secret_keys"]:
            lines.append(
                "# Option key names for this auth path are inferred from UI labels; if the "
                "bootstrap job reports INVALID_PARAMETER_VALUE, the error names the keys it wants."
            )
        if entry["options_json_secret"]:
            lines.append(
                f"# {entry['connection_type']} option keys are not cataloged. Store a JSON object "
                "of every option, e.g. from a connection created once in the UI."
            )
        for key in required_secrets(entry):
            option = key.rsplit(".", 1)[1]
            command = "put_file" if option in MULTILINE_OPTIONS else "put"
            lines.append(f"{command} {key}")
        lines.append("")
    return "\n".join(lines) + "\n"
