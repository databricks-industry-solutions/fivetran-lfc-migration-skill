"""Create or update the Unity Catalog connections the ingestion bundle references.

Reads ``connections.json``, pulls every customer-supplied option from the
bundle's secret scope, and creates each connection, or replaces the options of
one that already exists so that re-running after a credential rotation works.
Secret values are never printed.

Runs as a serverless job task (the environment ships databricks-sdk), or
locally with ``python bootstrap_connections.py --scope ... --manifest ...``
against the default Databricks CLI profile.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, NotFound

OPTIONS_JSON = "options_json"
CONNECTIONS_PATH = "/api/2.1/unity-catalog/connections"

SecretReader = Callable[[str], str]


def secret_key(connection_name: str, option: str) -> str:
    return f"{connection_name}.{option}"


def required_secrets(spec: dict[str, Any]) -> list[str]:
    keys = [secret_key(spec["name"], option) for option in spec.get("from_secrets", [])]
    if spec.get("options_json_secret"):
        keys.append(secret_key(spec["name"], OPTIONS_JSON))
    return keys


def resolve_options(spec: dict[str, Any], read_secret: SecretReader) -> dict[str, str]:
    """Merge fixed options with values read from the secret scope."""
    options = dict(spec.get("options") or {})
    for option in spec.get("from_secrets", []):
        options[option] = read_secret(secret_key(spec["name"], option))

    if spec.get("options_json_secret"):
        key = secret_key(spec["name"], OPTIONS_JSON)
        try:
            parsed = json.loads(read_secret(key))
        except json.JSONDecodeError as exc:
            # The message would echo part of the secret, so report position only.
            raise ValueError(f"secret {key} is not valid JSON (line {exc.lineno})") from None
        if not isinstance(parsed, dict):
            raise ValueError(f"secret {key} must hold a JSON object of connection options")
        options.update({str(k): str(v) for k, v in parsed.items()})
    return options


def apply_connection(client: WorkspaceClient, spec: dict[str, Any], options: dict[str, str]) -> str:
    """Create the connection, or replace the options of an existing one."""
    name = spec["name"]
    try:
        client.connections.get(name=name)
    except NotFound:
        # The SDK's ConnectionType enum lags the API, so newer connector types
        # would be rejected client-side; post the documented body directly.
        client.api_client.do(
            "POST",
            CONNECTIONS_PATH,
            body={
                "name": name,
                "connection_type": spec["connection_type"],
                "options": options,
                "read_only": True,
                "comment": f"Lakeflow Connect ingestion, migrated from Fivetran "
                f"{spec.get('fivetran_service') or 'connector'}",
            },
        )
        return "created"
    # PATCH replaces the options map rather than merging it, so send all of it.
    client.connections.update(name=name, options=options)
    return "updated"


def run(client: WorkspaceClient, manifest: dict[str, Any], scope: str) -> int:
    present = {secret.key for secret in client.dbutils.secrets.list(scope)}

    def read_secret(key: str) -> str:
        return client.dbutils.secrets.get(scope=scope, key=key)

    failures = 0
    for spec in manifest.get("connections", []):
        name = spec["name"]
        missing = [key for key in required_secrets(spec) if key not in present]
        if missing:
            print(
                f"SKIPPED {name}: secret scope '{scope}' is missing {', '.join(missing)}. "
                "Run scripts/put_secrets.sh, then re-run this job.",
                file=sys.stderr,
            )
            failures += 1
            continue
        try:
            action = apply_connection(client, spec, resolve_options(spec, read_secret))
        except (DatabricksError, ValueError) as exc:
            print(f"FAILED {name}: {exc}", file=sys.stderr)
            failures += 1
            continue
        print(f"{action} {name} ({spec['connection_type']})")

    total = len(manifest.get("connections", []))
    print(f"{total - failures} of {total} connection(s) ready.")
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", required=True, help="secret scope holding the credentials")
    parser.add_argument("--manifest", required=True, type=Path, help="path to connections.json")
    args = parser.parse_args(argv)

    manifest = json.loads(args.manifest.read_text())
    return run(WorkspaceClient(), manifest, args.scope)


if __name__ == "__main__":
    raise SystemExit(main())
