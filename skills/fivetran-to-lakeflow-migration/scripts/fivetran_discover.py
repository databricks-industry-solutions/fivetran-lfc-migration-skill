#!/usr/bin/env python3
"""Stage 1 - discover a customer's complete Fivetran setup.

Read-only. Writes a versioned inventory JSON that every later stage consumes.

    python3 fivetran_discover.py --secret-store uc \
        --api-key-ref main.fivetran.api_key --api-secret-ref main.fivetran.api_secret \
        --databricks-profile PROFILE --output out/inventory.json

On Databricks compute a workspace secret scope also works (--secret-store scope,
default refs fivetran/api-key and fivetran/api-secret). FIVETRAN_API_KEY and
FIVETRAN_API_SECRET are used when no secret store is configured.

Secret-looking config values are redacted before anything is written to disk.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ftlfc.credentials import SecretError, load_credentials, resolve_source
from ftlfc.fivetran import FivetranClient, FivetranError
from ftlfc.inventory import build_inventory


def add_credential_args(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group("Fivetran credentials")
    group.add_argument(
        "--secret-store",
        choices=("scope", "uc", "unity_catalog"),
        help="where the API key and secret live (env FIVETRAN_SECRET_STORE)",
    )
    group.add_argument(
        "--api-key-ref",
        help="catalog.schema.secret for uc, scope/key for scope (env FIVETRAN_API_KEY_REF)",
    )
    group.add_argument(
        "--api-secret-ref",
        help="catalog.schema.secret for uc, scope/key for scope (env FIVETRAN_API_SECRET_REF)",
    )
    group.add_argument(
        "--databricks-profile",
        help="CLI profile used to read uc secrets off Databricks compute",
    )


def build_client(args: argparse.Namespace) -> FivetranClient:
    source = resolve_source(
        store=args.secret_store,
        key_ref=args.api_key_ref,
        secret_ref=args.api_secret_ref,
        profile=args.databricks_profile,
    )
    if source is None:
        return FivetranClient.from_env()
    logging.info("reading Fivetran credentials from %s", source.describe())
    key, secret = load_credentials(source)
    return FivetranClient(api_key=key, api_secret=secret)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-o", "--output", default="out/inventory.json", help="path for the inventory JSON"
    )
    parser.add_argument(
        "-g",
        "--group",
        action="append",
        dest="groups",
        help="restrict discovery to a Fivetran group id (repeatable)",
    )
    parser.add_argument(
        "--connection",
        action="append",
        dest="connections",
        help="restrict discovery to a connection id or destination schema name (repeatable); "
        "use only when the customer chose to limit the scope",
    )
    parser.add_argument(
        "--no-schemas",
        action="store_true",
        help="skip per-table discovery (much faster, but the plan will lack detail)",
    )
    parser.add_argument(
        "--columns",
        action="store_true",
        help=(
            "optional: resolve the full column list per table, which adds primary keys "
            "for tables nobody customised. Not needed to plan managed connectors, which "
            "read keys from the source themselves. Costs one rate-limited request per table."
        ),
    )
    parser.add_argument(
        "--max-workers", type=int, default=8, help="parallel schema fetches (default 8)"
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    add_credential_args(parser)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
        stream=sys.stderr,
    )

    try:
        client = build_client(args)
        inventory = build_inventory(
            client,
            group_ids=args.groups,
            include_schemas=not args.no_schemas,
            include_columns=args.columns,
            max_workers=args.max_workers,
            connection_names=args.connections,
        )
    except (FivetranError, SecretError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    if args.connections and not inventory["connections"]:
        print(
            f"error: no connection matched {', '.join(args.connections)}; nothing was written",
            file=sys.stderr,
        )
        return 1

    destination = Path(args.output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(inventory, indent=2) + "\n")

    _report(inventory, destination)
    return 0


def _report(inventory: dict, destination: Path) -> None:
    summary = inventory["summary"]
    account = inventory["account"]

    print(f"Wrote {destination}")
    print(f"  account: {account.get('account_name')} ({account.get('key_type')} key)")
    print(
        f"  {summary['connections_total']} connections "
        f"({summary['connections_active']} active, {summary['connections_paused']} paused, "
        f"{summary['connections_broken']} broken)"
    )
    print(f"  {summary['distinct_services']} distinct connector types")
    print(
        f"  {summary['tables_enabled']} enabled tables "
        f"({summary['tables_history_mode']} in history/SCD2 mode)"
    )
    if summary["columns_hashed"]:
        print(f"  {summary['columns_hashed']} hashed columns to preserve")
    if summary["non_direct_networking"]:
        print(
            f"  {summary['non_direct_networking']} connections use private networking "
            "(PrivateLink/SSH/proxy) and need network design work"
        )

    for warning in inventory["warnings"]:
        print(f"  warning: {warning}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
