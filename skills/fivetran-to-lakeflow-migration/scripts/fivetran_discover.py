#!/usr/bin/env python3
"""Stage 1 - discover a customer's complete Fivetran setup.

Read-only. Writes a versioned inventory JSON that every later stage consumes.

    export FIVETRAN_API_KEY=... FIVETRAN_API_SECRET=...
    python3 fivetran_discover.py --output out/inventory.json

Secret-looking config values are redacted before anything is written to disk.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ftlfc.fivetran import FivetranClient, FivetranError
from ftlfc.inventory import build_inventory


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
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
        stream=sys.stderr,
    )

    try:
        client = FivetranClient.from_env()
        inventory = build_inventory(
            client,
            group_ids=args.groups,
            include_schemas=not args.no_schemas,
            include_columns=args.columns,
            max_workers=args.max_workers,
        )
    except FivetranError as exc:
        print(f"error: {exc}", file=sys.stderr)
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
