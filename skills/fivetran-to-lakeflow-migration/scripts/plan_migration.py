#!/usr/bin/env python3
"""Stage 3 - map the Fivetran inventory onto Lakeflow Connect.

Produces the migration plan: the target shape for every connection, what must
happen before it can be built, and every reason the migration might not be
faithful.

    python3 plan_migration.py -i out/inventory.json -c main_prod -o out/plan.json
    python3 plan_migration.py -i out/inventory.json -c main_prod --mar out/mar.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ftlfc.mapping import (
    SQLSERVER_ARCH_CHOICES,
    SQLSERVER_ARCH_INTEGRATED,
    build_plan,
)
from ftlfc.workspace import WorkspaceError, cli_connection_lookup


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-i", "--inventory", type=Path, default=Path("out/inventory.json"))
    parser.add_argument("--mar", type=Path, help="MAR JSON from fivetran_mar.py")
    parser.add_argument("-c", "--catalog", required=True, help="destination Unity Catalog catalog")
    parser.add_argument(
        "--include-paused",
        action="store_true",
        help="also plan connections that are paused in Fivetran",
    )
    parser.add_argument(
        "--use-connection",
        action="append",
        default=[],
        metavar="KEY=NAME",
        help=(
            "reuse an existing UC connection instead of generating one. KEY is a Fivetran "
            "connection id or service (e.g. pagerduty); repeatable"
        ),
    )
    parser.add_argument(
        "--databricks-profile",
        help="CLI profile of the target workspace, to confirm reused connections exist",
    )
    parser.add_argument(
        "--sqlserver-arch",
        choices=SQLSERVER_ARCH_CHOICES,
        default=SQLSERVER_ARCH_INTEGRATED,
        help=(
            "SQL Server architecture: 'integrated' (default) generates a single "
            "integrated CDC pipeline with no gateway; 'gateway' generates the "
            "gateway-based pair on classic compute"
        ),
    )
    parser.add_argument("-o", "--output", default="out/plan.json")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    try:
        existing = _parse_use_connection(args.use_connection)
    except ValueError as exc:
        parser.error(str(exc))

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
        stream=sys.stderr,
    )

    if not args.inventory.exists():
        print(
            f"error: {args.inventory} not found. Run fivetran_discover.py first.", file=sys.stderr
        )
        return 1

    inventory = json.loads(args.inventory.read_text())
    mar = None
    if args.mar:
        if not args.mar.exists():
            print(f"error: {args.mar} not found", file=sys.stderr)
            return 1
        mar = json.loads(args.mar.read_text()).get("mar")

    lookup = (
        cli_connection_lookup(args.databricks_profile)
        if args.databricks_profile and existing
        else None
    )
    try:
        plan = build_plan(
            inventory,
            target_catalog=args.catalog,
            mar=mar,
            include_paused=args.include_paused,
            existing_connections=existing,
            lookup_connection=lookup,
            sqlserver_arch=args.sqlserver_arch,
        )
    except (ValueError, WorkspaceError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    destination = Path(args.output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(plan, indent=2) + "\n")

    _report(plan, destination)
    return 0


def _parse_use_connection(values: list[str]) -> dict[str, str]:
    existing: dict[str, str] = {}
    for value in values:
        key, sep, name = value.partition("=")
        if not sep or not key.strip() or not name.strip():
            raise ValueError(f"--use-connection expects KEY=NAME, got '{value}'")
        existing[key.strip()] = name.strip()
    return existing


_EFFORT_LABEL = {
    "low": "fully automatable",
    "medium": "automatable after source-side admin work and customer-supplied credentials",
    "high": "connection needs a one-time browser sign-in; pipeline still generated",
    "blocked": "no managed connector",
}


def _report(plan: dict, destination: Path) -> None:
    summary = plan["summary"]
    print(f"Wrote {destination}")
    print(
        f"  {summary['connections_migratable']} of {summary['connections_total']} connections "
        "will be generated in the bundle (the rest have blockers, listed below)"
    )
    manual = summary.get("connections_manual_sign_in", 0)
    if manual:
        print(f"  {manual} of those need a one-time browser sign-in to create the connection")
    reused = summary.get("connections_reused", 0)
    if reused:
        print(f"  {reused} reuse an existing UC connection")
    print(f"  {summary['tables_total']} tables ({summary['tables_scd2']} needing SCD type 2)")
    print(
        f"  {summary['jobs_required']} jobs and {summary['gateways_required']} gateways to create "
        "alongside the pipelines"
    )
    integrated = summary.get("integrated_cdc_pipelines", 0)
    if integrated:
        print(
            f"  {integrated} SQL Server integrated CDC pipeline(s) (no gateway; "
            "use --sqlserver-arch gateway for the gateway-based architecture)"
        )

    if summary["by_effort"]:
        print("  effort breakdown:")
        for effort in ("low", "medium", "high", "blocked"):
            count = summary["by_effort"].get(effort)
            if count:
                print(f"    {count} {_EFFORT_LABEL[effort]}")

    blockers = plan["blockers"]
    if blockers:
        print(f"\n  {len(blockers)} blocker(s):", file=sys.stderr)
        for entry in blockers:
            print(f"    [{entry['connection']}] {entry['message']}", file=sys.stderr)

    warnings = plan["warnings"]
    if warnings:
        print(f"\n  {len(warnings)} warning(s):", file=sys.stderr)
        for entry in warnings[:20]:
            print(f"    [{entry['connection']}] {entry['message']}", file=sys.stderr)
        if len(warnings) > 20:
            print(f"    ... and {len(warnings) - 20} more in {destination}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
