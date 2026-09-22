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

from ftlfc.mapping import build_plan


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
    parser.add_argument("-o", "--output", default="out/plan.json")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

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

    plan = build_plan(
        inventory,
        target_catalog=args.catalog,
        mar=mar,
        include_paused=args.include_paused,
    )

    destination = Path(args.output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(plan, indent=2) + "\n")

    _report(plan, destination)
    return 0


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
        "have a managed Lakeflow Connect connector and will be generated in the bundle"
    )
    manual = summary.get("connections_manual_sign_in", 0)
    if manual:
        print(f"  {manual} of those need a one-time browser sign-in to create the connection")
    print(f"  {summary['tables_total']} tables ({summary['tables_scd2']} needing SCD type 2)")
    print(
        f"  {summary['jobs_required']} jobs and {summary['gateways_required']} gateways to create "
        "alongside the pipelines"
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
