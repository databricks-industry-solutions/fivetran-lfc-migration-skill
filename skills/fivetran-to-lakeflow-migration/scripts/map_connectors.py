#!/usr/bin/env python3
"""Stage 3 - map a Fivetran inventory onto Lakeflow Connect and emit a plan.

    python3 map_connectors.py --inventory out/inventory.json \
        --target-catalog main --output out/plan.json --report out/plan.md

The plan is meant to be reviewed and hand-edited before stage 4 generates a
bundle. Exits 2 when any connection is blocked, so a caller can gate on it.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ftlfc.catalog import CatalogError, load_catalog  # noqa: E402
from ftlfc.mapping import build_plan  # noqa: E402
from ftlfc.report import render_plan_report  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-i", "--inventory", required=True, help="inventory JSON from stage 1")
    parser.add_argument(
        "--target-catalog", required=True, help="Unity Catalog catalog to land data in"
    )
    parser.add_argument(
        "--target-schema",
        default=None,
        help="override the destination schema for every table (default: reuse Fivetran's)",
    )
    parser.add_argument(
        "--name-prefix", default="lfc", help="prefix for generated resource names (default lfc)"
    )
    parser.add_argument(
        "--include-paused",
        action="store_true",
        help="also plan connections that are paused in Fivetran",
    )
    parser.add_argument("-o", "--output", default="out/plan.json", help="path for the plan JSON")
    parser.add_argument("--report", default=None, help="also write a markdown report here")
    args = parser.parse_args(argv)

    inventory = json.loads(Path(args.inventory).read_text())

    try:
        catalog_data = load_catalog()
    except CatalogError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    plan = build_plan(
        inventory,
        target_catalog=args.target_catalog,
        target_schema=args.target_schema,
        name_prefix=args.name_prefix,
        include_paused=args.include_paused,
        catalog_data=catalog_data,
    )

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(plan, indent=2) + "\n")
    print(f"Wrote {output}")

    if args.report:
        report_path = Path(args.report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(render_plan_report(plan))
        print(f"Wrote {report_path}")

    summary = plan["summary"]
    print(
        f"  {summary['connections_planned']} connections planned, "
        f"{summary['connections_migratable']} migratable, "
        f"{summary['connections_blocked']} blocked"
    )
    print(
        f"  {summary['tables_planned']} tables "
        f"({summary['tables_scd_type_2']} SCD type 2), "
        f"{summary['gateways_required']} gateway(s) required"
    )
    if summary["manual_step_count"]:
        print(f"  {summary['manual_step_count']} manual step(s) flagged - read the report")

    return 2 if summary["connections_blocked"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
