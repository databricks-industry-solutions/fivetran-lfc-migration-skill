#!/usr/bin/env python3
"""Stage 2 - compare Fivetran cost against modelled Lakeflow Connect cost.

    python3 compare_billing.py --plan out/plan.json \
        --fivetran-annual-cost 480000 \
        --report out/billing.md

Every input is optional, but the comparison is only customer-presentable when
both sides are measured. Prefer --fivetran-annual-cost (from the invoice) and
--lfc-actual-monthly-cost (from system.billing.usage).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ftlfc.billing import compare  # noqa: E402
from ftlfc.catalog import CatalogError, load_pricing  # noqa: E402
from ftlfc.report import render_billing_report  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-p", "--plan", required=True, help="migration plan JSON from stage 3")
    parser.add_argument(
        "--fivetran-annual-cost",
        type=float,
        default=None,
        help="known annual Fivetran spend in USD (strongest input)",
    )
    parser.add_argument(
        "--fivetran-monthly-mar",
        type=float,
        default=None,
        help="monthly active rows, from the Platform Connector incremental_mar table",
    )
    parser.add_argument(
        "--lfc-actual-monthly-cost",
        type=float,
        default=None,
        help="observed monthly Lakeflow Connect spend in USD from system.billing.usage",
    )
    parser.add_argument("-o", "--output", default="out/billing.json")
    parser.add_argument("--report", default=None, help="also write a markdown report here")
    args = parser.parse_args(argv)

    plan = json.loads(Path(args.plan).read_text())

    try:
        pricing = load_pricing()
    except CatalogError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    comparison = compare(
        plan,
        fivetran_annual_cost=args.fivetran_annual_cost,
        fivetran_monthly_mar=args.fivetran_monthly_mar,
        lfc_actual_monthly_cost=args.lfc_actual_monthly_cost,
        pricing=pricing,
    )

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(comparison, indent=2) + "\n")
    print(f"Wrote {output}")

    if args.report:
        report_path = Path(args.report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(render_billing_report(comparison))
        print(f"Wrote {report_path}")

    print(f"  confidence: {comparison['confidence']}")
    for scenario in comparison["scenarios"]:
        print(
            f"  {scenario['scenario']:<12} "
            f"Fivetran ${scenario['fivetran_monthly']:,.0f}/mo vs "
            f"Lakeflow ${scenario['lakeflow_monthly']:,.0f}/mo "
            f"({scenario['annual_savings']:+,.0f}/yr)"
        )
    if not comparison["scenarios"]:
        print("  not enough input to compare; see the report for what is missing")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
