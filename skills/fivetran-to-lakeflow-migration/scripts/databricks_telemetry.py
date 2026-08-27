#!/usr/bin/env python3
"""Stage 2.5 - query Databricks system tables for Lakeflow Connect telemetry.

    python3 databricks_telemetry.py --warehouse-id <ID> -o out/telemetry.json
    python3 databricks_telemetry.py --print-sql

Queries system.billing.list_prices, system.billing.usage, and
system.lakeflow.pipelines to extract:

- Actual DBU rates for serverless and gateway SKUs
- Actual Lakeflow Connect pipeline DBU consumption (if any pipelines exist)
- Estimated per-run duration and gateway hourly rate from billing data

The output replaces hardcoded RateCard defaults with observed values, so
compare_cost.py (stage 4) produces grounded numbers rather than pure
assumptions.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ftlfc.costs import RateCard
from ftlfc.system_tables import (
    build_list_prices_query,
    build_run_duration_query,
    build_usage_query,
    enrich_rate_card,
    fetch_lakeflow_usage,
    fetch_list_prices,
    fetch_run_durations,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--warehouse-id",
        help="Databricks SQL warehouse ID for executing queries",
    )
    parser.add_argument("--profile", help="Databricks CLI profile name")
    parser.add_argument(
        "--months",
        type=int,
        default=3,
        help="How many months of usage data to query (default: 3)",
    )
    parser.add_argument(
        "--print-sql",
        action="store_true",
        help="Print the SQL queries and exit without executing them",
    )
    parser.add_argument(
        "--rates",
        help=(
            "JSON object with base rate card overrides. "
            "System table values take priority over these."
        ),
    )
    parser.add_argument("-o", "--output", default="out/telemetry.json")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
        stream=sys.stderr,
    )

    if args.print_sql:
        _print_sql(args.months)
        return 0

    if not args.warehouse_id:
        print(
            "error: --warehouse-id is required unless --print-sql is used",
            file=sys.stderr,
        )
        return 1

    base_overrides = json.loads(args.rates) if args.rates else None
    base = RateCard(**base_overrides) if base_overrides else RateCard()

    print("Querying system.billing.list_prices...", file=sys.stderr)
    prices = fetch_list_prices(args.warehouse_id, args.profile)

    print("Querying system.billing.usage for Lakeflow pipelines...", file=sys.stderr)
    usage = fetch_lakeflow_usage(args.warehouse_id, args.profile, args.months)

    print("Querying run durations from billing segments...", file=sys.stderr)
    durations = fetch_run_durations(args.warehouse_id, args.profile, args.months)

    enriched, enrichment_log = enrich_rate_card(base, prices, usage, durations)

    output = {
        "rate_card": {
            "serverless_dbu_usd": enriched.serverless_dbu_usd,
            "gateway_dbu_usd": enriched.gateway_dbu_usd,
            "minutes_per_run": enriched.minutes_per_run,
            "gateway_dbu_per_hour": enriched.gateway_dbu_per_hour,
            "infra_uplift": enriched.infra_uplift,
        },
        "enrichment_log": enrichment_log,
        "raw_prices": prices,
        "lakeflow_usage_rows": len(usage),
        "run_duration_rows": len(durations),
    }

    dest = Path(args.output)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(output, indent=2) + "\n")

    _report(output, dest)
    return 0


def _print_sql(months: int) -> None:
    divider = "-" * 60
    print(f"-- List prices query\n{divider}")
    print(build_list_prices_query())
    print(f"\n-- Lakeflow usage query ({months} months)\n{divider}")
    print(build_usage_query(months))
    print(f"\n-- Run duration query ({months} months)\n{divider}")
    print(build_run_duration_query(months))


def _report(output: dict, dest: Path) -> None:
    print(f"\nWrote {dest}")
    rc = output["rate_card"]
    print("\n  Enriched rate card:")
    print(f"    Serverless DBU rate:    ${rc['serverless_dbu_usd']:.4f}/DBU")
    print(f"    Gateway DBU rate:       ${rc['gateway_dbu_usd']:.4f}/DBU")
    print(f"    Minutes per run:        {rc['minutes_per_run']:.1f} min")
    print(f"    Gateway DBU/hour:       {rc['gateway_dbu_per_hour']:.2f}")

    log = output["enrichment_log"]
    if log["grounded_fields"]:
        print(f"\n  Grounded from system tables ({len(log['grounded_fields'])} fields):")
        for field in log["grounded_fields"]:
            print(f"    * {field}")
        print(f"    Sources: {', '.join(log['sources'])}")

    if log["unchanged_fields"]:
        print(f"\n  Using defaults ({len(log['unchanged_fields'])} fields):")
        for field in log["unchanged_fields"]:
            print(f"    - {field}")


if __name__ == "__main__":
    raise SystemExit(main())
