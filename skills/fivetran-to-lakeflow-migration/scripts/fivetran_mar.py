#!/usr/bin/env python3
"""Stage 2 - collect Fivetran MAR and spend from the Platform Connector.

No Fivetran REST endpoint exposes MAR or cost, so this reads the Platform
Connector tables in the customer's destination warehouse.

    # Destination is Databricks: query it directly.
    python3 fivetran_mar.py --warehouse-id abc123 --profile prod -o out/mar.json

    # Destination is Snowflake/BigQuery/Redshift: hand the SQL over, ingest the export.
    python3 fivetran_mar.py --print-sql
    python3 fivetran_mar.py --csv mar_export.csv -o out/mar.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ftlfc.mar import (
    PLATFORM_SCHEMAS,
    MarError,
    aggregate,
    build_cost_query,
    build_mar_query,
    detect_platform_schema,
    load_csv,
    load_rows,
    run_databricks_query,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--csv", type=Path, help="MAR export produced by --print-sql")
    source.add_argument(
        "--warehouse-id", help="Databricks SQL warehouse id, when the destination is Databricks"
    )
    source.add_argument(
        "--print-sql",
        action="store_true",
        help="print the queries to run against the destination warehouse, then exit",
    )
    parser.add_argument("--profile", help="Databricks CLI profile")
    parser.add_argument("--catalog", help="Unity Catalog catalog holding the Fivetran schema")
    parser.add_argument(
        "--schema",
        help=f"Platform Connector schema (default: autodetect {' or '.join(PLATFORM_SCHEMAS)})",
    )
    parser.add_argument("--months", type=int, default=6, help="months of history (default 6)")
    parser.add_argument("-o", "--output", default="out/mar.json")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
        stream=sys.stderr,
    )

    if args.print_sql:
        schema = args.schema or PLATFORM_SCHEMAS[0]
        print(f"-- Fivetran MAR, {args.months} months. Export the result as CSV.")
        print(build_mar_query(schema, args.months))
        print()
        print("-- Fivetran spend. Exactly one branch returns rows (dollars or credits).")
        print(build_cost_query(schema, args.months))
        return 0

    try:
        records, cost_rows = _load(args)
    except MarError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if not records:
        print(
            "error: no paid MAR rows found. The Platform Connector may be newly created, "
            "or the account may be on a plan where all usage is free-tier.",
            file=sys.stderr,
        )
        return 1

    summary = aggregate(records)
    payload = {
        "source": "fivetran-platform-connector",
        "months_requested": args.months,
        "mar": summary,
        "spend": cost_rows,
        "records": [r.__dict__ for r in records],
    }

    destination = Path(args.output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(payload, indent=2, default=str) + "\n")

    _report(destination, summary, cost_rows)
    return 0


def _load(args: argparse.Namespace) -> tuple[list, list]:
    if args.csv:
        with args.csv.open() as handle:
            return load_csv(handle), []

    if not args.warehouse_id:
        raise MarError(
            "Choose a source: --warehouse-id to query Databricks, --csv to ingest an "
            "export, or --print-sql to get the queries."
        )

    schema = args.schema or detect_platform_schema(args.warehouse_id, args.profile, args.catalog)
    if args.schema and args.catalog:
        schema = f"{args.catalog}.{args.schema}"

    records = load_rows(
        run_databricks_query(build_mar_query(schema, args.months), args.warehouse_id, args.profile)
    )
    try:
        cost_rows = run_databricks_query(
            build_cost_query(schema, args.months), args.warehouse_id, args.profile
        )
    except MarError as exc:
        logging.warning("Spend tables unavailable (%s); continuing with MAR only", exc)
        cost_rows = []
    return records, cost_rows


def _report(destination: Path, summary: dict, cost_rows: list) -> None:
    print(f"Wrote {destination}")
    print(f"  {summary['month_count']} months: {', '.join(summary['months'])}")
    print(f"  {summary['total_mar']:,} total paid MAR")
    print(f"  {summary['latest_month_mar']:,} MAR in {summary['latest_month']} (latest)")
    print(f"  {summary['mean_monthly_mar']:,} mean monthly MAR")

    top = list(summary["by_connection"].items())[:5]
    if top:
        print("  top connections by MAR:")
        for name, mar in top:
            share = mar / summary["total_mar"] * 100 if summary["total_mar"] else 0
            print(f"    {name}: {mar:,} ({share:.1f}%)")

    billed = [r for r in cost_rows if r.get("cost_usd") is not None]
    credits = [r for r in cost_rows if r.get("credits") is not None]
    if billed:
        total = sum(float(r["cost_usd"]) for r in billed)
        print(f"  reported spend: ${total:,.2f} across {len(billed)} destination-months")
    if credits:
        print(f"  credit-billed account: {len(credits)} destination-months of credit usage")
    if not cost_rows:
        print(
            "  note: no spend rows. Cost comparison will rely on the modelled per-MAR rate "
            "rather than the customer's actual invoice.",
            file=sys.stderr,
        )


if __name__ == "__main__":
    raise SystemExit(main())
