#!/usr/bin/env python3
"""Record Fivetran spend and consumption for the Databricks account team.

The skill never prices Lakeflow Connect, compares costs, or states a saving.
It records what the customer consumes and pays in Fivetran, as given, so the
account team can model cost outside the skill.

    # Stage 2: record the spend the customer shared (optional, never required).
    python3 fivetran_consumption.py spend --month 2026-09 --monthly-usd 12000 \\
        --basis invoice --monthly-mar 25000000 -o out/fivetran_spend.json

    # Gate 1: print the fivetran_spend and fivetran_consumption sections. Writes nothing.
    python3 fivetran_consumption.py summary -p out/plan.json --mar out/mar.json \\
        --spend out/fivetran_spend.json

    # Gate 1, only after the customer agrees: write the CSV export.
    python3 fivetran_consumption.py export -p out/plan.json --mar out/mar.json \\
        --spend out/fivetran_spend.json --warehouse-id abc123 --profile prod -o out
    python3 fivetran_consumption.py export ... -o out --volume main.fivetran_migration.exports
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ftlfc.consumption import (
    SPEND_BASES,
    ConsumptionError,
    billing_rows,
    check_volume,
    consumption_rows,
    load_spend,
    mar_by_table_rows,
    parse_metastore,
    plan_group_ids,
    record_spend,
    save_spend,
    summary_lines,
    write_export,
)
from ftlfc.mar import MarError, run_databricks_query


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)

    spend = sub.add_parser("spend", help="record the Fivetran spend the customer shared")
    spend.add_argument("--month", required=True, help="month the amount covers, YYYY-MM")
    spend.add_argument("--monthly-usd", type=float, required=True, help="monthly amount in USD")
    spend.add_argument("--basis", choices=SPEND_BASES, help="where the figure comes from")
    spend.add_argument("--monthly-mar", type=int, help="monthly MAR, if the customer shared it")
    spend.add_argument("--note", help="plan, renewal date, or anything else they shared")
    spend.add_argument("-o", "--output", type=Path, default=Path("out/fivetran_spend.json"))

    for name, help_text in (
        ("summary", "print the Gate 1 consumption and spend sections; writes nothing"),
        ("export", "write the CSV export, only after the customer agrees at Gate 1"),
    ):
        p = sub.add_parser(name, help=help_text)
        p.add_argument("-p", "--plan", type=Path, default=Path("out/plan.json"))
        p.add_argument("--mar", type=Path, help="mar.json from fivetran_mar.py, if it ran")
        p.add_argument("--spend", type=Path, help="fivetran_spend.json, if spend was recorded")

    export = sub.choices["export"]
    export.add_argument("-o", "--output-dir", type=Path, default=Path("out"))
    export.add_argument("--cloud", help="target cloud (aws, azure, gcp)")
    export.add_argument("--region", help="target region, e.g. us-west-2")
    export.add_argument(
        "--warehouse-id", help="SQL warehouse to read cloud and region from current_metastore()"
    )
    export.add_argument("--profile", help="Databricks CLI profile")
    export.add_argument(
        "--volume",
        help="also copy the folder to this existing UC volume, <catalog>.<schema>.<volume>",
    )

    args = parser.parse_args(argv)
    try:
        if args.command == "spend":
            return _spend(args)
        return _summary_or_export(args)
    except (ConsumptionError, MarError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


def _spend(args: argparse.Namespace) -> int:
    entries = record_spend(
        load_spend(args.output),
        month=args.month,
        monthly_usd=args.monthly_usd,
        basis=args.basis,
        monthly_mar=args.monthly_mar,
        note=args.note,
    )
    save_spend(args.output, entries)
    print(f"Recorded Fivetran spend for {args.month} in {args.output} (as given, not priced)")
    return 0


def _summary_or_export(args: argparse.Namespace) -> int:
    if not args.plan.exists():
        print(f"error: {args.plan} not found. Run plan_migration.py first.", file=sys.stderr)
        return 1
    plan = json.loads(args.plan.read_text())
    mar_doc = None
    if args.mar:
        if not args.mar.exists():
            print(f"error: {args.mar} not found", file=sys.stderr)
            return 1
        mar_doc = json.loads(args.mar.read_text())
    spend = load_spend(args.spend)

    cloud = region = None
    if args.command == "export":
        if args.volume:
            check_volume(args.volume)
        cloud, region = _cloud_region(args)

    consumption = consumption_rows(plan, mar_doc, cloud=cloud, region=region)
    billing = billing_rows(mar_doc, spend, plan_group_ids(plan))

    if args.command == "summary":
        _print_summary(consumption, billing)
        return 0

    out, counts = write_export(
        args.output_dir, consumption, billing, mar_by_table_rows(plan, mar_doc)
    )
    print(f"Wrote {out}")
    for file_name, n in counts.items():
        print(f"  {file_name}: {n} rows")
    if not counts["mar_by_table.csv"]:
        print("  No Platform Connector MAR was loaded; connections.csv has no MAR columns filled.")
    if args.volume:
        target = _copy_to_volume(out, args.volume, args.profile)
        print(f"Copied to {target}")
    return 0


def _cloud_region(args: argparse.Namespace) -> tuple[str | None, str | None]:
    if args.cloud or args.region or not args.warehouse_id:
        return args.cloud, args.region
    rows = run_databricks_query(
        "SELECT current_metastore() AS metastore", args.warehouse_id, args.profile
    )
    return parse_metastore(str(rows[0]["metastore"])) if rows else (None, None)


def _copy_to_volume(folder: Path, volume: str, profile: str | None) -> str:
    catalog, schema, name = check_volume(volume)
    target = f"dbfs:/Volumes/{catalog}/{schema}/{name}/{folder.name}"
    command = ["databricks", "fs", "cp", "--recursive", "--overwrite", str(folder), target]
    if profile:
        command += ["--profile", profile]
    try:
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
    except FileNotFoundError as exc:
        raise ConsumptionError("The databricks CLI is not installed or not on PATH") from exc
    if completed.returncode != 0:
        raise ConsumptionError(
            f"Copy to {target} failed: {completed.stderr.strip() or completed.stdout.strip()}. "
            f"The files are still in {folder}."
        )
    return target.removeprefix("dbfs:")


def _print_summary(consumption: list[dict], billing: list[dict]) -> None:
    lines = summary_lines(consumption, billing)
    if not lines:
        print("No Fivetran consumption or spend was recorded.")
        return
    section = None
    for name, item, detail in lines:
        if name != section:
            print(name)
            section = name
        print(f"  {item}: {detail}")
    print("Recorded figures, not estimates. The skill does not price Lakeflow Connect.")


if __name__ == "__main__":
    raise SystemExit(main())
