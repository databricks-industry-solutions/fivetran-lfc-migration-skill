#!/usr/bin/env python3
"""Stage 2 - collect Fivetran MAR and spend from the Platform Connector.

No Fivetran REST endpoint exposes MAR or cost, so this reads the Platform
Connector tables in the customer's destination warehouse.

    # Destination is Databricks: query it directly.
    python3 fivetran_mar.py --warehouse-id abc123 --profile prod -o out/mar.json

    # Destination is Snowflake: query it directly.
    python3 fivetran_mar.py --snowflake --sf-account xy12345 --sf-user admin \\
        --sf-database FIVETRAN_DB --sf-warehouse COMPUTE_WH -o out/mar.json

    # Destination is BigQuery: query it directly.
    python3 fivetran_mar.py --bigquery --bq-project my-project -o out/mar.json

    # Destination is Redshift: query it directly.
    python3 fivetran_mar.py --redshift --rs-cluster my-cluster --rs-database fivetran \\
        --rs-db-user admin -o out/mar.json

    # Any warehouse: hand the SQL over, ingest the export.
    python3 fivetran_mar.py --print-sql
    python3 fivetran_mar.py --csv mar_export.csv -o out/mar.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ftlfc.mar import (
    PLATFORM_SCHEMAS,
    BigQueryConfig,
    MarError,
    RedshiftConfig,
    SnowflakeConfig,
    aggregate,
    build_cost_query,
    build_mar_query,
    detect_bigquery_platform_schema,
    detect_platform_schema,
    detect_redshift_platform_schema,
    detect_snowflake_platform_schema,
    load_csv,
    load_rows,
    run_bigquery_query,
    run_databricks_query,
    run_redshift_query,
    run_snowflake_query,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )

    source = parser.add_mutually_exclusive_group()
    source.add_argument("--csv", type=Path, help="MAR export produced by --print-sql")
    source.add_argument(
        "--warehouse-id", help="Databricks SQL warehouse id, when the destination is Databricks"
    )
    source.add_argument(
        "--snowflake",
        action="store_true",
        help="query Snowflake directly (requires snowflake-connector-python)",
    )
    source.add_argument(
        "--bigquery",
        action="store_true",
        help="query BigQuery directly (requires google-cloud-bigquery)",
    )
    source.add_argument(
        "--redshift",
        action="store_true",
        help="query Redshift via the Data API (requires boto3)",
    )
    source.add_argument(
        "--print-sql",
        action="store_true",
        help="print the queries to run against the destination warehouse, then exit",
    )

    # Databricks-specific options
    parser.add_argument("--profile", help="Databricks CLI profile")
    parser.add_argument("--catalog", help="Unity Catalog catalog holding the Fivetran schema")

    # Snowflake-specific options
    sf = parser.add_argument_group("Snowflake options (used with --snowflake)")
    sf.add_argument("--sf-account", help="Snowflake account identifier (e.g. xy12345.us-east-1)")
    sf.add_argument("--sf-user", help="Snowflake username")
    sf.add_argument(
        "--sf-password",
        help="Snowflake password (prefer SNOWFLAKE_PASSWORD env var to avoid shell history)",
    )
    sf.add_argument("--sf-database", help="Snowflake database containing Platform Connector tables")
    sf.add_argument("--sf-warehouse", help="Snowflake virtual warehouse to execute the query")
    sf.add_argument("--sf-role", help="Snowflake role to use")
    sf.add_argument(
        "--sf-authenticator",
        help="Snowflake authenticator (e.g. externalbrowser, snowflake_jwt)",
    )

    # BigQuery-specific options
    bq = parser.add_argument_group("BigQuery options (used with --bigquery)")
    bq.add_argument("--bq-project", help="GCP project id containing Platform Connector tables")
    bq.add_argument(
        "--bq-credentials-json",
        help="Path to a GCP service-account JSON key file "
        "(default: application default credentials)",
    )
    bq.add_argument("--bq-location", help="BigQuery dataset location (e.g. US, EU)")

    # Redshift-specific options
    rs = parser.add_argument_group("Redshift options (used with --redshift)")
    rs.add_argument("--rs-cluster", help="Redshift provisioned cluster identifier")
    rs.add_argument("--rs-workgroup", help="Redshift Serverless workgroup name")
    rs.add_argument("--rs-database", help="Redshift database containing Platform Connector tables")
    rs.add_argument(
        "--rs-db-user",
        help="Redshift database user (required for provisioned clusters)",
    )
    rs.add_argument("--rs-region", help="AWS region (default: from AWS config)")

    # Common options
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
    source_labels = {
        "snowflake": "snowflake",
        "bigquery": "bigquery",
        "redshift": "redshift",
    }
    source_label = next(
        (label for flag, label in source_labels.items() if getattr(args, flag, False)),
        "fivetran-platform-connector",
    )
    payload = {
        "source": source_label,
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


def _build_snowflake_config(args: argparse.Namespace) -> SnowflakeConfig:
    """Construct a SnowflakeConfig from CLI args, filling password from env."""
    if not args.sf_account:
        raise MarError("--sf-account is required when using --snowflake")
    if not args.sf_user:
        raise MarError("--sf-user is required when using --snowflake")
    if not args.sf_database:
        raise MarError("--sf-database is required when using --snowflake")

    password = args.sf_password or os.environ.get("SNOWFLAKE_PASSWORD")
    authenticator = args.sf_authenticator
    if not password and not authenticator:
        raise MarError(
            "Provide a password via --sf-password or SNOWFLAKE_PASSWORD env var, "
            "or use --sf-authenticator (e.g. externalbrowser) for passwordless auth."
        )

    return SnowflakeConfig(
        account=args.sf_account,
        user=args.sf_user,
        database=args.sf_database,
        password=password,
        authenticator=authenticator,
        warehouse=args.sf_warehouse,
        role=args.sf_role,
    )


def _load(args: argparse.Namespace) -> tuple[list, list]:
    if args.csv:
        with args.csv.open() as handle:
            return load_csv(handle), []

    if args.snowflake:
        return _load_from_snowflake(args)

    if args.bigquery:
        return _load_from_bigquery(args)

    if args.redshift:
        return _load_from_redshift(args)

    if not args.warehouse_id:
        raise MarError(
            "Choose a source: --warehouse-id (Databricks), --snowflake, --bigquery, "
            "--redshift, --csv, or --print-sql."
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


def _load_from_snowflake(args: argparse.Namespace) -> tuple[list, list]:
    """Query the Fivetran Platform Connector tables in Snowflake directly."""
    config = _build_snowflake_config(args)

    schema = args.schema or detect_snowflake_platform_schema(config)
    qualified = f"{config.database}.{schema}"

    records = load_rows(
        run_snowflake_query(build_mar_query(qualified, args.months), config)
    )
    try:
        cost_rows = run_snowflake_query(build_cost_query(qualified, args.months), config)
    except MarError as exc:
        logging.warning("Spend tables unavailable (%s); continuing with MAR only", exc)
        cost_rows = []
    return records, cost_rows


def _load_from_bigquery(args: argparse.Namespace) -> tuple[list, list]:
    """Query the Fivetran Platform Connector tables in BigQuery directly."""
    config = _build_bigquery_config(args)

    schema = args.schema or detect_bigquery_platform_schema(config)
    qualified = f"`{config.project}.{schema}`"

    # BigQuery uses backtick-qualified table names in the query
    mar_sql = build_mar_query(qualified, args.months)
    cost_sql = build_cost_query(qualified, args.months)

    records = load_rows(run_bigquery_query(mar_sql, config))
    try:
        cost_rows = run_bigquery_query(cost_sql, config)
    except MarError as exc:
        logging.warning("Spend tables unavailable (%s); continuing with MAR only", exc)
        cost_rows = []
    return records, cost_rows


def _build_bigquery_config(args: argparse.Namespace) -> BigQueryConfig:
    """Construct a BigQueryConfig from CLI args."""
    if not args.bq_project:
        raise MarError("--bq-project is required when using --bigquery")

    credentials_json = None
    if args.bq_credentials_json:
        creds_path = Path(args.bq_credentials_json)
        if not creds_path.exists():
            raise MarError(f"Credentials file not found: {creds_path}")
        credentials_json = creds_path.read_text()

    return BigQueryConfig(
        project=args.bq_project,
        dataset=args.schema or "",
        credentials_json=credentials_json,
        location=args.bq_location,
    )


def _load_from_redshift(args: argparse.Namespace) -> tuple[list, list]:
    """Query the Fivetran Platform Connector tables in Redshift directly."""
    config = _build_redshift_config(args)

    schema = args.schema or detect_redshift_platform_schema(config)

    records = load_rows(
        run_redshift_query(build_mar_query(schema, args.months), config)
    )
    try:
        cost_rows = run_redshift_query(build_cost_query(schema, args.months), config)
    except MarError as exc:
        logging.warning("Spend tables unavailable (%s); continuing with MAR only", exc)
        cost_rows = []
    return records, cost_rows


def _build_redshift_config(args: argparse.Namespace) -> RedshiftConfig:
    """Construct a RedshiftConfig from CLI args."""
    if not args.rs_database:
        raise MarError("--rs-database is required when using --redshift")
    if not args.rs_cluster and not args.rs_workgroup:
        raise MarError(
            "--rs-cluster (provisioned) or --rs-workgroup (serverless) is required "
            "when using --redshift"
        )

    return RedshiftConfig(
        database=args.rs_database,
        cluster_identifier=args.rs_cluster,
        db_user=args.rs_db_user,
        workgroup_name=args.rs_workgroup,
        region=args.rs_region,
    )


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
