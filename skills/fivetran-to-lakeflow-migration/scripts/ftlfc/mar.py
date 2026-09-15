"""Fivetran MAR and cost data from the Platform Connector.

No Fivetran REST endpoint exposes MAR, usage, cost, or credits. Volume and spend
have to come from the Fivetran Platform Connector tables, which live in the
customer's own destination warehouse -- which is often not Databricks.

So this module supports six paths, in decreasing order of convenience:

1. ``--warehouse-id`` runs the query directly via the Databricks CLI.
2. ``--snowflake`` runs the query directly against Snowflake.
3. ``--bigquery`` runs the query directly against BigQuery.
4. ``--redshift`` runs the query directly against Redshift.
5. ``--csv`` ingests an export the customer ran themselves, from any warehouse.
6. ``--print-sql`` emits the query to hand to whoever does have access.

Everything downstream consumes the same normalised record shape, so the cost
model does not care which path produced it.
"""

from __future__ import annotations

import csv
import datetime as dt
import io
import json
import logging
import subprocess
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

log = logging.getLogger(__name__)

# Auto-created Platform connections land in fivetran_metadata. fivetran_log is
# the historical name and is still what older accounts and most community
# tooling use, so probe for both.
PLATFORM_SCHEMAS = ("fivetran_metadata", "fivetran_log")

# free_type is part of incremental_mar's primary key, so the same table on the
# same day appears once per billing category. Summing without this filter
# conflates billable and non-billable volume and inflates the estimate.
PAID = "PAID"

MAR_QUERY = """
SELECT
    connection_name,
    schema_name,
    table_name,
    date_trunc('month', measured_date) AS measured_month,
    SUM(incremental_rows)              AS mar
FROM {schema}.incremental_mar
WHERE free_type = '{paid}'
  AND measured_date >= '{since}'
GROUP BY connection_name, schema_name, table_name, date_trunc('month', measured_date)
ORDER BY measured_month, mar DESC
""".strip()

# An account is billed on dollars or credits, never both, so exactly one of
# these returns rows. Reconciling spend against MAR also detects an ELA, where
# the two are uncorrelated and any per-MAR savings claim is meaningless.
COST_QUERY = """
SELECT measured_month, group_id, amount AS cost_usd, NULL AS credits
FROM {schema}.usage_cost
WHERE measured_month >= '{since_month}'
UNION ALL
SELECT measured_month, group_id, NULL AS cost_usd, credits_consumed AS credits
FROM {schema}.credits_used
WHERE measured_month >= '{since_month}'
ORDER BY measured_month
""".strip()

REQUIRED_MAR_FIELDS = ("connection_name", "schema_name", "table_name", "measured_month", "mar")


class MarError(RuntimeError):
    """MAR data could not be loaded."""


@dataclass(frozen=True)
class MarRecord:
    connection_name: str
    schema_name: str
    table_name: str
    measured_month: str  # YYYY-MM
    mar: int

    @property
    def qualified_table(self) -> str:
        return f"{self.schema_name}.{self.table_name}"


def months_ago(months: int) -> dt.date:
    today = dt.date.today().replace(day=1)
    year, month = divmod((today.year * 12 + today.month - 1) - months, 12)
    return dt.date(year, month + 1, 1)


def build_mar_query(schema: str, months: int = 6) -> str:
    return MAR_QUERY.format(schema=schema, paid=PAID, since=months_ago(months).isoformat())


def build_cost_query(schema: str, months: int = 6) -> str:
    return COST_QUERY.format(schema=schema, since_month=months_ago(months).strftime("%Y-%m"))


# -- Loading ---------------------------------------------------------------


def load_csv(handle: io.TextIOBase) -> list[MarRecord]:
    """Parse a MAR export produced by any warehouse.

    Column names are matched case-insensitively so an export from Snowflake
    (which upper-cases identifiers) works without pre-processing.
    """
    reader = csv.DictReader(handle)
    if not reader.fieldnames:
        raise MarError("CSV export is empty")

    lookup = {name.strip().lower(): name for name in reader.fieldnames}
    missing = [f for f in REQUIRED_MAR_FIELDS if f not in lookup]
    if missing:
        raise MarError(
            f"CSV export is missing required column(s): {', '.join(missing)}. "
            f"Found: {', '.join(sorted(lookup))}. Generate it with --print-sql."
        )

    records = []
    for line, row in enumerate(reader, start=2):
        try:
            records.append(_to_record({f: row[lookup[f]] for f in REQUIRED_MAR_FIELDS}))
        except (TypeError, ValueError) as exc:
            raise MarError(f"CSV line {line}: {exc}") from exc
    return records


def load_rows(rows: Iterable[dict[str, Any]]) -> list[MarRecord]:
    return [_to_record(row) for row in rows]


def _to_record(row: dict[str, Any]) -> MarRecord:
    raw_mar = row.get("mar")
    mar = int(float(raw_mar)) if raw_mar not in (None, "") else 0
    return MarRecord(
        connection_name=str(row.get("connection_name") or "").strip(),
        schema_name=str(row.get("schema_name") or "").strip(),
        table_name=str(row.get("table_name") or "").strip(),
        measured_month=_normalise_month(row.get("measured_month")),
        mar=mar,
    )


def _normalise_month(value: Any) -> str:
    """Coerce the many shapes a month column arrives in to YYYY-MM."""
    if value in (None, ""):
        return ""
    if isinstance(value, (dt.datetime, dt.date)):
        return value.strftime("%Y-%m")
    text = str(value).strip()
    # Tolerates "2026-08", "2026-08-01", and "2026-08-01T00:00:00Z".
    if len(text) >= 7 and text[4] == "-":
        return text[:7]
    raise ValueError(f"unrecognised month value {value!r}; expected YYYY-MM")


# -- Databricks execution ---------------------------------------------------


def run_databricks_query(
    sql: str, warehouse_id: str, profile: str | None = None, timeout: float = 300.0
) -> list[dict[str, Any]]:
    """Execute SQL through the Databricks CLI and return rows as dicts.

    Uses the CLI rather than a SQL driver so the skill inherits whatever auth
    the SA already has configured, with no extra dependency.
    """
    payload = {
        "statement": sql,
        "warehouse_id": warehouse_id,
        "format": "JSON_ARRAY",
        "disposition": "INLINE",
        "wait_timeout": "50s",
    }
    command = [
        "databricks",
        "api",
        "post",
        "/api/2.0/sql/statements/",
        "--json",
        json.dumps(payload),
    ]
    if profile:
        command += ["--profile", profile]

    try:
        completed = subprocess.run(
            command, capture_output=True, text=True, timeout=timeout, check=False
        )
    except FileNotFoundError as exc:
        raise MarError(
            "The databricks CLI is not installed or not on PATH. Install it, or use "
            "--print-sql and --csv instead."
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise MarError(f"Databricks query timed out after {timeout}s") from exc

    if completed.returncode != 0:
        raise MarError(
            f"databricks CLI failed: {completed.stderr.strip() or completed.stdout.strip()}"
        )

    try:
        response = json.loads(completed.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise MarError(f"Could not parse the CLI response: {completed.stdout[:500]}") from exc

    return _rows_from_statement(response)


def _rows_from_statement(response: dict[str, Any]) -> list[dict[str, Any]]:
    status = (response.get("status") or {}).get("state")
    if status not in ("SUCCEEDED", None):
        message = ((response.get("status") or {}).get("error") or {}).get("message", status)
        raise MarError(f"Databricks statement did not succeed ({status}): {message}")

    manifest = response.get("manifest") or {}
    columns = [c.get("name") for c in (manifest.get("schema") or {}).get("columns", [])]
    data = (response.get("result") or {}).get("data_array") or []
    if not columns:
        return []
    # A truncated result silently under-reports spend, which is worse than failing.
    if (response.get("result") or {}).get("truncated"):
        raise MarError(
            "Databricks truncated the result set. Narrow the query window with --months "
            "or export via --print-sql instead."
        )
    return [dict(zip(columns, row, strict=False)) for row in data]


def detect_platform_schema(
    warehouse_id: str, profile: str | None = None, catalog: str | None = None
) -> str:
    """Find whichever of fivetran_metadata / fivetran_log actually exists."""
    for schema in PLATFORM_SCHEMAS:
        qualified = f"{catalog}.{schema}" if catalog else schema
        try:
            run_databricks_query(
                f"SELECT 1 FROM {qualified}.incremental_mar LIMIT 1", warehouse_id, profile
            )
            log.info("Found Fivetran Platform Connector data in %s", qualified)
            return qualified
        except MarError:
            continue
    raise MarError(
        "Could not find the Fivetran Platform Connector tables. Looked for "
        f"{' and '.join(PLATFORM_SCHEMAS)}. Confirm the Platform connection exists and "
        "that the warehouse can read it, or supply the schema explicitly with --schema."
    )


# -- Snowflake execution ----------------------------------------------------


def _import_snowflake():
    """Lazy-import snowflake-connector-python so it's not a hard dependency."""
    try:
        import snowflake.connector
        return snowflake.connector
    except ImportError as exc:
        raise MarError(
            "snowflake-connector-python is required for --snowflake. "
            "Install it with: pip install snowflake-connector-python"
        ) from exc


@dataclass(frozen=True)
class SnowflakeConfig:
    """Connection parameters for a Snowflake account."""
    account: str
    user: str
    database: str
    password: str | None = None
    authenticator: str | None = None
    warehouse: str | None = None
    role: str | None = None

    def connect(self):
        sf = _import_snowflake()
        kwargs: dict[str, Any] = {
            "account": self.account,
            "user": self.user,
            "database": self.database,
        }
        if self.password:
            kwargs["password"] = self.password
        if self.authenticator:
            kwargs["authenticator"] = self.authenticator
        if self.warehouse:
            kwargs["warehouse"] = self.warehouse
        if self.role:
            kwargs["role"] = self.role
        return sf.connect(**kwargs)


def run_snowflake_query(sql: str, config: SnowflakeConfig) -> list[dict[str, Any]]:
    """Execute SQL against Snowflake and return rows as lowercase-keyed dicts.

    Column names are lowercased to match the Databricks path output, so
    ``load_rows`` works identically for both.
    """
    try:
        conn = config.connect()
    except Exception as exc:
        raise MarError(f"Snowflake connection failed: {exc}") from exc

    try:
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            if not cursor.description:
                return []
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]
        finally:
            cursor.close()
    finally:
        conn.close()


def detect_snowflake_platform_schema(config: SnowflakeConfig) -> str:
    """Find whichever of fivetran_metadata / fivetran_log exists in Snowflake."""
    for schema in PLATFORM_SCHEMAS:
        try:
            run_snowflake_query(
                f"SELECT 1 FROM {config.database}.{schema}.incremental_mar LIMIT 1",
                config,
            )
            log.info("Found Fivetran Platform Connector data in %s.%s", config.database, schema)
            return schema
        except MarError:
            continue
    raise MarError(
        f"Could not find the Fivetran Platform Connector tables in Snowflake database "
        f"'{config.database}'. Looked for schemas "
        f"{' and '.join(PLATFORM_SCHEMAS)}. Confirm the Platform connection exists, or "
        "supply the schema explicitly with --schema."
    )


# -- BigQuery execution ------------------------------------------------------


def _import_bigquery():
    """Lazy-import google-cloud-bigquery so it's not a hard dependency."""
    try:
        from google.cloud import bigquery
        return bigquery
    except ImportError as exc:
        raise MarError(
            "google-cloud-bigquery is required for --bigquery. "
            "Install it with: pip install google-cloud-bigquery"
        ) from exc


@dataclass(frozen=True)
class BigQueryConfig:
    """Connection parameters for a BigQuery project."""
    project: str
    dataset: str
    credentials_json: str | None = None
    location: str | None = None

    def client(self):
        bq = _import_bigquery()
        kwargs: dict[str, Any] = {"project": self.project}
        if self.credentials_json:
            import json as _json

            from google.oauth2 import service_account
            info = _json.loads(self.credentials_json)
            kwargs["credentials"] = service_account.Credentials.from_service_account_info(info)
        if self.location:
            kwargs["location"] = self.location
        return bq.Client(**kwargs)


def run_bigquery_query(sql: str, config: BigQueryConfig) -> list[dict[str, Any]]:
    """Execute SQL against BigQuery and return rows as lowercase-keyed dicts."""
    try:
        client = config.client()
    except Exception as exc:
        raise MarError(f"BigQuery connection failed: {exc}") from exc

    try:
        result = client.query(sql).result()
        columns = [field.name.lower() for field in result.schema]
        return [
            dict(zip(columns, [row[i] for i in range(len(columns))], strict=False))
            for row in result
        ]
    except Exception as exc:
        raise MarError(f"BigQuery query failed: {exc}") from exc


def detect_bigquery_platform_schema(config: BigQueryConfig) -> str:
    """Find whichever of fivetran_metadata / fivetran_log exists in BigQuery."""
    for schema in PLATFORM_SCHEMAS:
        try:
            run_bigquery_query(
                f"SELECT 1 FROM `{config.project}.{schema}.incremental_mar` LIMIT 1",
                config,
            )
            log.info(
                "Found Fivetran Platform Connector data in %s.%s", config.project, schema
            )
            return schema
        except MarError:
            continue
    raise MarError(
        f"Could not find the Fivetran Platform Connector tables in BigQuery project "
        f"'{config.project}'. Looked for datasets "
        f"{' and '.join(PLATFORM_SCHEMAS)}. Confirm the Platform connection exists, or "
        "supply the dataset explicitly with --schema."
    )


# -- Redshift execution ------------------------------------------------------


def _import_boto3():
    """Lazy-import boto3 so it's not a hard dependency."""
    try:
        import boto3
        return boto3
    except ImportError as exc:
        raise MarError(
            "boto3 is required for --redshift. "
            "Install it with: pip install boto3"
        ) from exc


@dataclass(frozen=True)
class RedshiftConfig:
    """Connection parameters for a Redshift cluster or Serverless workgroup."""
    database: str
    # Provisioned cluster
    cluster_identifier: str | None = None
    db_user: str | None = None
    # Serverless
    workgroup_name: str | None = None
    # Common
    region: str | None = None

    def _validate(self) -> None:
        if not self.cluster_identifier and not self.workgroup_name:
            raise MarError(
                "Provide either --rs-cluster (provisioned) or --rs-workgroup (serverless)."
            )
        if self.cluster_identifier and not self.db_user:
            raise MarError("--rs-db-user is required when using --rs-cluster (provisioned).")


def run_redshift_query(sql: str, config: RedshiftConfig) -> list[dict[str, Any]]:
    """Execute SQL against Redshift via the Data API and return lowercase-keyed dicts."""
    config._validate()
    boto3 = _import_boto3()

    kwargs: dict[str, Any] = {"region_name": config.region} if config.region else {}
    client = boto3.client("redshift-data", **kwargs)

    exec_params: dict[str, Any] = {"Database": config.database, "Sql": sql}
    if config.cluster_identifier:
        exec_params["ClusterIdentifier"] = config.cluster_identifier
        exec_params["DbUser"] = config.db_user
    else:
        exec_params["WorkgroupName"] = config.workgroup_name

    try:
        response = client.execute_statement(**exec_params)
        statement_id = response["Id"]
    except Exception as exc:
        raise MarError(f"Redshift execute_statement failed: {exc}") from exc

    # Poll until done (Data API is async)
    import time
    waiter_delay = 1.0
    for _ in range(300):
        desc = client.describe_statement(Id=statement_id)
        status = desc["Status"]
        if status == "FINISHED":
            break
        if status in ("FAILED", "ABORTED"):
            raise MarError(
                f"Redshift query {status}: {desc.get('Error', 'unknown error')}"
            )
        time.sleep(waiter_delay)
        waiter_delay = min(waiter_delay * 1.5, 5.0)
    else:
        raise MarError("Redshift query timed out after polling for ~5 minutes.")

    # Fetch results
    try:
        result = client.get_statement_result(Id=statement_id)
        columns = [col["name"].lower() for col in result["ColumnMetadata"]]
        rows = []
        for record in result["Records"]:
            row_values = []
            for field in record:
                # Data API returns typed fields like {"stringValue": "..."} or {"longValue": 42}
                val = next(iter(field.values()))
                row_values.append(val)
            rows.append(dict(zip(columns, row_values, strict=False)))
        return rows
    except Exception as exc:
        raise MarError(f"Redshift get_statement_result failed: {exc}") from exc


def detect_redshift_platform_schema(config: RedshiftConfig) -> str:
    """Find whichever of fivetran_metadata / fivetran_log exists in Redshift."""
    for schema in PLATFORM_SCHEMAS:
        try:
            run_redshift_query(
                f"SELECT 1 FROM {schema}.incremental_mar LIMIT 1",
                config,
            )
            log.info("Found Fivetran Platform Connector data in %s", schema)
            return schema
        except MarError:
            continue
    raise MarError(
        f"Could not find the Fivetran Platform Connector tables in Redshift database "
        f"'{config.database}'. Looked for schemas "
        f"{' and '.join(PLATFORM_SCHEMAS)}. Confirm the Platform connection exists, or "
        "supply the schema explicitly with --schema."
    )


# -- Aggregation -------------------------------------------------------------


def aggregate(records: list[MarRecord]) -> dict[str, Any]:
    """Roll MAR up to the grains the cost model and the report need."""
    if not records:
        return {"months": [], "total_mar": 0, "by_connection": {}, "by_table": {}, "monthly": {}}

    months = sorted({r.measured_month for r in records if r.measured_month})
    by_connection: dict[str, int] = {}
    by_table: dict[str, int] = {}
    monthly: dict[str, int] = {}

    for record in records:
        by_connection[record.connection_name] = (
            by_connection.get(record.connection_name, 0) + record.mar
        )
        key = f"{record.connection_name}|{record.qualified_table}"
        by_table[key] = by_table.get(key, 0) + record.mar
        monthly[record.measured_month] = monthly.get(record.measured_month, 0) + record.mar

    return {
        "months": months,
        "month_count": len(months),
        "total_mar": sum(r.mar for r in records),
        # The most recent complete month is the fairest single basis for a
        # monthly cost comparison; an average across a ramp-up understates it.
        "latest_month": months[-1] if months else None,
        "latest_month_mar": monthly.get(months[-1], 0) if months else 0,
        "peak_month_mar": max(monthly.values()) if monthly else 0,
        "mean_monthly_mar": round(sum(monthly.values()) / len(monthly)) if monthly else 0,
        "by_connection": dict(sorted(by_connection.items(), key=lambda kv: -kv[1])),
        "by_table": dict(sorted(by_table.items(), key=lambda kv: -kv[1])),
        "monthly": dict(sorted(monthly.items())),
    }
