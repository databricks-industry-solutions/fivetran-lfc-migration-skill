"""Fivetran MAR and cost data from the Platform Connector.

No Fivetran REST endpoint exposes MAR, usage, cost, or credits. Volume and spend
have to come from the Fivetran Platform Connector tables, which live in the
customer's own destination warehouse -- which is often not Databricks.

So this module supports three paths, in decreasing order of convenience:

1. ``--warehouse databricks`` runs the query directly via the Databricks CLI.
2. ``--csv`` ingests an export the customer ran themselves, from any warehouse.
3. ``--print-sql`` emits the query to hand to whoever does have access.

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
from dataclasses import dataclass
from typing import Any, Iterable

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
    command = ["databricks", "api", "post", "/api/2.0/sql/statements/", "--json", json.dumps(payload)]
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
        raise MarError(f"databricks CLI failed: {completed.stderr.strip() or completed.stdout.strip()}")

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
    return [dict(zip(columns, row)) for row in data]


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
