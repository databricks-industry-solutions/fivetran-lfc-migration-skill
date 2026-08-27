"""Query Databricks system tables for Lakeflow Connect telemetry.

Three system tables ground the cost model in observable data rather than
hardcoded assumptions:

- ``system.billing.list_prices`` — the customer's actual DBU rates by SKU,
  replacing the default $0.45 and $0.36 per-DBU assumptions.
- ``system.billing.usage`` — actual DBU consumption for any existing Lakeflow
  Connect pipelines, replacing the assumed 6 minutes per run.
- ``system.lakeflow.pipelines`` — pipeline metadata to identify ingestion
  pipelines vs gateways and join with billing data.

Every query runs through the Databricks CLI (``databricks api post``) so the
skill inherits whatever auth the user already has, with no extra dependency.
"""

from __future__ import annotations

import datetime as dt
import logging
from typing import Any

from .costs import RateCard
from .mar import MarError, run_databricks_query

log = logging.getLogger(__name__)

SERVERLESS_SKU_PATTERN = "JOBS_SERVERLESS_COMPUTE"
GATEWAY_SKU_PATTERN = "DLT_ADVANCED_COMPUTE"

LIST_PRICES_QUERY = """
SELECT
    sku_name,
    pricing.effective_list.default AS dbu_rate_usd
FROM system.billing.list_prices
WHERE price_end_time IS NULL
  AND (sku_name LIKE '%{serverless_pattern}%'
       OR sku_name LIKE '%{gateway_pattern}%')
ORDER BY sku_name
""".strip()

LAKEFLOW_USAGE_QUERY = """
WITH latest_pipelines AS (
    SELECT
        workspace_id,
        pipeline_id,
        name AS pipeline_name,
        pipeline_type,
        ROW_NUMBER() OVER (
            PARTITION BY workspace_id, pipeline_id
            ORDER BY change_time DESC
        ) AS rn
    FROM system.lakeflow.pipelines
    WHERE pipeline_type IN ('INGESTION_PIPELINE', 'INGESTION_GATEWAY')
)
SELECT
    p.pipeline_type,
    p.pipeline_name,
    u.usage_date,
    u.sku_name,
    SUM(u.usage_quantity) AS dbus,
    SUM(u.usage_quantity * lp.pricing.effective_list.default) AS list_cost_usd
FROM system.billing.usage u
JOIN latest_pipelines p
    ON u.usage_metadata.dlt_pipeline_id = p.pipeline_id
    AND p.rn = 1
JOIN system.billing.list_prices lp
    ON lp.sku_name = u.sku_name
    AND u.usage_end_time >= lp.price_start_time
    AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
WHERE u.usage_date >= '{since}'
GROUP BY p.pipeline_type, p.pipeline_name, u.usage_date, u.sku_name
ORDER BY p.pipeline_type, u.usage_date
""".strip()

PIPELINE_RUN_DURATION_QUERY = """
WITH latest_pipelines AS (
    SELECT
        workspace_id,
        pipeline_id,
        name AS pipeline_name,
        pipeline_type,
        ROW_NUMBER() OVER (
            PARTITION BY workspace_id, pipeline_id
            ORDER BY change_time DESC
        ) AS rn
    FROM system.lakeflow.pipelines
    WHERE pipeline_type = 'INGESTION_PIPELINE'
),
runs AS (
    SELECT
        u.usage_metadata.dlt_pipeline_id AS pipeline_id,
        u.usage_date,
        SUM(u.usage_quantity) AS dbus_per_day,
        COUNT(DISTINCT u.usage_start_time) AS run_segments
    FROM system.billing.usage u
    JOIN latest_pipelines p
        ON u.usage_metadata.dlt_pipeline_id = p.pipeline_id
        AND p.rn = 1
    WHERE u.usage_date >= '{since}'
      AND u.sku_name LIKE '%{serverless_pattern}%'
    GROUP BY u.usage_metadata.dlt_pipeline_id, u.usage_date
)
SELECT
    pipeline_id,
    AVG(dbus_per_day) AS avg_daily_dbus,
    AVG(run_segments) AS avg_daily_runs
FROM runs
GROUP BY pipeline_id
""".strip()


def build_list_prices_query() -> str:
    return LIST_PRICES_QUERY.format(
        serverless_pattern=SERVERLESS_SKU_PATTERN,
        gateway_pattern=GATEWAY_SKU_PATTERN,
    )


def build_usage_query(months: int = 3) -> str:
    since = (dt.date.today().replace(day=1) - dt.timedelta(days=months * 30)).strftime(
        "%Y-%m-%d"
    )
    return LAKEFLOW_USAGE_QUERY.format(since=since)


def build_run_duration_query(months: int = 3) -> str:
    since = (dt.date.today().replace(day=1) - dt.timedelta(days=months * 30)).strftime(
        "%Y-%m-%d"
    )
    return PIPELINE_RUN_DURATION_QUERY.format(
        since=since, serverless_pattern=SERVERLESS_SKU_PATTERN
    )


def fetch_list_prices(
    warehouse_id: str, profile: str | None = None
) -> dict[str, float]:
    """Return current DBU rates keyed by SKU name."""
    sql = build_list_prices_query()
    try:
        rows = run_databricks_query(sql, warehouse_id, profile)
    except MarError as exc:
        log.warning("Could not fetch list prices: %s", exc)
        return {}

    prices: dict[str, float] = {}
    for row in rows:
        sku = row.get("sku_name", "")
        rate = row.get("dbu_rate_usd")
        if sku and rate is not None:
            try:
                prices[sku] = float(rate)
            except (TypeError, ValueError):
                continue
    return prices


def fetch_lakeflow_usage(
    warehouse_id: str, profile: str | None = None, months: int = 3
) -> list[dict[str, Any]]:
    """Return per-pipeline-type, per-day usage rows for Lakeflow pipelines."""
    sql = build_usage_query(months)
    try:
        return run_databricks_query(sql, warehouse_id, profile)
    except MarError as exc:
        log.warning("Could not fetch Lakeflow usage: %s", exc)
        return []


def fetch_run_durations(
    warehouse_id: str, profile: str | None = None, months: int = 3
) -> list[dict[str, Any]]:
    """Return average daily DBU consumption and run count per ingestion pipeline."""
    sql = build_run_duration_query(months)
    try:
        return run_databricks_query(sql, warehouse_id, profile)
    except MarError as exc:
        log.warning("Could not fetch run durations: %s", exc)
        return []


def extract_dbu_rates(prices: dict[str, float]) -> dict[str, float]:
    """Pick the serverless and gateway rates from the price map.

    The SKU names vary by cloud and region (e.g.
    ``ENTERPRISE_JOBS_SERVERLESS_COMPUTE_US_EAST_1``), so we match on the
    suffix pattern and take the first match.
    """
    rates: dict[str, float] = {}
    for sku, rate in prices.items():
        upper = sku.upper()
        if SERVERLESS_SKU_PATTERN in upper and "serverless_dbu_usd" not in rates:
            rates["serverless_dbu_usd"] = rate
        elif GATEWAY_SKU_PATTERN in upper and "gateway_dbu_usd" not in rates:
            rates["gateway_dbu_usd"] = rate
    return rates


def estimate_minutes_per_run(
    run_durations: list[dict[str, Any]],
    schedule_runs_per_day: float | None = None,
) -> float | None:
    """Derive average minutes per ingestion run from billing telemetry.

    The billing table reports total DBUs per day, not per run. If the pipeline's
    schedule is known (runs per day), we can divide daily DBUs by that count to
    get per-run duration. Without the schedule, we use the ``avg_daily_runs``
    estimate from the billing segments, which is a rougher proxy.
    """
    if not run_durations:
        return None

    total_daily_dbus = 0.0
    total_daily_runs = 0.0
    count = 0

    for row in run_durations:
        try:
            daily_dbus = float(row.get("avg_daily_dbus", 0))
            daily_runs = float(
                schedule_runs_per_day or row.get("avg_daily_runs", 0)
            )
        except (TypeError, ValueError):
            continue
        if daily_dbus > 0 and daily_runs > 0:
            total_daily_dbus += daily_dbus
            total_daily_runs += daily_runs
            count += 1

    if count == 0 or total_daily_runs == 0:
        return None

    avg_dbus_per_run = total_daily_dbus / total_daily_runs
    minutes = avg_dbus_per_run * 60
    return round(minutes, 1)


def estimate_gateway_dbu_per_hour(
    usage_rows: list[dict[str, Any]],
) -> float | None:
    """Derive gateway DBU/hour from actual billing data.

    Gateway pipelines run continuously, so total daily DBUs / 24 gives the
    hourly rate directly.
    """
    gateway_rows = [
        r for r in usage_rows
        if str(r.get("pipeline_type", "")).upper() == "INGESTION_GATEWAY"
    ]
    if not gateway_rows:
        return None

    daily_totals: dict[str, float] = {}
    for row in gateway_rows:
        day = str(row.get("usage_date", ""))
        try:
            dbus = float(row.get("dbus", 0))
        except (TypeError, ValueError):
            continue
        daily_totals[day] = daily_totals.get(day, 0) + dbus

    if not daily_totals:
        return None

    avg_daily = sum(daily_totals.values()) / len(daily_totals)
    return round(avg_daily / 24, 2)


def enrich_rate_card(
    base: RateCard,
    prices: dict[str, float] | None = None,
    usage_rows: list[dict[str, Any]] | None = None,
    run_durations: list[dict[str, Any]] | None = None,
) -> tuple[RateCard, dict[str, Any]]:
    """Return a RateCard with system-table-grounded values and a changelog.

    The changelog records what was overridden and why, so the cost report can
    distinguish model assumptions from observed data.
    """
    overrides: dict[str, Any] = {}
    enrichment_log: dict[str, Any] = {
        "grounded_fields": [],
        "unchanged_fields": [],
        "sources": [],
    }

    if prices:
        dbu_rates = extract_dbu_rates(prices)
        if "serverless_dbu_usd" in dbu_rates:
            overrides["serverless_dbu_usd"] = dbu_rates["serverless_dbu_usd"]
            enrichment_log["grounded_fields"].append(
                f"serverless_dbu_usd: ${dbu_rates['serverless_dbu_usd']:.4f} "
                f"(was ${base.serverless_dbu_usd:.2f})"
            )
            enrichment_log["sources"].append("system.billing.list_prices")
        if "gateway_dbu_usd" in dbu_rates:
            overrides["gateway_dbu_usd"] = dbu_rates["gateway_dbu_usd"]
            enrichment_log["grounded_fields"].append(
                f"gateway_dbu_usd: ${dbu_rates['gateway_dbu_usd']:.4f} "
                f"(was ${base.gateway_dbu_usd:.2f})"
            )

    if run_durations:
        minutes = estimate_minutes_per_run(run_durations)
        if minutes is not None and minutes > 0:
            overrides["minutes_per_run"] = minutes
            enrichment_log["grounded_fields"].append(
                f"minutes_per_run: {minutes:.1f} min "
                f"(was {base.minutes_per_run:.1f} min)"
            )
            enrichment_log["sources"].append("system.billing.usage (ingestion runs)")

    if usage_rows:
        gw_dbu = estimate_gateway_dbu_per_hour(usage_rows)
        if gw_dbu is not None and gw_dbu > 0:
            overrides["gateway_dbu_per_hour"] = gw_dbu
            enrichment_log["grounded_fields"].append(
                f"gateway_dbu_per_hour: {gw_dbu:.2f} DBU/hr "
                f"(was {base.gateway_dbu_per_hour:.1f} DBU/hr)"
            )
            enrichment_log["sources"].append("system.billing.usage (gateways)")

    unchanged = []
    enrichable = (
        "serverless_dbu_usd", "gateway_dbu_usd",
        "minutes_per_run", "gateway_dbu_per_hour",
    )
    for field_name in enrichable:
        if field_name not in overrides:
            unchanged.append(f"{field_name}: using default ({getattr(base, field_name)})")
    enrichment_log["unchanged_fields"] = unchanged

    enriched = RateCard(**{**base.__dict__, **overrides}) if overrides else base
    return enriched, enrichment_log
