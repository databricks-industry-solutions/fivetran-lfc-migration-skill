"""Turn a Fivetran inventory into a Lakeflow Connect migration plan.

The plan is the decision artifact: for every Fivetran connection it records the
target Lakeflow Connect shape, everything that has to happen first, and every
reason the migration might not be faithful. It is deliberately conservative --
where the inventory is uncertain, the plan says so rather than guessing, because
a silently wrong pipeline is worse than a flagged gap.
"""

from __future__ import annotations

import datetime as dt
import re
from typing import Any

from . import PLAN_SCHEMA_VERSION
from .catalog import Availability, Gateway, Scriptable, Target, lookup

STALENESS_DAYS = 90

# Fivetran attaches sync frequency to the connector. Databricks has no supported
# pipeline-level schedule, so each pipeline needs a companion job. Quartz format
# is: seconds minutes hours day-of-month month day-of-week.
CRON_BY_MINUTES: dict[int, str] = {
    15: "0 0/15 * * * ?",
    30: "0 0/30 * * * ?",
    60: "0 0 * * * ?",
    120: "0 0 0/2 * * ?",
    180: "0 0 0/3 * * ?",
    360: "0 0 0/6 * * ?",
    480: "0 0 0/8 * * ?",
    720: "0 0 0/12 * * ?",
    1440: "0 0 0 * * ?",
}

# Below an hour a periodic trigger cannot express the cadence at all, and at
# 1-5 minutes a cron job thrashes: every fire is a pipeline update with real
# startup cost. A continuous job is the honest equivalent.
CONTINUOUS_THRESHOLD_MINUTES = 5

DEFAULT_SYNC_MINUTES = 360

_INVALID_NAME = re.compile(r"[^a-z0-9_]+")


def to_identifier(*parts: str) -> str:
    """Build a Unity Catalog / pipeline-safe identifier from arbitrary text."""
    joined = "_".join(p for p in parts if p)
    cleaned = _INVALID_NAME.sub("_", joined.lower()).strip("_")
    return re.sub(r"_{2,}", "_", cleaned) or "unnamed"


def build_plan(
    inventory: dict[str, Any],
    target_catalog: str,
    mar: dict[str, Any] | None = None,
    include_paused: bool = False,
) -> dict[str, Any]:
    """Produce a migration plan from a discovery inventory."""
    connections = [
        c for c in inventory.get("connections", []) if include_paused or not c.get("paused")
    ]

    items = [_plan_connection(c, target_catalog, mar) for c in connections]

    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "target_catalog": target_catalog,
        "source_account": inventory.get("account", {}),
        "items": items,
        "summary": summarise_plan(items),
        "blockers": _collect(items, "blockers"),
        "warnings": _collect(items, "warnings"),
    }


def _plan_connection(
    connection: dict[str, Any], target_catalog: str, mar: dict[str, Any] | None
) -> dict[str, Any]:
    service = connection.get("service") or ""
    target = lookup(service)
    name = to_identifier(connection.get("destination_schema") or service, connection.get("id"))

    objects, obj_warnings = _plan_objects(connection, target, target_catalog)
    schedule = _plan_schedule(connection)
    blockers, warnings = _assess(connection, target, objects, schedule)

    return {
        "fivetran": {
            "connection_id": connection.get("id"),
            "service": service,
            "destination_schema": connection.get("destination_schema"),
            "paused": connection.get("paused"),
            "setup_state": connection.get("status", {}).get("setup_state"),
            "sync_frequency_minutes": connection.get("sync_frequency_minutes"),
            "networking_method": connection.get("networking_method"),
            "monthly_mar": _mar_for(connection, mar),
        },
        "target": {
            "connection_name": to_identifier(service, "conn", connection.get("id")),
            "connection_type": target.connection_type,
            "category": target.category.value,
            "availability": target.availability.value,
            "gateway": target.gateway.value,
            "scriptable": target.scriptable.value,
            "preferred_auth": target.preferred_auth,
            "effort": target.effort.value,
            "pipeline_name": name,
            "destination_catalog": target_catalog,
            "destination_schema": to_identifier(connection.get("destination_schema") or service),
            "alternative": target.alternative,
            "has_supported_tables": target.supported_tables is not None,
        },
        "objects": objects,
        "schedule": schedule,
        "prerequisites": _prerequisites(target),
        "blockers": blockers,
        "warnings": warnings + obj_warnings,
    }


def _plan_objects(
    connection: dict[str, Any], target: Target, target_catalog: str
) -> tuple[list[dict[str, Any]], list[str]]:
    """Translate enabled Fivetran tables into Lakeflow object specs.

    When the target has a ``supported_tables`` set, only tables in that set are
    emitted.  Fivetran tables absent from the set are excluded and a warning is
    generated so the plan clearly reports the coverage gap.
    """
    warnings: list[str] = []
    if not target.has_managed_connector:
        return [], warnings

    destination_schema = to_identifier(
        connection.get("destination_schema") or connection.get("service")
    )
    specs: list[dict[str, Any]] = []
    dropped: list[str] = []

    for table in connection.get("objects", []):
        if not table.get("enabled"):
            continue

        source_table = table.get("source_table") or ""

        if (
            target.supported_tables is not None
            and source_table.lower() not in {t.lower() for t in target.supported_tables}
        ):
            dropped.append(
                f"{table.get('source_schema', '?')}.{source_table}"
            )
            continue

        config: dict[str, Any] = {}

        # Fivetran HISTORY mode is SCD type 2. Anything else merges in place.
        scd_type = "SCD_TYPE_2" if table.get("retains_history") else "SCD_TYPE_1"
        config["scd_type"] = scd_type

        if table.get("primary_keys"):
            config["primary_keys"] = table["primary_keys"]
        elif table.get("primary_keys_known"):
            if target.has_managed_connector:
                warnings.append(
                    f"{table['source_schema']}.{table['source_table']} has no primary key "
                    "in Fivetran's column metadata, but the managed Lakeflow Connect "
                    f"connector ({target.connection_type}) will detect keys from the "
                    "source directly. Keeping SCD_TYPE_1; verify after first sync."
                )
            else:
                config["scd_type"] = "APPEND_ONLY"
                warnings.append(
                    f"{table['source_schema']}.{table['source_table']} has no primary key; "
                    "planned as APPEND_ONLY rather than a merge."
                )
        else:
            warnings.append(
                f"{table['source_schema']}.{table['source_table']} has unknown primary keys. "
                "Re-run discovery with --columns; SCD behaviour cannot be set safely without them."
            )

        if table.get("excluded_columns"):
            config["exclude_columns"] = table["excluded_columns"]

        if table.get("hashed_columns"):
            warnings.append(
                f"{table['source_schema']}.{table['source_table']} hashes "
                f"{len(table['hashed_columns'])} column(s) in Fivetran "
                f"({', '.join(table['hashed_columns'])}). Lakeflow Connect has no equivalent "
                "at ingest; reproduce with a downstream masking policy or transformation."
            )

        specs.append(
            {
                "type": "report" if target.connection_type == "WORKDAY_RAAS" else "table",
                "source_schema": target.fixed_source_schema or table.get("source_schema"),
                "fivetran_source_schema": table.get("source_schema"),
                "source_table": table.get("source_table"),
                "destination_catalog": target_catalog,
                "destination_schema": destination_schema,
                "destination_table": to_identifier(
                    table.get("destination_table") or table.get("source_table")
                ),
                "table_configuration": config,
                "primary_keys_known": table.get("primary_keys_known", False),
            }
        )

    if dropped:
        docs_hint = f" See {target.docs_url}" if target.docs_url else ""
        warnings.append(
            f"{len(dropped)} Fivetran table(s) have no Lakeflow Connect equivalent "
            f"in the {target.connection_type} connector and were excluded: "
            f"{', '.join(sorted(dropped))}.{docs_hint}"
        )

    if target.supported_tables is not None and target.last_verified:
        try:
            verified = dt.date.fromisoformat(target.last_verified)
            age = (dt.date.today() - verified).days
            if age > STALENESS_DAYS:
                docs_suffix = f" {target.docs_url}" if target.docs_url else ""
                warnings.append(
                    f"The {target.connection_type} supported-table list was last verified "
                    f"on {target.last_verified} ({age} days ago). Check the connector "
                    f"reference for newly added tables.{docs_suffix}"
                )
        except ValueError:
            pass

    rewritten = {
        s["fivetran_source_schema"]
        for s in specs
        if s["fivetran_source_schema"] != s["source_schema"]
    }
    if rewritten:
        warnings.append(
            f"Source schema rewritten from {', '.join(sorted(rewritten))} to "
            f"'{target.fixed_source_schema}': the {target.connection_type} connector "
            "addresses objects under a schema it defines, not the one Fivetran used."
        )

    return specs, warnings


def _plan_schedule(connection: dict[str, Any]) -> dict[str, Any]:
    """Map a Fivetran sync frequency onto a Lakeflow Job trigger.

    There is no supported pipeline-level schedule, so every ingestion pipeline
    needs a companion job. max_concurrent_runs is pinned to 1 because Fivetran
    serialises syncs and an unbounded cron job would not.
    """
    minutes = connection.get("sync_frequency_minutes") or DEFAULT_SYNC_MINUTES

    if minutes <= CONTINUOUS_THRESHOLD_MINUTES:
        return {
            "mode": "continuous",
            "fivetran_minutes": minutes,
            "max_concurrent_runs": 1,
            "note": (
                f"Fivetran syncs every {minutes} min. A cron job at that cadence would "
                "thrash, since each fire is a full pipeline update. A continuous job is "
                "the closer equivalent, but there is no documented minimum interval for "
                "managed ingestion and source API limits may bind. Confirm before cutover."
            ),
        }

    cron = CRON_BY_MINUTES.get(minutes)
    if not cron:
        nearest = min(CRON_BY_MINUTES, key=lambda m: abs(m - minutes))
        return {
            "mode": "cron",
            "fivetran_minutes": minutes,
            "quartz_cron_expression": CRON_BY_MINUTES[nearest],
            "timezone_id": "UTC",
            "max_concurrent_runs": 1,
            "note": (
                f"No exact cron for {minutes} min; using the nearest supported "
                f"cadence ({nearest} min)."
            ),
        }

    return {
        "mode": "cron",
        "fivetran_minutes": minutes,
        "quartz_cron_expression": cron,
        "timezone_id": "UTC",
        "max_concurrent_runs": 1,
        "note": "",
    }


def _prerequisites(target: Target) -> dict[str, Any]:
    steps: list[str] = []
    if target.source_prerequisites:
        steps.append(target.source_prerequisites)
    if target.gateway is Gateway.REQUIRED:
        steps.append(
            "Create an ingestion gateway pipeline first. It runs continuously on classic "
            "compute and is billed even while the ingestion pipeline is idle. The ingestion "
            "pipeline references it by the gateway's pipeline_id."
        )
    return {
        "steps": steps,
        "automatable": target.prerequisites_automatable,
        "auth_note": target.auth_note,
    }


def _assess(
    connection: dict[str, Any],
    target: Target,
    objects: list[dict[str, Any]],
    schedule: dict[str, Any],
) -> tuple[list[str], list[str]]:
    """Separate what stops the migration from what merely needs attention."""
    blockers: list[str] = []
    warnings: list[str] = []
    service = connection.get("service")

    if not target.has_managed_connector:
        blockers.append(
            f"No managed Lakeflow Connect connector for '{service}'. {target.alternative}"
        )
        return blockers, warnings

    # A browser sign-in costs one manual step for the connection only; the
    # pipeline and job are still generated and reference it by name.
    if target.scriptable is Scriptable.NO:
        warnings.append(
            f"'{service}' uses browser-based OAuth only, so its Unity Catalog connection "
            "cannot be created programmatically. A human must create it once in Catalog "
            "Explorer, using the connection name in the bundle, before deploying; the "
            "pipeline and job are generated and deploy normally afterwards."
        )
    elif target.scriptable is Scriptable.CONDITIONAL:
        auth = f" (generated as {target.preferred_auth})" if target.preferred_auth else ""
        warnings.append(f"Conditionally scriptable{auth}: {target.auth_note}")

    if not target.prerequisites_automatable:
        warnings.append(
            f"Source-side setup for '{service}' cannot be fully scripted; generate a "
            "runbook for the source administrator."
        )

    if target.availability in (Availability.BETA, Availability.PUBLIC_PREVIEW):
        state = target.availability.value.replace("_", " ")
        warnings.append(
            f"The {target.connection_type} connector is {state}; enrollment via the "
            "Databricks account team may be required."
        )
    elif target.availability is Availability.UNVERIFIED:
        warnings.append(
            f"Release state for {target.connection_type} was not verified. Confirm against "
            "the current Lakeflow Connect connector list before committing to a date."
        )

    if not objects:
        warnings.append(
            "No enabled tables found. Either nothing is selected in Fivetran, or discovery "
            "ran with --no-schemas."
        )

    unknown_keys = [o for o in objects if not o["primary_keys_known"]]
    if unknown_keys:
        warnings.append(
            f"{len(unknown_keys)} of {len(objects)} tables have unknown primary keys. "
            "Re-run discovery with --columns before generating pipelines."
        )

    if connection.get("networking_method") not in (None, "Directly"):
        warnings.append(
            f"Source reachable over {connection['networking_method']}. Private networking "
            "must be designed on the Databricks side before ingestion will connect."
        )

    if connection.get("status", {}).get("setup_state") == "broken":
        warnings.append("Connection is broken in Fivetran; its configuration may be stale.")

    if schedule.get("note"):
        warnings.append(schedule["note"])

    return blockers, warnings


def _mar_for(connection: dict[str, Any], mar: dict[str, Any] | None) -> int | None:
    """Attach MAR to a connection.

    Fivetran's Platform Connector keys MAR by connection *name*, which is the
    destination schema, not the connection id the REST API returns.
    """
    if not mar:
        return None
    by_connection = mar.get("by_connection") or {}
    return by_connection.get(connection.get("destination_schema"))


def _collect(items: list[dict[str, Any]], key: str) -> list[dict[str, str]]:
    out = []
    for item in items:
        for message in item.get(key, []):
            out.append({"connection": item["fivetran"]["connection_id"], "message": message})
    return out


def summarise_plan(items: list[dict[str, Any]]) -> dict[str, Any]:
    by_effort: dict[str, int] = {}
    by_category: dict[str, int] = {}
    for item in items:
        effort = item["target"]["effort"]
        category = item["target"]["category"]
        by_effort[effort] = by_effort.get(effort, 0) + 1
        by_category[category] = by_category.get(category, 0) + 1

    migratable = [i for i in items if not i["blockers"]]
    needs_gateway = [i for i in items if i["target"]["gateway"] == Gateway.REQUIRED.value]
    manual_connections = [i for i in migratable if i["target"]["scriptable"] == Scriptable.NO.value]

    tables_dropped = sum(
        1
        for i in items
        for w in i.get("warnings", [])
        if "have no Lakeflow Connect equivalent" in w
    )

    return {
        "connections_total": len(items),
        "connections_migratable": len(migratable),
        "connections_blocked": len(items) - len(migratable),
        "connections_manual_sign_in": len(manual_connections),
        "tables_total": sum(len(i["objects"]) for i in items),
        "tables_dropped": tables_dropped,
        "tables_scd2": sum(
            1
            for i in items
            for o in i["objects"]
            if o["table_configuration"].get("scd_type") == "SCD_TYPE_2"
        ),
        "gateways_required": len(needs_gateway),
        "jobs_required": len(migratable),
        "by_effort": by_effort,
        "by_category": by_category,
    }
