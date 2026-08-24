"""Translate a Fivetran inventory into a Lakeflow Connect migration plan.

The plan is the reviewable middle artifact: a human (or the agent, with the
customer) edits it before any Databricks resource is generated. Nothing here
calls an API.
"""

from __future__ import annotations

import datetime as dt
import re
from typing import Any

from . import PLAN_SCHEMA_VERSION
from .catalog import load_catalog, resolve, verdict_rank

# Fivetran sync_mode -> Lakeflow Connect table_configuration.scd_type.
SCD_BY_SYNC_MODE = {
    "HISTORY": "SCD_TYPE_2",
    "SOFT_DELETE_HISTORY": "SCD_TYPE_2",
    "SOFT_DELETE": "SCD_TYPE_1",
    "LIVE": "SCD_TYPE_1",
    "LEGACY": "SCD_TYPE_1",
}

#: When more than this fraction of a table's columns are deselected in Fivetran,
#: emit an allowlist (include_columns) instead of a denylist (exclude_columns).
#: Lakeflow Connect accepts one or the other per table, never both.
INCLUDE_LIST_THRESHOLD = 0.5

_INVALID_NAME_CHARS = re.compile(r"[^a-z0-9_]+")


def build_plan(
    inventory: dict[str, Any],
    target_catalog: str,
    target_schema: str | None = None,
    name_prefix: str = "lfc",
    include_paused: bool = False,
    catalog_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Produce a migration plan from a discovery inventory."""
    catalog_data = catalog_data or load_catalog()
    connections: list[dict[str, Any]] = []

    for connection in inventory.get("connections", []):
        if connection.get("paused") and not include_paused:
            continue
        connections.append(
            _plan_connection(
                connection,
                target_catalog=target_catalog,
                target_schema=target_schema,
                name_prefix=name_prefix,
                catalog_data=catalog_data,
            )
        )

    connections.sort(key=lambda c: (verdict_rank(c["verdict"]), c["fivetran_service"]))

    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "inventory_generated_at": inventory.get("generated_at"),
        "catalog_last_reviewed": catalog_data.get("last_reviewed"),
        "target": {"catalog": target_catalog, "schema": target_schema},
        "connections": connections,
        "summary": _summarise_plan(connections),
    }


def _plan_connection(
    connection: dict[str, Any],
    target_catalog: str,
    target_schema: str | None,
    name_prefix: str,
    catalog_data: dict[str, Any],
) -> dict[str, Any]:
    service = connection.get("service") or "unknown"
    resolution = resolve(service, catalog_data)
    target = resolution["target"] or {}

    slug = sanitize_name(f"{service}_{connection.get('destination_schema') or connection['id']}")
    warnings: list[str] = []
    blockers: list[str] = []
    manual_steps: list[str] = []
    notes = list(resolution["notes"])

    if resolution["verdict"] == "unsupported":
        blockers.append(
            f"Lakeflow Connect has no managed connector for Fivetran service '{service}'."
        )
    elif resolution["verdict"] == "unknown":
        blockers.append(f"Fivetran service '{service}' is not in the connector catalog.")
    elif resolution["verdict"] == "preview":
        warnings.append(
            f"The {target.get('display_name', service)} connector is "
            f"{target.get('status')}. Confirm the customer's workspace is enrolled."
        )

    if connection.get("status", {}).get("setup_state") == "broken":
        warnings.append(
            "The Fivetran connection is in 'broken' setup state, so its discovered "
            "schema may be stale. Re-verify table selection before deploying."
        )

    networking = connection.get("networking_method")
    if networking and networking.lower() not in ("directly", "direct"):
        manual_steps.append(
            f"Source is reached over '{networking}' in Fivetran. Reproduce network "
            "access from Databricks (Private Link / NCC, or place the ingestion "
            "gateway on a network with a route to the source)."
        )

    objects = [
        _plan_object(obj, target_catalog, target_schema, connection)
        for obj in connection.get("objects", [])
        if obj.get("enabled")
    ]

    hashed = sorted({col for obj in objects for col in obj["notes_hashed_columns"]})
    if hashed:
        warnings.append(
            "Fivetran hashes these columns at ingest, which Lakeflow Connect does "
            f"not do: {', '.join(hashed)}. Either ingest them raw and hash in a "
            "downstream table, or exclude them and re-derive."
        )

    if not objects and resolution["verdict"] not in ("unsupported", "unknown"):
        warnings.append(
            "No enabled tables were discovered. Either the connector does not "
            "expose a schema config, or discovery ran with --no-schemas."
        )

    schema_handling = connection.get("schema_change_handling")
    if schema_handling and schema_handling != "ALLOW_ALL":
        notes.append(
            f"Fivetran schema_change_handling is '{schema_handling}'. Lakeflow "
            "Connect ingests new columns automatically for table-level selections; "
            "use include_columns to pin an explicit allowlist instead."
        )

    return {
        "fivetran_connection_id": connection.get("id"),
        "fivetran_service": service,
        "fivetran_group_id": connection.get("group_id"),
        "fivetran_destination_schema": connection.get("destination_schema"),
        "fivetran_paused": bool(connection.get("paused")),
        "verdict": resolution["verdict"],
        "target": {
            "connector": resolution["target_name"],
            "display_name": target.get("display_name"),
            "status": target.get("status"),
            "source_type": target.get("source_type"),
            "connection_type": target.get("connection_type"),
            "requires_gateway": bool(target.get("requires_gateway")),
            "object_spec": target.get("object_spec", "table"),
        },
        "uc_connection_name": sanitize_name(f"{name_prefix}_{slug}"),
        "gateway_pipeline_name": (
            sanitize_name(f"{name_prefix}_{slug}_gateway")
            if target.get("requires_gateway")
            else None
        ),
        "pipeline_name": sanitize_name(f"{name_prefix}_{slug}_ingest"),
        "schedule": _plan_schedule(connection, target),
        "objects": objects,
        "source_prerequisites": resolution["source_prerequisites"],
        "alternatives": resolution["alternatives"],
        "notes": notes,
        "warnings": warnings,
        "blockers": blockers,
        "manual_steps": manual_steps,
    }


def _plan_object(
    obj: dict[str, Any],
    target_catalog: str,
    target_schema: str | None,
    connection: dict[str, Any],
) -> dict[str, Any]:
    sync_mode = obj.get("sync_mode")
    scd_type = SCD_BY_SYNC_MODE.get(sync_mode or "", "SCD_TYPE_1")

    columns = obj.get("columns") or []
    excluded = [c for c in columns if not c["enabled"] and not c["is_primary_key"]]
    include_columns: list[str] = []
    exclude_columns: list[str] = []
    if columns and excluded:
        if len(excluded) / len(columns) > INCLUDE_LIST_THRESHOLD:
            include_columns = [c["source_column"] for c in columns if c["enabled"]]
        else:
            exclude_columns = [c["source_column"] for c in excluded]

    notes: list[str] = []
    primary_keys = list(obj.get("primary_keys") or [])
    if not primary_keys:
        notes.append(
            "No primary key was discovered in Fivetran. Lakeflow Connect needs "
            "primary_keys to deduplicate; set it manually or accept append-only."
        )
    if scd_type == "SCD_TYPE_2" and not primary_keys:
        notes.append("SCD type 2 requires primary_keys. Fill this in before deploying.")
    if sync_mode == "LEGACY":
        notes.append(
            "Fivetran sync_mode 'LEGACY' is append-only with no deletes. Mapped to "
            "SCD type 1; confirm this matches downstream expectations."
        )

    return {
        "source_schema": obj.get("source_schema"),
        "source_table": obj.get("source_table"),
        "destination_catalog": target_catalog,
        "destination_schema": target_schema
        or obj.get("destination_schema")
        or connection.get("destination_schema"),
        "destination_table": obj.get("destination_table") or obj.get("source_table"),
        "fivetran_sync_mode": sync_mode,
        "scd_type": scd_type,
        "primary_keys": primary_keys,
        "include_columns": include_columns,
        "exclude_columns": exclude_columns,
        "notes": notes,
        # Surfaced to the connection level, not emitted into the bundle.
        "notes_hashed_columns": list(obj.get("hashed_columns") or []),
    }


def _plan_schedule(connection: dict[str, Any], target: dict[str, Any]) -> dict[str, Any]:
    """Map Fivetran's sync_frequency to a Lakeflow Connect trigger."""
    minutes = connection.get("sync_frequency_minutes")
    if target.get("requires_gateway"):
        # Gateway-based CDC sources stream changes; the gateway runs continuously
        # and the ingestion pipeline applies them on a trigger.
        return {
            "mode": "triggered",
            "cron": _cron_for_minutes(minutes),
            "gateway_continuous": True,
            "fivetran_sync_frequency_minutes": minutes,
        }
    if minutes and minutes <= 5:
        return {
            "mode": "continuous",
            "cron": None,
            "gateway_continuous": False,
            "fivetran_sync_frequency_minutes": minutes,
        }
    return {
        "mode": "triggered",
        "cron": _cron_for_minutes(minutes),
        "gateway_continuous": False,
        "fivetran_sync_frequency_minutes": minutes,
    }


def _cron_for_minutes(minutes: int | None) -> str:
    """Build a Quartz cron expression approximating a Fivetran sync frequency."""
    if not minutes or minutes >= 1440:
        return "0 0 2 * * ?"
    if minutes >= 720:
        return "0 0 2,14 * * ?"
    if minutes >= 60:
        hours = max(1, minutes // 60)
        return f"0 0 0/{hours} * * ?"
    step = max(5, minutes)
    return f"0 0/{step} * * * ?"


def sanitize_name(value: str) -> str:
    """Normalise a string into a safe Databricks resource/identifier name."""
    cleaned = _INVALID_NAME_CHARS.sub("_", (value or "").lower()).strip("_")
    cleaned = re.sub(r"_{2,}", "_", cleaned)
    return cleaned or "unnamed"


def _summarise_plan(connections: list[dict[str, Any]]) -> dict[str, Any]:
    by_verdict: dict[str, int] = {}
    for connection in connections:
        by_verdict[connection["verdict"]] = by_verdict.get(connection["verdict"], 0) + 1

    migratable = [c for c in connections if not c["blockers"]]
    return {
        "connections_planned": len(connections),
        "connections_migratable": len(migratable),
        "connections_blocked": len(connections) - len(migratable),
        "by_verdict": by_verdict,
        "gateways_required": sum(1 for c in connections if c["target"]["requires_gateway"]),
        "tables_planned": sum(len(c["objects"]) for c in connections),
        "tables_scd_type_2": sum(
            1 for c in connections for o in c["objects"] if o["scd_type"] == "SCD_TYPE_2"
        ),
        "manual_step_count": sum(len(c["manual_steps"]) for c in connections),
    }
