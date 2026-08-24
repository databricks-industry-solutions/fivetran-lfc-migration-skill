"""Normalise raw Fivetran API responses into a stable inventory document.

The inventory is the single artifact every later stage reads, so its shape is
versioned (``schema_version``) and deliberately independent of Fivetran's
response quirks.

The most important of those quirks: ``GET /connections/{id}/schemas`` returns
only the columns that were *explicitly overridden*, not the full column list. A
table with an empty ``columns`` map is one nobody customised, where every column
syncs. That asymmetry is handled explicitly below rather than papered over,
because getting it wrong silently drops columns from the migration plan.
"""

from __future__ import annotations

import datetime as dt
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from . import INVENTORY_SCHEMA_VERSION
from .fivetran import FivetranClient, FivetranRateLimitError, redact

log = logging.getLogger(__name__)

# Fivetran's table-level sync_mode values. HISTORY is its SCD type 2 equivalent
# and is the highest-effort behaviour to reproduce in Lakeflow Connect.
SYNC_MODE_HISTORY = "HISTORY"

# Only a Connected connection can serve the per-table columns endpoint.
CONNECTED = "connected"


def build_inventory(
    client: FivetranClient,
    group_ids: list[str] | None = None,
    include_schemas: bool = True,
    include_columns: bool = False,
    max_workers: int = 8,
) -> dict[str, Any]:
    """Discover the full Fivetran setup reachable by the client's credentials.

    ``include_columns`` triggers a per-table crawl of the columns endpoint. It
    is the only way to learn primary keys for tables nobody has customised, but
    it costs one rate-limited request per table, so it is opt-in.
    """
    account = _describe_account(client)

    groups = [
        _normalise_group(group, client)
        for group in client.list_groups()
        if not group_ids or group.get("id") in group_ids
    ]

    connections: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for group in groups:
        for raw in client.list_connections(group["id"]):
            if raw.get("id") in seen_ids:
                continue
            seen_ids.add(raw.get("id"))
            connections.append(_normalise_connection(raw, default_group_id=group["id"]))

    if include_schemas and connections:
        _attach_schemas(client, connections, max_workers=max_workers)
        if include_columns:
            _attach_columns(client, connections, max_workers=max_workers)

    catalog = {c.get("id"): c for c in client.list_connector_types() if c.get("id")}
    for connection in connections:
        _attach_connector_metadata(connection, catalog)

    return {
        "schema_version": INVENTORY_SCHEMA_VERSION,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source": "fivetran-api",
        "account": account,
        "groups": groups,
        "connections": connections,
        "transformations": _normalise_transformations(client),
        "summary": summarise(connections),
        "warnings": list(client.warnings),
    }


def _describe_account(client: FivetranClient) -> dict[str, Any]:
    info = client.get_account_info()
    # A scoped key inherits its owner's RBAC and will quietly return a partial
    # estate. Surface that rather than letting it look like a complete picture.
    scoped = bool(info.get("user_id")) and not info.get("system_key_id")
    if scoped:
        client.warnings.append(
            "Discovery used a scoped API key, which only sees what its owning user can "
            "see. Connections outside that user's RBAC scope are missing from this "
            "inventory. Re-run with a system key for a complete estate."
        )
    return {
        "account_id": info.get("account_id"),
        "account_name": info.get("account_name"),
        "key_type": "scoped" if scoped else "system",
        "coverage_complete": not scoped,
    }


def _normalise_group(group: dict[str, Any], client: FivetranClient) -> dict[str, Any]:
    destination = client.get_destination(group.get("id", ""))
    return {
        "id": group.get("id"),
        "name": group.get("name"),
        "created_at": group.get("created_at"),
        "destination": {
            "id": destination.get("id"),
            "service": destination.get("service"),
            "region": destination.get("region"),
            "setup_status": destination.get("setup_status"),
            "networking_method": destination.get("networking_method"),
            "time_zone_offset": destination.get("time_zone_offset"),
            "config": redact(destination.get("config") or {}),
        },
    }


def _normalise_connection(raw: dict[str, Any], default_group_id: str) -> dict[str, Any]:
    status = raw.get("status") or {}
    return {
        "id": raw.get("id"),
        "group_id": raw.get("group_id") or default_group_id,
        # "service" is Fivetran's connector-type identifier, e.g. "salesforce".
        "service": raw.get("service"),
        "service_version": raw.get("service_version"),
        # "schema" is the destination schema (or schema prefix) Fivetran writes to.
        "destination_schema": raw.get("schema"),
        "destination_schema_names": raw.get("destination_schema_names"),
        "paused": bool(raw.get("paused")),
        "sync_frequency_minutes": raw.get("sync_frequency"),
        "schedule_type": raw.get("schedule_type"),
        "daily_sync_time": raw.get("daily_sync_time"),
        "networking_method": raw.get("networking_method"),
        "connected_by": raw.get("connected_by"),
        "created_at": raw.get("created_at"),
        "succeeded_at": raw.get("succeeded_at"),
        "failed_at": raw.get("failed_at"),
        "data_delay_sensitivity": raw.get("data_delay_sensitivity"),
        "data_delay_threshold": raw.get("data_delay_threshold"),
        "status": {
            "setup_state": status.get("setup_state"),
            "sync_state": status.get("sync_state"),
            "update_state": status.get("update_state"),
            "is_historical_sync": status.get("is_historical_sync"),
            "warning_count": len(status.get("warnings") or []),
            "task_count": len(status.get("tasks") or []),
        },
        "config": redact(raw.get("config") or {}),
        # source_sync_details has no stable schema across connectors; keep it
        # opaque but redacted, since it can carry source scope identifiers.
        "source_sync_details": redact(raw.get("source_sync_details") or {}),
        "schema_change_handling": None,
        "objects": [],
        "counts": {},
    }


def _attach_connector_metadata(connection: dict[str, Any], catalog: dict[str, Any]) -> None:
    meta = catalog.get(connection.get("service")) or {}
    features = [f.get("id") for f in (meta.get("supported_features") or []) if f.get("id")]
    connection["connector_metadata"] = {
        "name": meta.get("name"),
        "type": meta.get("type"),
        "connector_class": meta.get("connector_class"),
        "service_status": meta.get("service_status"),
        "supported_features": features,
        "docs_url": meta.get("link_to_docs"),
    }


def _attach_schemas(
    client: FivetranClient, connections: list[dict[str, Any]], max_workers: int
) -> None:
    def fetch(connection: dict[str, Any]) -> None:
        payload = client.get_connection_schemas(connection["id"])
        connection["schema_change_handling"] = payload.get("schema_change_handling")
        connection["enable_new_by_default"] = payload.get("enable_new_by_default")
        objects = flatten_schemas(payload)
        connection["objects"] = objects
        connection["counts"] = count_objects(objects)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        list(pool.map(fetch, connections))


def _attach_columns(
    client: FivetranClient, connections: list[dict[str, Any]], max_workers: int
) -> None:
    """Resolve the true column list for tables that support column config.

    Costs one rate-limited request per table. Aborts the whole crawl on a long
    Retry-After rather than stalling, leaving already-resolved tables intact.
    """
    targets: list[tuple[dict[str, Any], dict[str, Any]]] = [
        (connection, table)
        for connection in connections
        if connection["status"].get("setup_state") == CONNECTED
        for table in connection["objects"]
        if table["enabled"] and table["supports_columns_config"]
    ]
    if not targets:
        return

    log.info("Resolving columns for %d tables", len(targets))
    aborted = False

    def fetch(target: tuple[dict[str, Any], dict[str, Any]]) -> None:
        nonlocal aborted
        if aborted:
            return
        connection, table = target
        try:
            payload = client.get_table_columns(
                connection["id"], table["source_schema"], table["source_table"]
            )
        except FivetranRateLimitError as exc:
            aborted = True
            client.warnings.append(f"column resolution stopped early: {exc}")
            return
        columns = payload.get("columns")
        if not isinstance(columns, dict) or not columns:
            return
        _apply_columns(table, _flatten_columns(columns), complete=True)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        list(pool.map(fetch, targets))

    for connection in connections:
        connection["counts"] = count_objects(connection["objects"])


def flatten_schemas(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten Fivetran's nested schema config into a list of table records.

    Fivetran keys schemas, tables, and columns by name inside nested maps. A flat
    list is far easier to map, diff, and count.
    """
    tables: list[dict[str, Any]] = []
    for schema_name, schema in (payload.get("schemas") or {}).items():
        if not isinstance(schema, dict):
            continue
        schema_enabled = schema.get("enabled", True)
        for table_name, table in (schema.get("tables") or {}).items():
            if not isinstance(table, dict):
                continue
            sync_mode = table.get("sync_mode")
            record = {
                "source_schema": schema_name,
                "source_table": table_name,
                "destination_schema": schema.get("name_in_destination") or schema_name,
                "destination_table": table.get("name_in_destination") or table_name,
                "schema_enabled": bool(schema_enabled),
                "enabled": bool(schema_enabled) and bool(table.get("enabled", False)),
                "enabled_patch_settings": table.get("enabled_patch_settings") or {},
                # sync_mode is absent unless the connector supports switching
                # modes, so absence is not the same as SOFT_DELETE.
                "sync_mode": sync_mode,
                "retains_history": sync_mode == SYNC_MODE_HISTORY,
                "supports_history_mode": bool(table.get("supports_history_mode", False)),
                "supports_columns_config": bool(table.get("supports_columns_config", True)),
            }
            _apply_columns(record, _flatten_columns(table.get("columns") or {}), complete=False)
            tables.append(record)
    return tables


def _apply_columns(
    table: dict[str, Any], columns: list[dict[str, Any]], complete: bool
) -> None:
    """Attach column state to a table record.

    ``complete`` distinguishes the exhaustive list from the columns endpoint
    from the partial, override-only list in the schemas response. Exclusions and
    hashing are trustworthy either way, because opting a column out *is* an
    override. Primary keys are not: a PK nobody touched never appears.
    """
    table["columns"] = columns
    table["columns_complete"] = complete
    table["hashed_columns"] = sorted(c["source_column"] for c in columns if c["hashed"])
    table["excluded_columns"] = sorted(
        c["source_column"] for c in columns if not c["enabled"] and not c["is_primary_key"]
    )
    primary_keys = sorted(c["source_column"] for c in columns if c["is_primary_key"])
    table["primary_keys"] = primary_keys
    # Without the exhaustive list an empty primary_keys means "unknown", not
    # "none". Downstream stages must not emit a Lakeflow table_configuration
    # claiming no primary key on that basis.
    table["primary_keys_known"] = complete or bool(primary_keys)


def _flatten_columns(columns: dict[str, Any]) -> list[dict[str, Any]]:
    flat: list[dict[str, Any]] = []
    for name, column in columns.items():
        if not isinstance(column, dict):
            continue
        flat.append(
            {
                "source_column": name,
                "destination_column": column.get("name_in_destination") or name,
                "enabled": bool(column.get("enabled", True)),
                "hashed": bool(column.get("hashed", False)),
                "is_primary_key": bool(column.get("is_primary_key", False)),
            }
        )
    return flat


def count_objects(objects: list[dict[str, Any]]) -> dict[str, int]:
    enabled = [t for t in objects if t["enabled"]]
    return {
        "tables_total": len(objects),
        "tables_enabled": len(enabled),
        "tables_history_mode": sum(1 for t in enabled if t["retains_history"]),
        "tables_columns_resolved": sum(1 for t in enabled if t["columns_complete"]),
        "tables_primary_keys_unknown": sum(1 for t in enabled if not t["primary_keys_known"]),
        "columns_excluded": sum(len(t["excluded_columns"]) for t in enabled),
        "columns_hashed": sum(len(t["hashed_columns"]) for t in enabled),
    }


def _normalise_transformations(client: FivetranClient) -> list[dict[str, Any]]:
    records = []
    for raw in client.list_transformations():
        schedule = raw.get("schedule") or {}
        config = raw.get("transformation_config") or {}
        records.append(
            {
                "id": raw.get("id"),
                # QUICKSTART models are Fivetran-proprietary and must be rebuilt;
                # DBT_CORE projects port to Databricks with modest effort.
                "type": raw.get("type"),
                "name": config.get("name"),
                "paused": bool(raw.get("paused")),
                "output_model_names": raw.get("output_model_names") or [],
                "schedule_type": schedule.get("schedule_type"),
                # The dependency edge back to the connections feeding this
                # transformation, which orders the cutover.
                "upstream_connection_ids": schedule.get("connection_ids") or [],
            }
        )
    return records


def summarise(connections: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate counts used by the discovery report and the cost model."""
    by_service: dict[str, int] = {}
    for connection in connections:
        service = connection.get("service") or "unknown"
        by_service[service] = by_service.get(service, 0) + 1

    active = [c for c in connections if not c["paused"]]

    def total(key: str) -> int:
        return sum(c.get("counts", {}).get(key, 0) for c in connections)

    return {
        "connections_total": len(connections),
        "connections_active": len(active),
        "connections_paused": len(connections) - len(active),
        "connections_broken": sum(
            1 for c in connections if c["status"].get("setup_state") == "broken"
        ),
        "distinct_services": len(by_service),
        "connections_by_service": dict(sorted(by_service.items(), key=lambda kv: (-kv[1], kv[0]))),
        "tables_enabled": total("tables_enabled"),
        "tables_history_mode": total("tables_history_mode"),
        "tables_primary_keys_unknown": total("tables_primary_keys_unknown"),
        "columns_hashed": total("columns_hashed"),
        "non_direct_networking": sum(
            1 for c in active if c.get("networking_method") not in (None, "Directly")
        ),
    }
