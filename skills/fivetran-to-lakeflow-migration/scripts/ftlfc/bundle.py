"""Emit a Databricks Asset Bundle from a migration plan.

One bundle per migration. Each migratable Fivetran connection becomes:

* a Unity Catalog connection (emitted as reviewable SQL, not as a bundle
  resource, because credentials must not be committed),
* an ingestion gateway pipeline, for CDC sources that require one,
* a managed ingestion pipeline carrying the table selection, SCD type, and
  column selection discovered from Fivetran,
* a Lakeflow job that triggers the pipeline on a schedule, when the Fivetran
  connection was not effectively continuous.

Blocked connections are skipped and listed in MIGRATION_NOTES.md.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

BUNDLE_YAML = "databricks.yml"
RESOURCES_DIR = "resources"
CONNECTIONS_SQL = "connections/create_connections.sql"
NOTES_FILE = "MIGRATION_NOTES.md"


class _BlockStyleDumper(yaml.SafeDumper):
    """Dumper that indents sequences under their key, matching Databricks docs."""

    def increase_indent(self, flow: bool = False, indentless: bool = False) -> None:
        super().increase_indent(flow, False)


def write_bundle(
    plan: dict[str, Any],
    output_dir: Path,
    bundle_name: str,
    host: str | None = None,
    default_catalog: str | None = None,
) -> dict[str, Any]:
    """Materialise the bundle on disk. Returns a summary of what was written."""
    output_dir = Path(output_dir)
    (output_dir / RESOURCES_DIR).mkdir(parents=True, exist_ok=True)
    (output_dir / "connections").mkdir(parents=True, exist_ok=True)

    migratable = [c for c in plan["connections"] if not c["blockers"] and c["objects"]]
    skipped = [c for c in plan["connections"] if c not in migratable]

    written: list[str] = []
    for connection in migratable:
        for filename, document in _resources_for(connection).items():
            path = output_dir / RESOURCES_DIR / filename
            path.write_text(_dump(document))
            written.append(str(path.relative_to(output_dir)))

    (output_dir / BUNDLE_YAML).write_text(
        _dump(_bundle_document(bundle_name, host, default_catalog or plan["target"]["catalog"]))
    )
    written.append(BUNDLE_YAML)

    (output_dir / CONNECTIONS_SQL).write_text(_connections_sql(migratable))
    written.append(CONNECTIONS_SQL)

    (output_dir / NOTES_FILE).write_text(_notes(plan, migratable, skipped))
    written.append(NOTES_FILE)

    return {
        "output_dir": str(output_dir),
        "files": written,
        "connections_emitted": len(migratable),
        "connections_skipped": len(skipped),
        "pipelines": sum(
            1 + (1 if c.get("gateway_pipeline_name") else 0) for c in migratable
        ),
    }


def _bundle_document(name: str, host: str | None, catalog: str) -> dict[str, Any]:
    targets: dict[str, Any] = {
        "dev": {
            "mode": "development",
            "default": True,
            "variables": {"catalog": catalog},
        },
        "prod": {
            "mode": "production",
            "variables": {"catalog": catalog},
        },
    }
    if host:
        targets["dev"]["workspace"] = {"host": host}
        targets["prod"]["workspace"] = {"host": host}

    return {
        "bundle": {"name": name},
        "variables": {
            "catalog": {
                "description": "Unity Catalog catalog that ingested tables land in",
                "default": catalog,
            }
        },
        "include": [f"{RESOURCES_DIR}/*.yml"],
        "targets": targets,
    }


def _resources_for(connection: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Build the resource documents for one Fivetran connection."""
    documents: dict[str, dict[str, Any]] = {}
    pipelines: dict[str, Any] = {}
    jobs: dict[str, Any] = {}

    gateway_key = connection.get("gateway_pipeline_name")
    if gateway_key:
        pipelines[gateway_key] = _gateway_pipeline(connection)

    pipeline_key = connection["pipeline_name"]
    pipelines[pipeline_key] = _ingestion_pipeline(connection, gateway_key)

    schedule = connection["schedule"]
    if schedule["mode"] == "triggered" and schedule.get("cron"):
        jobs[f"{pipeline_key}_schedule"] = _schedule_job(connection, pipeline_key)

    document: dict[str, Any] = {"resources": {"pipelines": pipelines}}
    if jobs:
        document["resources"]["jobs"] = jobs

    documents[f"{pipeline_key}.yml"] = document
    return documents


def _gateway_pipeline(connection: dict[str, Any]) -> dict[str, Any]:
    first = connection["objects"][0]
    staging_schema = f"{first['destination_schema']}_staging"
    return {
        "name": connection["gateway_pipeline_name"],
        "gateway_definition": {
            "connection_name": connection["uc_connection_name"],
            "gateway_storage_catalog": "${var.catalog}",
            "gateway_storage_schema": staging_schema,
            "gateway_storage_name": f"{connection['gateway_pipeline_name']}_storage",
        },
    }


def _ingestion_pipeline(connection: dict[str, Any], gateway_key: str | None) -> dict[str, Any]:
    ingestion: dict[str, Any] = {}
    if gateway_key:
        # The gateway owns the source credentials; the ingestion pipeline points
        # at the gateway rather than at the UC connection directly.
        ingestion["ingestion_gateway_id"] = f"${{resources.pipelines.{gateway_key}.id}}"
    else:
        ingestion["connection_name"] = connection["uc_connection_name"]

    source_type = connection["target"].get("source_type")
    if source_type:
        ingestion["source_type"] = source_type

    spec_key = connection["target"].get("object_spec") or "table"
    ingestion["objects"] = [_object_spec(obj, spec_key) for obj in connection["objects"]]

    pipeline: dict[str, Any] = {
        "name": connection["pipeline_name"],
        "catalog": "${var.catalog}",
        "schema": connection["objects"][0]["destination_schema"],
        "serverless": True,
        "channel": "PREVIEW",
        "continuous": connection["schedule"]["mode"] == "continuous",
        "ingestion_definition": ingestion,
    }
    return pipeline


def _object_spec(obj: dict[str, Any], spec_key: str) -> dict[str, Any]:
    table_configuration: dict[str, Any] = {"scd_type": obj["scd_type"]}
    if obj["primary_keys"]:
        table_configuration["primary_keys"] = obj["primary_keys"]
    # Lakeflow Connect accepts include_columns or exclude_columns, never both.
    if obj["include_columns"]:
        table_configuration["include_columns"] = obj["include_columns"]
    elif obj["exclude_columns"]:
        table_configuration["exclude_columns"] = obj["exclude_columns"]

    spec: dict[str, Any] = {
        "source_schema": obj["source_schema"],
        "destination_catalog": "${var.catalog}",
        "destination_schema": obj["destination_schema"],
        "table_configuration": table_configuration,
    }
    if spec_key == "schema":
        return {"schema": spec}

    spec["source_table"] = obj["source_table"]
    spec["destination_table"] = obj["destination_table"]
    if spec_key == "report":
        spec["source_url"] = "TODO_SET_WORKDAY_RAAS_REPORT_URL"
        return {"report": spec}
    return {"table": spec}


def _schedule_job(connection: dict[str, Any], pipeline_key: str) -> dict[str, Any]:
    return {
        "name": f"{connection['pipeline_name']}_schedule",
        "schedule": {
            "quartz_cron_expression": connection["schedule"]["cron"],
            "timezone_id": "UTC",
            "pause_status": "UNPAUSED",
        },
        "tasks": [
            {
                "task_key": "ingest",
                "pipeline_task": {"pipeline_id": f"${{resources.pipelines.{pipeline_key}.id}}"},
            }
        ],
    }


def _connections_sql(connections: list[dict[str, Any]]) -> str:
    """Emit CREATE CONNECTION statements with credentials left as placeholders.

    Credentials never enter the repo. Run these by hand, or replace the
    placeholders with Databricks secret references before running.
    """
    lines = [
        "-- Unity Catalog connections for the migrated Fivetran sources.",
        "--",
        "-- Review every statement before running. Credential values are",
        "-- placeholders on purpose: fill them in at run time and do not commit them.",
        "-- OAuth-based sources (Salesforce, Workday, ServiceNow, and similar) may",
        "-- require the interactive connection wizard in the Databricks UI instead;",
        "-- see MIGRATION_NOTES.md.",
        "",
    ]
    for connection in connections:
        target = connection["target"]
        connection_type = target.get("connection_type") or "TODO_CONNECTION_TYPE"
        options = target.get("connection_options") or {}
        lines.extend(
            [
                f"-- {target.get('display_name') or connection['fivetran_service']} "
                f"(from Fivetran connection {connection['fivetran_connection_id']})",
                f"CREATE CONNECTION IF NOT EXISTS `{connection['uc_connection_name']}`",
                f"  TYPE {connection_type}",
                "  OPTIONS (",
            ]
        )
        option_items = options or {"TODO": "see the connector docs for required options"}
        rendered = [f"    {key} '{value}'" for key, value in option_items.items()]
        lines.append(",\n".join(rendered))
        lines.extend(["  );", ""])
    return "\n".join(lines)


def _notes(
    plan: dict[str, Any], migratable: list[dict[str, Any]], skipped: list[dict[str, Any]]
) -> str:
    lines = [
        "# Migration notes",
        "",
        f"Generated from a plan dated {plan['generated_at']}.",
        "",
        "## Before deploying",
        "",
        "1. Create the Unity Catalog connections in `connections/create_connections.sql`.",
        "   Pipelines will fail to start until the connection they reference exists.",
        "2. Complete every source-side prerequisite listed below.",
        "3. Run `databricks bundle validate` and read the diff.",
        "4. Deploy to `dev` first and let one full sync complete before `prod`.",
        "5. Reconcile row counts against the Fivetran-managed tables, then pause the",
        "   Fivetran connection. Do not delete it until reconciliation passes.",
        "",
    ]

    prerequisites = [
        (c, item)
        for c in migratable
        for item in (c["source_prerequisites"] + c["manual_steps"])
    ]
    if prerequisites:
        lines.extend(["## Source-side and manual work", ""])
        for connection, item in prerequisites:
            lines.append(f"- **{connection['fivetran_service']}**: {item}")
        lines.append("")

    warnings = [(c, item) for c in migratable for item in c["warnings"]]
    if warnings:
        lines.extend(["## Warnings", ""])
        for connection, item in warnings:
            lines.append(f"- **{connection['fivetran_service']}**: {item}")
        lines.append("")

    if skipped:
        lines.extend(
            [
                "## Not migrated",
                "",
                "| Fivetran service | Connection | Reason |",
                "|---|---|---|",
            ]
        )
        for connection in skipped:
            reason = "; ".join(connection["blockers"]) or "no enabled tables discovered"
            lines.append(
                f"| `{connection['fivetran_service']}` "
                f"| `{connection['fivetran_connection_id']}` | {reason} |"
            )
        lines.append("")
        lines.append(
            "Keep these on Fivetran, or replace them with a custom Lakeflow "
            "Declarative Pipeline. Neither is a Lakeflow Connect managed connector today."
        )
        lines.append("")

    return "\n".join(lines)


def _dump(document: dict[str, Any]) -> str:
    return yaml.dump(
        document,
        Dumper=_BlockStyleDumper,
        sort_keys=False,
        default_flow_style=False,
        width=100,
    )
