"""Markdown renderers for the discovery, plan, and billing artifacts.

Output is plain markdown so it can be pasted into Slack, attached to a ticket, or
handed to the google-docs skill for a customer-ready deliverable.
"""

from __future__ import annotations

from typing import Any

VERDICT_LABEL = {
    "supported": "Supported",
    "supported_with_changes": "Supported with changes",
    "preview": "Preview",
    "unsupported": "No managed connector",
    "unknown": "Unknown",
}


def render_plan_report(plan: dict[str, Any]) -> str:
    summary = plan["summary"]
    lines: list[str] = [
        "# Fivetran to Lakeflow Connect migration plan",
        "",
        f"Generated: {plan['generated_at']}",
        f"Target catalog: `{plan['target']['catalog']}`",
        f"Connector catalog last reviewed: {plan.get('catalog_last_reviewed') or 'unknown'}",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Connections planned | {summary['connections_planned']} |",
        f"| Migratable today | {summary['connections_migratable']} |",
        f"| Blocked | {summary['connections_blocked']} |",
        f"| Tables | {summary['tables_planned']} |",
        f"| Tables needing SCD type 2 | {summary['tables_scd_type_2']} |",
        f"| Ingestion gateways required | {summary['gateways_required']} |",
        f"| Manual steps flagged | {summary['manual_step_count']} |",
        "",
        "## Connections",
        "",
        "| Fivetran service | Verdict | Lakeflow connector | Status | Gateway | Tables |",
        "|---|---|---|---|---|---|",
    ]

    for connection in plan["connections"]:
        target = connection["target"]
        lines.append(
            "| `{service}` | {verdict} | {connector} | {status} | {gateway} | {tables} |".format(
                service=connection["fivetran_service"],
                verdict=VERDICT_LABEL.get(connection["verdict"], connection["verdict"]),
                connector=target.get("display_name") or target.get("connector") or "-",
                status=target.get("status") or "-",
                gateway="yes" if target.get("requires_gateway") else "no",
                tables=len(connection["objects"]),
            )
        )

    for connection in plan["connections"]:
        lines.extend(_render_connection_detail(connection))

    return "\n".join(lines) + "\n"


def _render_connection_detail(connection: dict[str, Any]) -> list[str]:
    target = connection["target"]
    lines = [
        "",
        f"### `{connection['fivetran_service']}` "
        f"({connection['fivetran_connection_id']})",
        "",
        f"- Verdict: **{VERDICT_LABEL.get(connection['verdict'], connection['verdict'])}**",
        f"- Lakeflow Connect target: {target.get('display_name') or '-'} "
        f"(`source_type: {target.get('source_type') or '-'}`)",
        f"- UC connection: `{connection['uc_connection_name']}`",
        f"- Ingestion pipeline: `{connection['pipeline_name']}`",
    ]
    if connection.get("gateway_pipeline_name"):
        lines.append(f"- Ingestion gateway pipeline: `{connection['gateway_pipeline_name']}`")

    schedule = connection["schedule"]
    frequency = schedule.get("fivetran_sync_frequency_minutes")
    lines.append(
        f"- Schedule: {schedule['mode']}"
        + (f" (`{schedule['cron']}`)" if schedule.get("cron") else "")
        + (f", from Fivetran sync_frequency of {frequency} min" if frequency else "")
    )

    for label, key in (
        ("Blockers", "blockers"),
        ("Source-side prerequisites", "source_prerequisites"),
        ("Manual steps", "manual_steps"),
        ("Warnings", "warnings"),
        ("Notes", "notes"),
        ("Alternatives", "alternatives"),
    ):
        items = connection.get(key) or []
        if items:
            lines.extend(["", f"**{label}**", ""])
            lines.extend(f"- {item}" for item in items)

    if connection["objects"]:
        lines.extend(
            [
                "",
                "**Tables**",
                "",
                "| Source | Destination | Fivetran sync_mode | SCD | Primary keys | Column selection |",
                "|---|---|---|---|---|---|",
            ]
        )
        for obj in connection["objects"]:
            lines.append(
                "| `{src_schema}.{src_table}` | `{cat}.{schema}.{table}` | {mode} | {scd} "
                "| {pks} | {cols} |".format(
                    src_schema=obj["source_schema"],
                    src_table=obj["source_table"],
                    cat=obj["destination_catalog"],
                    schema=obj["destination_schema"],
                    table=obj["destination_table"],
                    mode=obj["fivetran_sync_mode"] or "-",
                    scd=obj["scd_type"].replace("SCD_TYPE_", "type "),
                    pks=", ".join(obj["primary_keys"]) or "**none**",
                    cols=_describe_columns(obj),
                )
            )

        table_notes = [
            f"`{obj['source_schema']}.{obj['source_table']}`: {note}"
            for obj in connection["objects"]
            for note in obj["notes"]
        ]
        if table_notes:
            lines.extend(["", "**Table notes**", ""])
            lines.extend(f"- {note}" for note in table_notes)

    return lines


def _describe_columns(obj: dict[str, Any]) -> str:
    if obj["include_columns"]:
        return f"include {len(obj['include_columns'])}"
    if obj["exclude_columns"]:
        return f"exclude {len(obj['exclude_columns'])}"
    return "all"


def render_discovery_report(inventory: dict[str, Any]) -> str:
    summary = inventory["summary"]
    lines = [
        "# Fivetran discovery",
        "",
        f"Generated: {inventory['generated_at']}",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Connections | {summary['connections_total']} |",
        f"| Active | {summary['connections_active']} |",
        f"| Paused | {summary['connections_paused']} |",
        f"| Broken setup | {summary['connections_broken']} |",
        f"| Distinct connector types | {summary['distinct_services']} |",
        f"| Enabled tables | {summary['tables_enabled']} |",
        f"| Tables retaining history | {summary['tables_history_mode']} |",
        "",
        "## Connector types in use",
        "",
        "| Fivetran service | Connections |",
        "|---|---|",
    ]
    for service, count in summary["connections_by_service"].items():
        lines.append(f"| `{service}` | {count} |")

    lines.extend(
        [
            "",
            "## Connections",
            "",
            "| Connection | Service | State | Sync freq (min) | Networking | Tables enabled |",
            "|---|---|---|---|---|---|",
        ]
    )
    for connection in inventory["connections"]:
        counts = connection.get("counts") or {}
        state = "paused" if connection["paused"] else connection["status"].get("sync_state") or "-"
        lines.append(
            "| `{cid}` | `{service}` | {state} | {freq} | {net} | {tables} |".format(
                cid=connection["id"],
                service=connection["service"],
                state=state,
                freq=connection.get("sync_frequency_minutes") or "-",
                net=connection.get("networking_method") or "-",
                tables=counts.get("tables_enabled", 0),
            )
        )

    if inventory.get("warnings"):
        lines.extend(["", "## Discovery warnings", ""])
        lines.extend(f"- {warning}" for warning in inventory["warnings"])

    return "\n".join(lines) + "\n"
