"""Emit a Databricks Asset Bundle from a migration plan.

The bundle is the deliverable: reviewable YAML the customer keeps, rather than a
pile of one-shot API calls nobody can audit afterwards.

Three structural facts from the API research shape what gets generated:

- **There is no ``resources.connections``.** Unity Catalog connections cannot be
  expressed in a bundle at all, so they are emitted as a separate pre-deploy
  script and referenced by name.
- **There is no supported pipeline-level schedule.** Both ``trigger.cron`` and
  pipeline ``continuous`` are deprecated in favour of wrapping the pipeline in a
  Lakeflow Job, so every ingestion pipeline gets a companion job.
- **CDC database sources need two pipelines.** A continuous gateway on classic
  compute, plus a serverless ingestion pipeline that references the gateway by
  its ``pipeline_id``.
"""

from __future__ import annotations

import json
from typing import Any

import yaml

# The gateway's workers do not affect throughput; the driver does. Databricks
# recommends the smallest practical workers with a large driver.
GATEWAY_DRIVER_NODE = "r5n.16xlarge"
GATEWAY_WORKER_NODE = "m5n.large"

_MANUAL_CONNECTION_NOTE = (
    "MANUAL. This connector uses browser-based OAuth only, so no script can create its "
    "connection. Create it once in Catalog Explorer (Catalog > External data > "
    "Connections > Create connection) with exactly the name and type below, then deploy "
    "the bundle; its pipeline references the connection by name."
)


class _Dumper(yaml.SafeDumper):
    """Keeps nested bundle YAML readable by indenting sequences under their key."""

    def increase_indent(self, flow: bool = False, indentless: bool = False):
        return super().increase_indent(flow, False)


def dump_yaml(document: dict[str, Any]) -> str:
    return yaml.dump(document, Dumper=_Dumper, sort_keys=False, width=100, allow_unicode=True)


def build_bundle(
    plan: dict[str, Any],
    bundle_name: str,
    host: str | None = None,
    notification_email: str | None = None,
) -> dict[str, str]:
    """Render a complete bundle as a mapping of relative path -> file contents."""
    migratable = [i for i in plan["items"] if not i["blockers"]]
    files: dict[str, str] = {
        "databricks.yml": dump_yaml(_root(plan, bundle_name, host)),
    }

    for item in migratable:
        key = item["target"]["pipeline_name"]
        # One pipeline per .pipeline.yml file, per the CLI's own recommendation,
        # so the gateway gets its own file rather than sharing the ingestion one.
        if item["target"]["gateway"] == "required":
            files[f"resources/{key}_gateway.pipeline.yml"] = dump_yaml(
                {"resources": {"pipelines": {f"{key}_gateway": _gateway_pipeline(item)}}}
            )
        files[f"resources/{key}.pipeline.yml"] = dump_yaml(_pipeline(item, notification_email))
        files[f"resources/{key}.job.yml"] = dump_yaml(_job(item, notification_email))

    files["scripts/create_connections.sh"] = _connection_script(plan)
    files["README.md"] = _bundle_readme(plan, bundle_name)
    return files


def _root(plan: dict[str, Any], bundle_name: str, host: str | None) -> dict[str, Any]:
    catalog = plan["target_catalog"]

    def target(mode: str, dest_catalog: str, **extra: Any) -> dict[str, Any]:
        # Built fresh per target: sharing one dict makes PyYAML emit an anchor
        # and an alias, which is valid but confusing in a file humans review.
        spec: dict[str, Any] = {"mode": mode, **extra}
        if host:
            spec["workspace"] = {"host": host}
        spec["variables"] = {"dest_catalog": dest_catalog}
        return spec

    return {
        "bundle": {"name": bundle_name},
        "include": ["resources/*.yml"],
        "variables": {
            "dest_catalog": {
                "description": "Unity Catalog catalog receiving ingested data",
                "default": catalog,
            },
            "staging_catalog": {
                "description": "Catalog holding ingestion gateway staging volumes",
                "default": catalog,
            },
            "staging_schema": {
                "description": "Schema holding ingestion gateway staging volumes",
                "default": "ingestion_staging",
            },
        },
        "targets": {
            # Development mode prefixes resource names and pauses schedules, so
            # a dev deploy cannot compete with production for the same source.
            "dev": target("development", f"{catalog}_dev", default=True),
            "prod": target("production", catalog),
        },
    }


def _pipeline(item: dict[str, Any], notification_email: str | None) -> dict[str, Any]:
    target = item["target"]
    key = target["pipeline_name"]
    needs_gateway = target["gateway"] == "required"

    ingestion: dict[str, Any] = {
        "name": f"lfc-{key}-" + "${bundle.target}",
        "serverless": True,
        "channel": "CURRENT",
        "catalog": "${var.dest_catalog}",
        "schema": target["destination_schema"],
        "ingestion_definition": _ingestion_definition(item, needs_gateway, key),
    }

    # Salesforce formula fields are enabled by a top-level configuration flag,
    # not by the private salesforce_include_formula_fields table field.
    if target["connection_type"] == "SALESFORCE":
        ingestion["configuration"] = {
            "pipelines.enableSalesforceFormulaFieldsMVComputation": "true"
        }

    if notification_email:
        ingestion["notifications"] = [
            {
                "email_recipients": [notification_email],
                "alerts": ["on-update-failure", "on-update-fatal-failure", "on-flow-failure"],
            }
        ]

    return {"resources": {"pipelines": {key: ingestion}}}


def _gateway_pipeline(item: dict[str, Any]) -> dict[str, Any]:
    target = item["target"]
    return {
        "name": f"lfc-{target['pipeline_name']}-gateway-" + "${bundle.target}",
        # Gateways must run continuously: if one stops, the source's change log
        # can be truncated and affected tables need a full refresh.
        "continuous": True,
        "serverless": False,
        "channel": "CURRENT",
        "catalog": "${var.staging_catalog}",
        "schema": "${var.staging_schema}",
        "clusters": [
            {
                "label": "default",
                "driver_node_type_id": GATEWAY_DRIVER_NODE,
                "node_type_id": GATEWAY_WORKER_NODE,
                "autoscale": {"min_workers": 1, "max_workers": 4},
            }
        ],
        "gateway_definition": {
            "connection_name": target["connection_name"],
            "gateway_storage_catalog": "${var.staging_catalog}",
            "gateway_storage_schema": "${var.staging_schema}",
            "gateway_storage_name": f"{target['pipeline_name']}_staging",
        },
    }


def _ingestion_definition(item: dict[str, Any], needs_gateway: bool, key: str) -> dict[str, Any]:
    target = item["target"]
    definition: dict[str, Any] = {}

    # Exactly one of these is set: a gateway id for CDC sources, a connection
    # name for everything else.
    if needs_gateway:
        definition["ingestion_gateway_id"] = "${resources.pipelines." + key + "_gateway.id}"
    else:
        definition["connection_name"] = target["connection_name"]

    # For managed SaaS connectors whose supported-table set is cataloged, omit
    # the explicit objects list and let the connector auto-discover its tables
    # from the source.  This avoids runtime failures from Fivetran tables that
    # the Lakeflow connector doesn't support, and picks up any new tables the
    # connector adds in the future.  The plan still reports which Fivetran
    # tables have no Lakeflow equivalent.
    #
    # Database CDC and gateway sources keep the explicit list because their
    # tables are user-defined and the plan's filtering does not apply.
    if (
        target.get("category") == "saas_managed"
        and target.get("has_supported_tables")
        and not needs_gateway
    ):
        definition["objects"] = [
            {
                "schema": {
                    "source_schema": item["objects"][0]["source_schema"]
                    if item["objects"]
                    else "default",
                    "destination_catalog": "${var.dest_catalog}",
                    "destination_schema": target["destination_schema"],
                }
            }
        ]
    else:
        definition["objects"] = [_object_spec(o, target) for o in item["objects"]]

    return definition


def _object_spec(obj: dict[str, Any], target: dict[str, Any]) -> dict[str, Any]:
    spec: dict[str, Any] = {}
    kind = obj["type"]

    if kind == "report":
        spec = {
            "source_url": obj.get("source_url")
            or f"REPLACE_WITH_RAAS_URL_FOR_{obj['source_table']}",
            "destination_catalog": "${var.dest_catalog}",
            "destination_schema": obj["destination_schema"],
            "destination_table": obj["destination_table"],
        }
    else:
        spec = {
            "source_schema": obj["source_schema"],
            "source_table": obj["source_table"],
            "destination_catalog": "${var.dest_catalog}",
            "destination_schema": obj["destination_schema"],
            "destination_table": obj["destination_table"],
        }

    config = {k: v for k, v in obj["table_configuration"].items() if v}
    if config:
        spec["table_configuration"] = config

    return {kind: spec}


def _job(item: dict[str, Any], notification_email: str | None) -> dict[str, Any]:
    target = item["target"]
    key = target["pipeline_name"]
    schedule = item["schedule"]

    job: dict[str, Any] = {
        "name": f"lfc-{key}-schedule-" + "${bundle.target}",
        # Fivetran serialises syncs; without this a cron job would not.
        "max_concurrent_runs": schedule.get("max_concurrent_runs", 1),
        "tasks": [
            {
                "task_key": "run_ingestion",
                "pipeline_task": {
                    "pipeline_id": "${resources.pipelines." + key + ".id}",
                    "full_refresh": False,
                },
            }
        ],
    }

    if schedule["mode"] == "continuous":
        job["continuous"] = {"pause_status": "UNPAUSED"}
    else:
        job["schedule"] = {
            "quartz_cron_expression": schedule["quartz_cron_expression"],
            "timezone_id": schedule.get("timezone_id", "UTC"),
            "pause_status": "UNPAUSED",
        }

    if notification_email:
        job["email_notifications"] = {"on_failure": [notification_email]}

    return {"resources": {"jobs": {f"{key}_schedule": job}}}


def _connection_script(plan: dict[str, Any]) -> str:
    """Emit UC connections as a pre-deploy script.

    Bundles have no resources.connections type, and SQL CREATE CONNECTION does
    not support Lakeflow Connect managed ingestion types, so this uses the CLI.
    """
    lines = [
        "#!/usr/bin/env bash",
        "# Create the Unity Catalog connections this bundle references.",
        "#",
        "# Bundles cannot express UC connections, and SQL CREATE CONNECTION does not",
        "# support Lakeflow Connect managed ingestion types. The Connections REST API,",
        "# via the CLI below, is the supported path.",
        "#",
        "# Run this BEFORE `databricks bundle deploy`. Fill in every REPLACE_ME first.",
        "# Secret option key names are not published; if a create call is rejected, the",
        "# INVALID_PARAMETER_VALUE error names the keys it actually wants.",
        "",
        "set -euo pipefail",
        "",
        'PROFILE="${1:-DEFAULT}"',
        "",
    ]

    seen: set[str] = set()
    for item in plan["items"]:
        target = item["target"]
        name = target["connection_name"]
        if name in seen or not target["connection_type"]:
            continue
        seen.add(name)

        lines.append(
            f"# -- {item['fivetran']['service']} -> {target['connection_type']} " + "-" * 20
        )

        if item["blockers"]:
            lines.append("# SKIPPED. Not included in this bundle:")
            for blocker in item["blockers"]:
                lines.append(f"#   {blocker}")
            lines.append("")
            continue

        if target.get("connection_source") == "existing":
            lines.append(f"# EXISTING. Reusing '{name}'; this script does not create or modify it.")
            lines.append("")
            continue

        if target["scriptable"] == "no":
            lines.append(f"# {_MANUAL_CONNECTION_NOTE}")
            lines.append(f"#   Connection name: {name}")
            lines.append(f"#   Connection type: {target['connection_type']}")
            lines.append("")
            continue

        preferred_auth = target.get("preferred_auth")
        if target["scriptable"] == "conditional":
            lines.append("# Conditionally scriptable. " + item["prerequisites"]["auth_note"])
        if preferred_auth:
            lines.append(f"# Generated for the {preferred_auth} auth path.")
        if (target["connection_type"], preferred_auth) in _UNVERIFIED_SECRET_KEYS:
            lines.append(
                "# The secret option key names below are inferred from UI labels and are "
                "not published; verify them before running."
            )

        payload = {
            "name": name,
            "connection_type": target["connection_type"],
            "read_only": True,
            "options": _connection_options(target["connection_type"], preferred_auth),
        }
        body = json.dumps(payload, indent=2)
        lines.append(f"echo 'Creating connection {name}...'")
        lines.append(f"databricks connections create --profile \"$PROFILE\" --json '{body}'")
        lines.append("")

    return "\n".join(lines) + "\n"


# Non-secret option keys observed on real connections of each type. Secret keys
# are redacted by the API and could not be observed, so they are marked for the
# operator to supply rather than guessed at.
_OPTION_HINTS: dict[str, dict[str, str]] = {
    "SQLSERVER": {
        "host": "REPLACE_ME",
        "port": "1433",
        "user": "REPLACE_ME",
        "password": "REPLACE_ME",
    },
    "POSTGRESQL": {
        "host": "REPLACE_ME",
        "port": "5432",
        "user": "REPLACE_ME",
        "password": "REPLACE_ME",
    },
    "MYSQL": {"host": "REPLACE_ME", "port": "3306", "user": "REPLACE_ME", "password": "REPLACE_ME"},
    "ORACLE": {
        "host": "REPLACE_ME",
        "port": "1521",
        "service_name": "REPLACE_ME",
        "user": "REPLACE_ME",
        "password": "REPLACE_ME",
    },
    "TERADATA": {
        "host": "REPLACE_ME",
        "port": "1025",
        "user": "REPLACE_ME",
        "password": "REPLACE_ME",
    },
    "SALESFORCE": {
        "instance_url": "REPLACE_ME",
        "is_sandbox": "false",
        "client_id": "REPLACE_ME",
        "client_secret": "REPLACE_ME",
    },
    "SERVICENOW": {
        "instance_url": "REPLACE_ME",
        "client_id": "REPLACE_ME",
        "client_secret": "REPLACE_ME",
        "oauth_scope": "useraccount",
        "user": "REPLACE_ME",
        "password": "REPLACE_ME",
    },
    "WORKDAY_HCM": {
        "instance_url": "REPLACE_ME",
        "tenant_name": "REPLACE_ME",
        "user": "REPLACE_ME",
        "password": "REPLACE_ME",
    },
    "WORKDAY_RAAS": {"user": "REPLACE_ME", "password": "REPLACE_ME"},
    "NETSUITE": {
        "host": "REPLACE_ME",
        "port": "1708",
        "account_id": "REPLACE_ME",
        "role_id": "REPLACE_ME",
        "data_source": "NetSuite2.com",
    },
}


# Option shapes for a specific auth path, keyed by (connection type, credential
# type). Used in preference to _OPTION_HINTS when the plan names a preferred auth.
_AUTH_OPTION_HINTS: dict[tuple[str, str], dict[str, str]] = {
    ("SALESFORCE", "OAUTH_MTLS"): {
        "instance_url": "REPLACE_ME",
        "is_sandbox": "false",
        "client_id": "REPLACE_ME",
        "client_secret": "REPLACE_ME",
        "client_private_key": "REPLACE_ME",
        "client_certificate": "REPLACE_ME",
    },
}

_UNVERIFIED_SECRET_KEYS: frozenset[tuple[str, str]] = frozenset({("SALESFORCE", "OAUTH_MTLS")})


def _connection_options(connection_type: str, preferred_auth: str | None = None) -> dict[str, str]:
    if preferred_auth and (connection_type, preferred_auth) in _AUTH_OPTION_HINTS:
        return _AUTH_OPTION_HINTS[(connection_type, preferred_auth)]
    return _OPTION_HINTS.get(
        connection_type, {"REPLACE_ME": "see references/lakeflow-connect-api.md"}
    )


def _bundle_readme(plan: dict[str, Any], bundle_name: str) -> str:
    summary = plan["summary"]
    migratable = [i for i in plan["items"] if not i["blockers"]]
    blocked = [i for i in plan["items"] if i["blockers"]]
    reused = [i for i in migratable if i["target"].get("connection_source") == "existing"]
    manual = [i for i in migratable if i["target"]["scriptable"] == "no" and i not in reused]

    lines = [
        f"# {bundle_name}",
        "",
        "Lakeflow Connect ingestion generated from a Fivetran migration plan.",
        "",
        "## Contents",
        "",
        f"- {len(migratable)} ingestion pipeline(s)",
        f"- {summary['gateways_required']} ingestion gateway(s) for CDC database sources",
        f"- {len(migratable)} companion job(s), one per pipeline",
        f"- {summary['tables_total']} table(s) total",
        "",
        "## Deploy",
        "",
        "```bash",
        "# 1. Create the Unity Catalog connections. Bundles cannot express them.",
        "./scripts/create_connections.sh <profile>",
        "",
        "# 2. Validate. --strict promotes warnings to errors.",
        "databricks bundle validate --strict -t dev",
        "",
        "# 3. Deploy to dev first. Development mode prefixes names and pauses schedules.",
        "databricks bundle deploy -t dev",
        "",
        "# 4. Check what landed, then promote.",
        "databricks bundle summary -t dev",
        "databricks bundle deploy -t prod",
        "```",
        "",
        "## Before you deploy",
        "",
        "- Fill in every `REPLACE_ME` in `scripts/create_connections.sh`.",
        (
            "- Gateways run continuously on classic compute and are billed even when the "
            "ingestion pipeline is idle."
        ),
        (
            "- A pipeline fails if a destination table already exists, so deploy into a clean "
            "schema or set `destination_table` explicitly."
        ),
    ]

    if manual:
        lines += [
            "",
            "## Manual connections",
            "",
            (
                "These connectors use browser-based OAuth only. Their pipelines and jobs are in "
                "this bundle, but each connection must be created once in Catalog Explorer "
                "(Catalog > External data > Connections > Create connection) with exactly the "
                "name below, by a user who can sign in to the source. Do this before "
                "`databricks bundle deploy`."
            ),
            "",
            "| Connection name | Type | Fivetran connection |",
            "|---|---|---|",
        ]
        seen: set[str] = set()
        for item in manual:
            name = item["target"]["connection_name"]
            if name in seen:
                continue
            seen.add(name)
            lines.append(
                f"| `{name}` | {item['target']['connection_type']} | "
                f"`{item['fivetran']['connection_id']}` |"
            )

    if reused:
        lines += [
            "",
            "## Existing connections",
            "",
            (
                "These pipelines reuse a Unity Catalog connection that already exists. Nothing "
                "here creates or modifies it."
            ),
            "",
            "| Connection name | Type | Confirmed in workspace | Fivetran connection |",
            "|---|---|---|---|",
        ]
        for item in reused:
            target = item["target"]
            confirmed = "yes" if target.get("connection_verified") else "not checked"
            lines.append(
                f"| `{target['connection_name']}` | {target['connection_type']} | {confirmed} | "
                f"`{item['fivetran']['connection_id']}` |"
            )

    if blocked:
        lines += [
            "",
            "## Not included",
            "",
            (
                "These Fivetran connections have no managed Lakeflow Connect connector and are "
                "absent from this bundle:"
            ),
            "",
        ]
        for item in blocked:
            service = item["fivetran"]["service"]
            lines.append(
                f"- **{service}** (`{item['fivetran']['connection_id']}`): {item['blockers'][0]}"
            )

    return "\n".join(lines) + "\n"
