"""Emit a Databricks Asset Bundle from a migration plan.

The bundle is the deliverable: reviewable YAML the customer keeps, rather than a
pile of one-shot API calls nobody can audit afterwards.

Three structural facts from the API research shape what gets generated:

- **There is no ``resources.connections``.** Unity Catalog connections cannot be
  expressed in a bundle at all, so pipelines reference them by name and a
  separate connections bundle (see ``connections.py``) creates them from a
  secret scope.
- **There is no supported pipeline-level schedule.** Both ``trigger.cron`` and
  pipeline ``continuous`` are deprecated in favour of wrapping the pipeline in a
  Lakeflow Job, so every ingestion pipeline gets a companion job.
- **CDC database sources need two pipelines.** A continuous gateway on classic
  compute, plus a serverless ingestion pipeline that references the gateway by
  its ``pipeline_id``.
"""

from __future__ import annotations

from typing import Any

from .connections import (
    BOOTSTRAP_JOB_KEY,
    CONNECTIONS_DIR,
    build_connections_bundle,
    required_secrets,
    scriptable_connections,
    secret_scope_name,
)
from .yaml_out import dump_yaml

# The gateway's workers do not affect throughput; the driver does. Databricks
# recommends the smallest practical workers with a large driver.
GATEWAY_DRIVER_NODE = "r5n.16xlarge"
GATEWAY_WORKER_NODE = "m5n.large"


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

    files.update(build_connections_bundle(plan, bundle_name, host))
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
        # The connections bundle is deployed on its own, first.
        "sync": {"exclude": [f"{CONNECTIONS_DIR}/**"]},
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


def _bundle_readme(plan: dict[str, Any], bundle_name: str) -> str:
    summary = plan["summary"]
    migratable = [i for i in plan["items"] if not i["blockers"]]
    blocked = [i for i in plan["items"] if i["blockers"]]
    manual = [i for i in migratable if i["target"]["scriptable"] == "no"]
    scripted = scriptable_connections(plan)

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
    ]
    if scripted:
        lines.append(
            f"- `{CONNECTIONS_DIR}/`: a separate bundle that creates {len(scripted)} Unity "
            "Catalog connection(s) from a secret scope"
        )

    lines += ["", "## Deploy", "", "```bash", *_deploy_steps(bool(scripted), bool(manual)), "```"]

    if scripted:
        lines += _connections_section(scripted, bundle_name)

    lines += [
        "",
        "## Before you deploy",
        "",
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


def _deploy_steps(has_scripted: bool, has_manual: bool) -> list[str]:
    steps: list[str] = []
    if has_scripted:
        steps += [
            "# 1. Create the connections that have a non-interactive auth path. Bundles cannot",
            "#    declare connections, so a separate bundle stores the credentials in a secret",
            "#    scope and a job creates the connections from them.",
            f"cd {CONNECTIONS_DIR}",
            "databricks bundle deploy --profile <profile>",
            "./scripts/put_secrets.sh <profile>",
            f"databricks bundle run {BOOTSTRAP_JOB_KEY} --profile <profile>",
            "cd ..",
            "",
        ]
    if has_manual:
        steps += [
            "# Create each browser-OAuth connection listed under 'Manual connections' below,",
            "# in Catalog Explorer, with exactly the name shown.",
            "",
        ]
    steps += [
        "# Validate the ingestion bundle. --strict promotes warnings to errors.",
        "databricks bundle validate --strict -t dev --profile <profile>",
        "",
        "# Deploy to dev first. Development mode prefixes names and pauses schedules.",
        "databricks bundle deploy -t dev --profile <profile>",
        "",
        "# Check what landed, then promote.",
        "databricks bundle summary -t dev --profile <profile>",
        "databricks bundle deploy -t prod --profile <profile>",
    ]
    return steps


def _connections_section(scripted: list[dict[str, Any]], bundle_name: str) -> list[str]:
    scope = secret_scope_name(bundle_name)
    lines = [
        "",
        "## Connections created from the secret scope",
        "",
        (
            f"The connections bundle declares the secret scope `{scope}` and the job "
            f"`{BOOTSTRAP_JOB_KEY}`. `scripts/put_secrets.sh` prompts for each value below "
            "(or a file path, for multi-line values), so no credential is written to a file. "
            "The job then creates each connection, or replaces the options of one that "
            "already exists, so re-running it after a credential rotation is safe. "
            "Connections are metastore-level: deploy this once per metastore."
        ),
        "",
        "| Connection name | Type | Auth path | Secrets to store |",
        "|---|---|---|---|",
    ]
    for entry in scripted:
        keys = ", ".join(f"`{k}`" for k in required_secrets(entry))
        auth = entry["preferred_auth"] or "default"
        lines.append(f"| `{entry['name']}` | {entry['connection_type']} | {auth} | {keys} |")

    if any(e["unverified_secret_keys"] for e in scripted):
        lines += [
            "",
            (
                "Salesforce mTLS option key names are inferred from UI labels. If the job "
                "reports `INVALID_PARAMETER_VALUE`, the error names the keys it expects."
            ),
        ]
    if any(e["options_json_secret"] for e in scripted):
        lines += [
            "",
            (
                "Where the only secret is `<connection>.options_json`, the option keys for "
                "that connector type are not cataloged. Store a JSON object of every option "
                "(see `references/lakeflow-connect-api.md` in the skill)."
            ),
        ]
    return lines
