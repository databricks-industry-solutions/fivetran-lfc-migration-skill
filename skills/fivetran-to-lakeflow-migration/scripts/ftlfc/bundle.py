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
- **Gateway-based CDC sources need two pipelines.** A continuous gateway on
  classic compute, plus a serverless ingestion pipeline that references the
  gateway by its ``pipeline_id``.
- **Integrated CDC sources need one pipeline.** A single serverless pipeline
  with ``connector_type: CDC`` that references the UC connection directly and
  stages through a ``data_staging_options`` volume, with no gateway. This is the
  default for SQL Server; ``--sqlserver-arch gateway`` selects the pair instead.
"""

from __future__ import annotations

import copy
import json
import re
from typing import Any

import yaml

# The gateway's workers do not affect throughput; the driver does. Databricks
# recommends the smallest practical workers with a large driver.
GATEWAY_DRIVER_NODE = "r5n.16xlarge"
GATEWAY_WORKER_NODE = "m5n.large"

# mode: production refuses to validate without an explicit root_path. A per-user
# path works with no extra setup; a shared path also needs a run_as service
# principal so table ownership does not follow whoever deployed last.
PROD_ROOT_PATH = (
    "/Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}"
)

DEV_SCHEMA_SUFFIX = "_dev"
_SCHEMA_SUFFIX_VAR = "${var.schema_suffix}"

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


def dev_catalog_for(plan: dict[str, Any], dev_catalog: str | None = None) -> str:
    return dev_catalog or f"{plan['target_catalog']}_dev"


def migratable_keys(plan: dict[str, Any]) -> list[str]:
    return [i["target"]["pipeline_name"] for i in plan["items"] if not i["blockers"]]


def is_pipeline_name_for(name: str, key: str) -> bool:
    """True when ``name`` is a deployed name this bundle would give ``key``'s
    pipeline or gateway, in any target, with or without the dev-mode prefix."""
    pattern = rf"(?:\[dev [^\]]+\] )?lfc-{re.escape(key)}-(?:gateway-)?[^-]+"
    return re.fullmatch(pattern, name) is not None


def rename_pipeline_keys(plan: dict[str, Any], renames: dict[str, str]) -> dict[str, Any]:
    """Return a copy of ``plan`` with each renamed key's pipeline_name replaced."""
    renamed = copy.deepcopy(plan)
    for item in renamed["items"]:
        key = item["target"]["pipeline_name"]
        item["target"]["pipeline_name"] = renames.get(key, key)
    return renamed


def build_bundle(
    plan: dict[str, Any],
    bundle_name: str,
    host: str | None = None,
    notification_email: str | None = None,
    dev_catalog: str | None = None,
    catalog_exists: dict[str, bool | None] | None = None,
    renamed_keys: dict[str, str] | None = None,
    kept_resources: list[str] | None = None,
    table_conflicts: list[dict[str, str]] | None = None,
) -> dict[str, str]:
    """Render a complete bundle as a mapping of relative path -> file contents.

    ``catalog_exists`` maps a catalog name to True or False when the workspace was
    checked, or None when the check could not run; it only feeds the README.
    ``renamed_keys`` (original key -> unique key, already applied to ``plan``) and
    ``kept_resources`` (resource files already in the output folder that will
    deploy with this bundle) are reported in the README, as are
    ``table_conflicts``: destination tables that already exist and belong to
    something other than this bundle (keys ``target``, ``table``, ``owner``).
    """
    migratable = [i for i in plan["items"] if not i["blockers"]]
    dev_catalog = dev_catalog_for(plan, dev_catalog)
    files: dict[str, str] = {
        "databricks.yml": dump_yaml(_root(plan, bundle_name, host, dev_catalog)),
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

    script = _connection_script(plan)
    files["scripts/create_connections.sh"] = script
    files["README.md"] = _bundle_readme(
        plan,
        bundle_name,
        dev_catalog=dev_catalog,
        catalog_exists=catalog_exists or {},
        has_placeholders=any(
            "REPLACE_ME" in line for line in script.splitlines() if not line.startswith("#")
        ),
        renamed_keys=renamed_keys or {},
        kept_resources=kept_resources or [],
        table_conflicts=table_conflicts or [],
    )
    return files


def _root(
    plan: dict[str, Any], bundle_name: str, host: str | None, dev_catalog: str
) -> dict[str, Any]:
    catalog = plan["target_catalog"]

    def target(
        mode: str, dest_catalog: str, schema_suffix: str | None = None, **extra: Any
    ) -> dict[str, Any]:
        # Built fresh per target: sharing one dict makes PyYAML emit an anchor
        # and an alias, which is valid but confusing in a file humans review.
        spec: dict[str, Any] = {"mode": mode, **extra}
        workspace: dict[str, str] = {}
        if host:
            workspace["host"] = host
        if mode == "production":
            workspace["root_path"] = PROD_ROOT_PATH
        if workspace:
            spec["workspace"] = workspace
        spec["variables"] = {"dest_catalog": dest_catalog}
        if schema_suffix:
            spec["variables"]["schema_suffix"] = schema_suffix
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
                "description": "Catalog holding gateway and integrated CDC staging volumes",
                "default": catalog,
            },
            "staging_schema": {
                "description": "Schema holding gateway and integrated CDC staging volumes",
                "default": "ingestion_staging",
            },
            "schema_suffix": {
                "description": "Appended to every destination schema; dev sets _dev",
                "default": "",
            },
        },
        "targets": {
            # Development mode prefixes resource names, and its own schemas keep a
            # dev run from writing into tables prod or another pipeline owns.
            "dev": target(
                "development", dev_catalog, default=True, schema_suffix=DEV_SCHEMA_SUFFIX
            ),
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
        "schema": target["destination_schema"] + _SCHEMA_SUFFIX_VAR,
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
    is_integrated_cdc = target.get("connector_type") == "CDC"

    # Exactly one of these is set: a gateway id for gateway-based CDC sources, a
    # connection name for integrated CDC, SaaS, and query-based connectors.
    if needs_gateway:
        definition["ingestion_gateway_id"] = "${resources.pipelines." + key + "_gateway.id}"
    else:
        definition["connection_name"] = target["connection_name"]

    # Integrated CDC pipelines must declare connector_type: CDC explicitly, or
    # the source defaults to a query-based connector rather than change capture.
    if is_integrated_cdc:
        definition["connector_type"] = "CDC"

    # Always table objects, never a schema object: a schema object would also
    # ingest tables the customer turned off in Fivetran, and schema-level
    # ingestion is not documented for every connector. For connectors with a
    # supported-table list, source_table is already the Lakeflow Connect name.
    definition["objects"] = [_object_spec(o, target) for o in item["objects"]]

    # Integrated CDC stages extracted change data through a UC volume. Naming the
    # catalog/schema keeps it in the bundle's staging location; the pipeline
    # autocreates one in the destination schema if this is omitted.
    if is_integrated_cdc:
        definition["data_staging_options"] = {
            "catalog_name": "${var.staging_catalog}",
            "schema_name": "${var.staging_schema}",
        }

    return definition


def _object_spec(obj: dict[str, Any], target: dict[str, Any]) -> dict[str, Any]:
    spec: dict[str, Any] = {}
    kind = obj["type"]

    if kind == "report":
        spec = {
            "source_url": obj.get("source_url")
            or f"REPLACE_WITH_RAAS_URL_FOR_{obj['source_table']}",
            "destination_catalog": "${var.dest_catalog}",
            "destination_schema": obj["destination_schema"] + _SCHEMA_SUFFIX_VAR,
            "destination_table": obj["destination_table"],
        }
    else:
        spec = {
            "source_schema": obj["source_schema"],
            "source_table": obj["source_table"],
            "destination_catalog": "${var.dest_catalog}",
            "destination_schema": obj["destination_schema"] + _SCHEMA_SUFFIX_VAR,
            "destination_table": obj["destination_table"],
        }
        # source_catalog is the source database name. Required for integrated
        # CDC; valid (and matches real gateway pipelines) when present otherwise.
        if obj.get("source_catalog"):
            spec = {"source_catalog": obj["source_catalog"], **spec}

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


def _catalog_note(name: str, target: str, exists: bool | None) -> str:
    if exists is True:
        status = "confirmed in the workspace"
    elif exists is False:
        status = "**not found in the workspace**; create it before the first deploy"
    else:
        status = (
            "not checked; regenerate with `--databricks-profile` to check, or run "
            f"`databricks catalogs get {name}`"
        )
    return f"  - `{name}` ({target}): {status}"


def _bundle_readme(
    plan: dict[str, Any],
    bundle_name: str,
    dev_catalog: str,
    catalog_exists: dict[str, bool | None],
    has_placeholders: bool,
    renamed_keys: dict[str, str],
    kept_resources: list[str],
    table_conflicts: list[dict[str, str]],
) -> str:
    summary = plan["summary"]
    prod_catalog = plan["target_catalog"]
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
        f"- {summary['gateways_required']} ingestion gateway(s) for gateway-based CDC sources",
        (
            f"- {summary.get('integrated_cdc_pipelines', 0)} integrated CDC pipeline(s) "
            "(single pipeline, no gateway)"
        ),
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
        "# 4. Check what landed. Validate prod only once you've decided to promote,",
        "#    then deploy it.",
        "databricks bundle summary -t dev",
        "databricks bundle validate --strict -t prod",
        "databricks bundle deploy -t prod",
        "```",
        "",
        "## Before you deploy",
        "",
    ]
    if has_placeholders:
        lines.append("- Fill in every `REPLACE_ME` in `scripts/create_connections.sh`.")
    lines += [
        (
            "- Both destination catalogs must exist; the bundle does not create them. "
            "Regenerate with `--dev-catalog` to point dev somewhere else."
        ),
        _catalog_note(dev_catalog, "dev", catalog_exists.get(dev_catalog)),
        _catalog_note(prod_catalog, "prod", catalog_exists.get(prod_catalog)),
        (
            "- The prod target deploys under the deploying user's home folder "
            "(`workspace.root_path`), which `mode: production` requires. For a team-owned "
            "deployment, switch it to `/Workspace/Shared/.bundle/${bundle.name}/${bundle.target}` "
            "and set `run_as.service_principal_name`, so table ownership does not follow "
            "whoever deployed last."
        ),
        (
            "- A pipeline fails if a destination table already exists, so deploy into a clean "
            "schema or set `destination_table` explicitly."
        ),
        (
            f"- The dev target writes to `<schema>{DEV_SCHEMA_SUFFIX}` (`schema_suffix`), so a "
            "dev run never writes into the schemas prod uses."
        ),
    ]

    if summary["gateways_required"]:
        lines.append(
            "- Gateways run continuously on classic compute and are billed even when the "
            "ingestion pipeline is idle."
        )

    if kept_resources:
        lines.append(
            "- This folder already held the resource files below, and they deploy with this "
            "bundle: "
            + ", ".join(f"`{path}`" for path in kept_resources)
            + ". Remove any the migration did not choose before deploying."
        )

    if renamed_keys:
        lines.append(
            "- Another bundle already deploys pipelines under these names, so these keys were "
            "given a suffix: "
            + ", ".join(f"`{old}` -> `{new}`" for old, new in renamed_keys.items())
            + ". Do not bind the existing pipelines or run them in place of this bundle's."
        )

    if summary.get("integrated_cdc_pipelines"):
        lines.append(
            "- Integrated CDC pipelines (`connector_type: CDC`) need the integrated CDC "
            "connector enabled on the workspace by the Databricks account team, and stage "
            "through the `${var.staging_catalog}.${var.staging_schema}` volume the pipeline "
            "creates. They run in triggered mode on their companion job."
        )

    if table_conflicts:
        lines += [
            "",
            "## Table conflicts",
            "",
            (
                "These destination tables already exist and belong to something other than this "
                "bundle. A table belongs to one pipeline at a time, so the update fails on them. "
                "Pick another schema; do not move them to this bundle's pipeline or drop them "
                "without the owner's approval."
            ),
            "",
            "| Target | Table | Owner |",
            "|---|---|---|",
        ]
        for c in table_conflicts:
            lines.append(f"| {c['target']} | `{c['table']}` | {c['owner']} |")

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

    renamed = [
        (item, obj)
        for item in migratable
        for obj in item["objects"]
        if obj.get("status") == "renamed"
    ]
    if renamed:
        lines += [
            "",
            "## Renamed tables",
            "",
            (
                "These tables land under the Lakeflow Connect name, not Fivetran's. Repoint "
                "downstream references and check column names against the landed table."
            ),
            "",
            "| Fivetran table | Lakeflow Connect table | Pipeline |",
            "|---|---|---|",
        ]
        for item, obj in renamed:
            lines.append(
                f"| `{obj['fivetran_table']}` | `{obj['destination_schema']}."
                f"{obj['destination_table']}` | `{item['target']['pipeline_name']}` |"
            )

    lakeflow_only = [
        (item, obj)
        for item in migratable
        for obj in item["objects"]
        if obj.get("status") == "lakeflow_only"
    ]
    if lakeflow_only:
        lines += [
            "",
            "## Lakeflow Connect-only tables",
            "",
            (
                "These tables match no table Fivetran synced, so they are new to downstream "
                "consumers. Remove any you do not need from the pipeline before deploying."
            ),
            "",
            "| Lakeflow Connect table | Pipeline |",
            "|---|---|",
        ]
        for item, obj in lakeflow_only:
            lines.append(
                f"| `{obj['destination_schema']}.{obj['destination_table']}` | "
                f"`{item['target']['pipeline_name']}` |"
            )

    skipped = []
    for item in migratable:
        names = sorted(
            r["lakeflow_table"]
            for r in item.get("table_plan") or []
            if r["status"] == "lakeflow_only" and not r.get("kept", True)
        )
        if names:
            skipped.append((item, names))
    if skipped:
        lines += [
            "",
            "## Lakeflow Connect tables not included",
            "",
            (
                "These connectors offer more tables than Fivetran synced. The extra tables "
                "are not included; rerun plan_migration.py with --include-lakeflow-only all, "
                "then generate_bundle.py, to add them."
            ),
            "",
            "| Pipeline | Tables |",
            "|---|---|",
        ]
        for item, names in skipped:
            lines.append(
                f"| `{item['target']['pipeline_name']}` | {', '.join(f'`{n}`' for n in names)} |"
            )

    if blocked:
        lines += [
            "",
            "## Not included",
            "",
            (
                "These Fivetran connections are absent from this bundle. The reason below "
                "comes with another path where one exists (a Lakeflow Community Connector, "
                "Auto Loader, federation, or keeping the connection on Fivetran):"
            ),
            "",
        ]
        for item in blocked:
            service = item["fivetran"]["service"]
            lines.append(
                f"- **{service}** (`{item['fivetran']['connection_id']}`): {item['blockers'][0]}"
            )

    return "\n".join(lines) + "\n"
