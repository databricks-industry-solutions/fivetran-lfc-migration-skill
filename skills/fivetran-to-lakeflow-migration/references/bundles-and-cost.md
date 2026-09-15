# Lakeflow Connect: Asset Bundles + Billing Reference

**Compiled:** 2026-08-24
**Verification basis:**

| Claim type | How it was verified |
| --- | --- |
| Bundle YAML field names | `databricks bundle schema` from **Databricks CLI v1.1.0** installed locally (authoritative — this is the JSON Schema the CLI validates against) |
| Billing field values, SKUs, prices | **Live queries** against `system.billing.usage`, `system.billing.list_prices`, `system.lakeflow.*` on an AWS **Enterprise**-tier workspace |
| Everything else | docs.databricks.com (URLs inline) |

Anything I could not confirm is flagged **`[VERIFY]`**.

---

# Part 1: Databricks Asset Bundles for Lakeflow Connect

> Note on naming: the docs now call these **"Declarative Automation Bundles"**. The CLI command is still `databricks bundle`, the file is still `databricks.yml`, and DAB/"Asset Bundles" still refers to the same thing.

## 1.1 `databricks.yml` structure

```yaml
# databricks.yml — bundle root
bundle:
  name: lakeflow-connect-sqlserver

include:
  - resources/*.yml

variables:
  connection_name:
    description: 'Unity Catalog connection name for the source'
  source_database:
    description: 'Source database name (maps to source_catalog in table specs)'
  source_schema:
    default: dbo
  dest_catalog:
    description: 'Destination UC catalog'
  dest_schema:
    description: 'Destination UC schema'
  staging_catalog:
    description: "Catalog for the gateway's staging volume"
  staging_schema:
    description: "Schema for the gateway's staging volume"
  # Optional: resolve an ID by name at deploy time instead of hardcoding it
  warehouse_id:
    lookup:
      warehouse: 'Shared SQL Warehouse'

targets:
  dev:
    default: true
    mode: development           # prefixes resource names, pauses schedules, sets development=true
    workspace:
      host: https://my-dev-workspace.cloud.databricks.com
      # 'profile:' is an alternative to 'host:' — do not set both
    variables:
      connection_name: sqlserver_dev
      source_database: AdventureWorks_Dev
      dest_catalog: dev_bronze
      dest_schema: sqlserver
      staging_catalog: dev_bronze
      staging_schema: _staging

  prod:
    mode: production            # forbids dev-only settings, enforces run_as consistency
    workspace:
      host: https://my-prod-workspace.cloud.databricks.com
      root_path: /Workspace/Shared/.bundle/${bundle.name}/${bundle.target}
    run_as:
      service_principal_name: sp-ingest-prod
    variables:
      connection_name: sqlserver_prod
      source_database: AdventureWorks
      dest_catalog: prod_bronze
      dest_schema: sqlserver
      staging_catalog: prod_bronze
      staging_schema: _staging
```

Layout:

```
project/
├── databricks.yml
└── resources/
    ├── sqlserver_ingest.pipeline.yml
    └── sqlserver_schedule.job.yml
```

Path resolution gotcha: inside `resources/*.yml` use `../src/...`; inside `databricks.yml` use `./src/...`. Not relevant for pure-ingestion bundles (no source files), but it bites as soon as you add a notebook task.

Useful substitutions: `${var.<name>}`, `${bundle.target}`, `${bundle.name}`, `${resources.pipelines.<key>.id}`, `${workspace.current_user.short_name}`.

## 1.2 Valid keys on a `resources.pipelines.<key>` entry

Extracted directly from CLI v1.1.0 `bundle schema`. This is the complete set:

```
allow_duplicate_names   budget_policy_id   catalog       channel     clusters
configuration           continuous         development   edition     environment
event_log               filters            gateway_definition        id
ingestion_definition    libraries          lifecycle     name        notifications
permissions             photon             restart_window            root_path
run_as                  schema             serverless    storage     tags
target                  trigger            usage_policy_id
```

### `ingestion_definition` — complete field list (CLI v1.1.0)

| Field | Notes |
| --- | --- |
| `connection_name` | UC connection. Used by **SaaS/query-based** connectors and by gateway-less database connectors. |
| `ingestion_gateway_id` | Used by **CDC** connectors instead of `connection_name`. Connection is inherited from the gateway. |
| `connector_type` | Optional. E.g. CDC vs query-based. |
| `objects` | Required. List of `table` / `schema` / `report` specs. |
| `table_configuration` | Pipeline-level defaults, overridable per schema and per table. |
| `data_staging_options` | Staged-data location. Required when migrating a gateway-based CDC pipeline to a combined CDC pipeline. |
| `full_refresh_window` | Time window for CDC snapshot queries. |
| `ingest_from_uc_foreign_catalog` | Immutable bool. If true, ingest straight from UC foreign catalogs with no connection or gateway. |
| `source_configurations` | Top-level source config. |
| `netsuite_jar_path` | NetSuite-specific. |

> **⚠️ `source_type` is NOT a valid bundle field in CLI v1.1.0.** The string `"source_type"` does not appear anywhere in the bundle JSON schema. The `IngestionSourceType` enum (100+ values: `SALESFORCE`, `SQLSERVER`, `WORKDAY_RAAS`, `SERVICENOW`, `MYSQL`, `POSTGRESQL`, `ORACLE`, `NETSUITE`, `KAFKA`, …) *is* defined in the schema but is not reachable from `ingestion_definition`. **The source type is inferred from the UC connection's type**, not declared in YAML. Older blog posts and pre-2026 examples that show `source_type: SALESFORCE` under `ingestion_definition` will fail `bundle validate` on current CLI. If you are pinned to an older CLI, run `databricks bundle schema | grep source_type` to check your own version.

### `gateway_definition` — complete field list

| Field | Notes |
| --- | --- |
| `connection_name` | Immutable. The UC connection. |
| `gateway_storage_catalog` | **Required**, immutable. |
| `gateway_storage_schema` | **Required**, immutable. |
| `gateway_storage_name` | Optional. If omitted, auto-generated as `__databricks_ingestion_gateway_staging_data-<pipeline_id>`. |
| `connection_id` | **Deprecated** — use `connection_name`. |
| `connection_parameters` | Marked "Optional, Internal" in the schema. Don't use. |

### `objects[]` — one of three specs

`table` (`TableSpec`): `source_catalog`, `source_schema`, `source_table`, `destination_catalog`, `destination_schema`, `destination_table`, `table_configuration`, `connector_options`

`schema` (`SchemaSpec`): `source_catalog`, `source_schema`, `destination_catalog`, `destination_schema`, `table_configuration`, `connector_options`

`report` (`ReportSpec`): for report-based sources such as Workday RaaS.

### `table_configuration` — complete field list

| Field | Notes |
| --- | --- |
| `scd_type` | `SCD_TYPE_1` \| `SCD_TYPE_2` \| `APPEND_ONLY` (exact enum from schema) |
| `include_columns` | Allowlist. Mutually exclusive with `exclude_columns`. Future columns **excluded**. |
| `exclude_columns` | Denylist. Future columns **included** automatically. |
| `primary_keys` | Override the key used to apply changes. |
| `sequence_by` | Columns giving logical event order (out-of-order change handling). |
| `row_filter` | DBSQL-format predicate **without** the `WHERE` keyword. Immutable. |
| `auto_full_refresh_policy` | `{enabled, min_interval_hours}` — auto-recover via full refresh. |
| `salesforce_include_formula_fields` | Salesforce only. |
| `query_based_connector_config` | Query-based connectors only. |
| `workday_report_parameters` | Workday RaaS only. |

## 1.3 Complete example A — SaaS connector (single pipeline, no gateway)

`resources/salesforce_ingest.pipeline.yml`:

```yaml
resources:
  pipelines:
    sfdc_ingest:
      name: 'lfc-salesforce-ingestion-${bundle.target}'
      serverless: true
      continuous: false            # ingestion pipelines cannot run continuous; use a job schedule
      channel: 'CURRENT'           # CURRENT (stable) or PREVIEW
      catalog: ${var.dest_catalog}
      schema: ${var.dest_schema}
      # budget_policy_id: '<policy-uuid>'   # optional cost attribution

      ingestion_definition:
        # SaaS connectors reference the UC connection directly — no gateway.
        connection_name: ${var.connection_name}

        table_configuration:
          scd_type: 'SCD_TYPE_1'

        objects:
          - table:
              source_schema: 'objects'
              source_table: 'Account'
              destination_catalog: ${var.dest_catalog}
              destination_schema: ${var.dest_schema}
              destination_table: 'sfdc_account'
              table_configuration:
                scd_type: 'SCD_TYPE_2'
                include_columns:
                  - 'Id'
                  - 'Name'
                  - 'AnnualRevenue'
                  - 'LastModifiedDate'
                sequence_by:
                  - 'LastModifiedDate'
                primary_keys:
                  - 'Id'
                salesforce_include_formula_fields: false

          - table:
              source_schema: 'objects'
              source_table: 'Opportunity'
              destination_catalog: ${var.dest_catalog}
              destination_schema: ${var.dest_schema}
              table_configuration:
                exclude_columns:
                  - 'Description'
                row_filter: "IsDeleted = false"

      notifications:
        - email_recipients:
            - 'data-eng@example.com'
          alerts:
            - 'on-update-failure'
            - 'on-update-fatal-failure'
            - 'on-flow-failure'

      permissions:
        - group_name: 'data-engineers'
          level: 'CAN_RUN'
```

`resources/salesforce_schedule.job.yml`:

```yaml
resources:
  jobs:
    sfdc_schedule:
      name: 'lfc-salesforce-schedule-${bundle.target}'
      schedule:
        quartz_cron_expression: '0 0 */4 * * ?'   # every 4 hours
        timezone_id: 'UTC'
      tasks:
        - task_key: 'run_ingestion'
          pipeline_task:
            pipeline_id: ${resources.pipelines.sfdc_ingest.id}
      email_notifications:
        on_failure:
          - 'data-eng@example.com'
```

## 1.4 Complete example B — gateway + ingestion (database CDC source)

Adapted from the official SQL Server bundle example
(<https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-pipeline>).

`resources/sqlserver_ingest.pipeline.yml`:

```yaml
resources:
  pipelines:

    # ---------- Pipeline 1: the gateway ----------
    # Extracts change data from the source and stages it in a UC volume.
    # Runs on CLASSIC compute. Must run continuously — if it stops, source
    # change logs can be truncated and affected tables need a full refresh.
    gw_pipeline:
      name: 'lfc-sqlserver-gateway-${bundle.target}'
      continuous: true                     # gateways MUST be continuous
      channel: 'CURRENT'
      catalog: ${var.staging_catalog}
      schema: ${var.staging_schema}

      # Optional: pin gateway cluster shape. Docs recommend the SMALLEST
      # possible workers (they don't affect gateway throughput) with a large
      # driver. Minimum 8 cores total for efficient extraction.
      clusters:
        - label: 'default'
          driver_node_type_id: 'r5n.16xlarge'
          node_type_id: 'm5n.large'
          autoscale:
            min_workers: 1
            max_workers: 4
          # policy_id: '<cluster-policy-id>'   # required if you lack unrestricted cluster creation

      gateway_definition:
        connection_name: ${var.connection_name}
        gateway_storage_catalog: ${var.staging_catalog}
        gateway_storage_schema: ${var.staging_schema}
        gateway_storage_name: 'sqlserver_gateway_staging'

    # ---------- Pipeline 2: the ingestion pipeline ----------
    # Reads staged data and applies it to Delta tables. SERVERLESS only.
    mi_pipeline:
      name: 'lfc-sqlserver-ingestion-${bundle.target}'
      serverless: true
      continuous: false                    # not supported here; drive it from a job
      channel: 'CURRENT'
      catalog: ${var.dest_catalog}
      schema: ${var.dest_schema}

      ingestion_definition:
        # Reference the gateway by bundle resource key — NOT connection_name.
        # The connection is inherited from the gateway.
        ingestion_gateway_id: ${resources.pipelines.gw_pipeline.id}

        table_configuration:
          scd_type: 'SCD_TYPE_1'
          auto_full_refresh_policy:
            enabled: true
            min_interval_hours: 24

        full_refresh_window:
          start_hour: 2
          days_of_week:
            - 'SUNDAY'
          time_zone_id: 'America/Los_Angeles'

        objects:
          # Whole-schema ingestion: new source tables are picked up automatically.
          - schema:
              source_catalog: ${var.source_database}
              source_schema: ${var.source_schema}
              destination_catalog: ${var.dest_catalog}
              destination_schema: ${var.dest_schema}

          # Per-table override for a table that needs history tracking.
          - table:
              source_catalog: ${var.source_database}
              source_schema: ${var.source_schema}
              source_table: 'Customers'
              destination_catalog: ${var.dest_catalog}
              destination_schema: ${var.dest_schema}
              destination_table: 'customers_scd2'
              table_configuration:
                scd_type: 'SCD_TYPE_2'      # requires CDC on source; change-tracking-only is not enough
                primary_keys:
                  - 'CustomerID'
                sequence_by:
                  - 'ModifiedDate'
                exclude_columns:
                  - 'InternalNotes'
```

Deployment-order note: `${resources.pipelines.gw_pipeline.id}` creates an implicit dependency, so the CLI creates the gateway before the ingestion pipeline. You still have to **start the gateway** — `bundle deploy` creates but does not run it.

## 1.5 UC connections in bundles — there is **no** `resources.connections`

**Confirmed against CLI v1.1.0.** The complete list of supported bundle resource types is:

```
alerts        apps                    catalogs              clusters
dashboards    database_catalogs       database_instances    experiments
external_locations   jobs             model_serving_endpoints
models        pipelines               postgres_branches     postgres_catalogs
postgres_endpoints   postgres_projects   postgres_synced_tables
quality_monitors     registered_models   schemas            secret_scopes
sql_warehouses       synced_database_tables   vector_search_endpoints
vector_search_indexes   volumes
```

`connections` is **absent**. Note that `catalogs`, `schemas`, `volumes`, and `external_locations` *are* bundle-manageable — so a UC connection is the one securable in the ingestion path you must create out-of-band.

### Workaround: create the connection before deploy, reference it by name

The docs explicitly bless this pattern under "Create connections programmatically"
(<https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/>):

```bash
# scripts/ensure_connection.sh — run before `bundle deploy`
set -euo pipefail
PROFILE="${1:-DEFAULT}"
CONN_NAME="${2:?connection name required}"

if databricks connections get "$CONN_NAME" --profile "$PROFILE" >/dev/null 2>&1; then
  echo "Connection $CONN_NAME already exists — skipping."
  exit 0
fi

databricks connections create --profile "$PROFILE" --json "{
  \"name\": \"$CONN_NAME\",
  \"connection_type\": \"SQLSERVER\",
  \"options\": {
    \"host\": \"$SQLSERVER_HOST\",
    \"port\": \"1433\",
    \"trustServerCertificate\": \"false\",
    \"user\": \"$SQLSERVER_USER\",
    \"password\": \"$SQLSERVER_PASSWORD\"
  }
}"
```

Then in CI:

```bash
./scripts/ensure_connection.sh prod sqlserver_prod
databricks bundle deploy -t prod --profile prod
```

The `--json` body follows the **Connections REST API** schema
(<https://docs.databricks.com/api/workspace/connections/create>). Grant `USE CONNECTION` to the deploying identity separately.

Alternatives:

1. **Notebook job task inside the bundle** — a task that calls the Connections API and runs before the pipeline task. Keeps everything in one `bundle deploy`, at the cost of a job run.
2. **Terraform** — the `databricks_connection` resource exists in the Terraform provider, unlike DABs. If connections must be IaC-managed, Terraform is the only first-class option today. **`[VERIFY]`** — I did not re-confirm the current Terraform provider resource name in this pass.

**Hard limitation:** connectors whose only auth option is browser-based OAuth (OAuth U2M) **cannot** be created programmatically at all — they need interactive sign-in for the initial token. Per the docs this covers: **Confluence, Google Ads, HubSpot, Jira, Meta Ads, Slack, Slack Audit Logs, TikTok Ads, Zendesk Support.** For these, create the connection once in Catalog Explorer per workspace, then reference it by name from the bundle.

## 1.6 Bundle commands

```bash
# Scaffold. Relevant templates: lakeflow-pipelines, default-python, default-sql, default-minimal
databricks bundle init
databricks bundle init lakeflow-pipelines

# Validate. ALWAYS use --strict: it promotes warnings to errors.
databricks bundle validate --strict --profile DEFAULT
databricks bundle validate --strict -t prod --profile prod
databricks bundle validate --strict --debug            # when an error message is opaque

# Deploy
databricks bundle deploy -t dev  --profile DEFAULT
databricks bundle deploy -t prod --profile prod
databricks bundle deploy -t prod --profile prod --auto-approve   # for CI
databricks bundle deploy -t dev  --var="dest_catalog=scratch"    # ad-hoc var override

# Run a specific resource by its bundle key
databricks bundle run gw_pipeline -t prod --profile prod    # start the gateway
databricks bundle run mi_pipeline -t prod --profile prod    # trigger one ingestion update
databricks bundle run mi_schedule -t prod --profile prod    # trigger the scheduling job

# Inspect what is deployed (resource keys -> workspace IDs and URLs)
databricks bundle summary -t prod --profile prod

# Tear down everything the bundle created in that target. Destructive.
databricks bundle destroy -t dev --profile DEFAULT

# Adopt a pipeline that already exists in the workspace (see warning below)
databricks bundle deployment bind mi_pipeline <existing-pipeline-id> -t prod --profile prod

# Reverse-engineer YAML from an existing pipeline
databricks bundle generate pipeline <pipeline-id> --profile DEFAULT
```

`--profile <name>` selects a stanza from `~/.databrickscfg` and works on every `bundle` subcommand. It overrides `targets.<t>.workspace.host`/`profile`. In CI, prefer env vars (`DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`) over shipping a profile.

### ⚠️ The recreate trap

Changing an immutable pipeline property — `catalog`, `storage`, `gateway_storage_catalog`, `gateway_storage_schema` — makes `bundle deploy` **drop and recreate** the pipeline, which drops the streaming tables and materialized views it manages and forces a full refresh of all of them. The CLI warns:

```
This action will result in the deletion or recreation of the following DLT Pipelines
along with the Streaming Tables (STs) and Materialized Views (MVs) managed by them.
```

Do not `--auto-approve` a plan containing "recreate pipeline". For pipelines created by hand and later moved into a bundle, use `bundle deployment bind` so the bundle adopts the existing ID instead of creating a duplicate.

## 1.7 Per-environment parameterization

Declare in `variables`, override in `targets.<env>.variables`. Precedence, lowest to highest:

1. `variables.<name>.default`
2. `variables.<name>.lookup` (resolved by name at deploy time)
3. `targets.<target>.variables.<name>`
4. `--var="name=value"` on the command line
5. `BUNDLE_VAR_<name>` environment variable

```yaml
variables:
  connection_name:
    description: 'UC connection name'
  dest_catalog:
    default: 'dev_bronze'
  scd_default:
    default: 'SCD_TYPE_1'

targets:
  dev:
    mode: development
    variables:
      connection_name: 'sqlserver_dev'
      dest_catalog: 'dev_bronze'
  prod:
    mode: production
    run_as:
      service_principal_name: 'sp-ingest-prod'
    variables:
      connection_name: 'sqlserver_prod'
      dest_catalog: 'prod_bronze'
      scd_default: 'SCD_TYPE_2'
```

`${var.scd_default}` is legal in `scd_type` — the schema's enum fields accept either a literal or a `${var.*}` pattern. It does **not** accept `${bundle.target}` or other substitution families in enum position.

Practical guidance:

- Parameterize `connection_name`, `dest_catalog`, `dest_schema`, `staging_catalog`, `staging_schema`, `source_database`. These are the values that genuinely differ per environment.
- Do **not** put `${bundle.target}` into `catalog`/`schema` after first deploy — see the recreate trap.
- Do use `${bundle.target}` in `name`, which is mutable and keeps dev/prod pipelines distinguishable in the UI.
- Use `run_as.service_principal_name` in prod so ingested-table ownership does not follow whoever last deployed.

---

# Part 2: Lakeflow Connect billing and cost model

## 2.1 The headline: there is no dedicated "Lakeflow Connect" SKU

This contradicts a common assumption, so here is the raw evidence. Live query against `system.billing.usage` on an AWS Enterprise workspace, 180-day window:

```
billing_origin_product | sku_name                                          | usage_unit | usage_type    | records | quantity
LAKEFLOW_CONNECT       | ENTERPRISE_JOBS_SERVERLESS_COMPUTE_US_WEST_OREGON | DBU        | COMPUTE_TIME  | 161417  | 22348.22
LAKEFLOW_CONNECT       | ENTERPRISE_DLT_ADVANCED_COMPUTE                   | DBU        | COMPUTE_TIME  |  63698  |  3735.14
LAKEFLOW_CONNECT       | ENTERPRISE_JOBS_SERVERLESS_COMPUTE_US_WEST_OREGON | DBU        | PROCESSED_GB  |   7158  |    65.22
LAKEFLOW_CONNECT       | ENTERPRISE_JOBS_SERVERLESS_COMPUTE_US_EAST_OHIO   | DBU        | COMPUTE_TIME  |   3724  |    27.67
LAKEFLOW_CONNECT       | ENTERPRISE_JOBS_SERVERLESS_COMPUTE_AP_TOKYO       | DBU        | COMPUTE_TIME  |      6  |     6.08
LAKEFLOW_CONNECT       | ENTERPRISE_JOBS_SERVERLESS_COMPUTE_US_WEST_OREGON | GB         | PROCESSED_GB  |   1437  |     0.97
```

And confirming the negative — searching `system.billing.list_prices` for any SKU containing `INGEST`, `LAKEFLOW`, or `CONNECT` returns **only** networking SKUs (`PRIVATE_CONNECTIVITY_*`, `PUBLIC_CONNECTIVITY_*`). **There is no `*_LAKEFLOW_CONNECT_*` or `*_INGESTION_*` SKU.**

So:

- **`sku_name` does NOT contain `INGESTION` or `LAKEFLOW`.** Never filter on `sku_name LIKE '%INGEST%'` — you will get zero rows.
- **`billing_origin_product = 'LAKEFLOW_CONNECT'` is the only reliable identifier.** It is a *dimension over shared SKUs*, exactly as the billing docs describe: "Some Databricks products are billed under the same shared SKU… the `billing_origin_product` column provides more insight."

### Which component maps to which SKU

| Component | Compute | SKU observed | Appears under `LAKEFLOW_CONNECT`? |
| --- | --- | --- | --- |
| **Ingestion pipeline** (SaaS, query-based, streaming, and the apply side of CDC) | Serverless | `ENTERPRISE_JOBS_SERVERLESS_COMPUTE_<REGION>` | Yes |
| **Ingestion gateway** (CDC database sources) | Classic (job compute, `cluster_type: dlt`) | `ENTERPRISE_DLT_ADVANCED_COMPUTE` | Yes, in the 180-day window |
| **Pipeline maintenance** (metadata, change tracking between runs) | Serverless | same serverless jobs SKU, low DBU/hr | Yes |
| **Downstream DLT/ETL pipelines** you write on top | Serverless or classic | DLT Core/Pro/Advanced | **No** — `billing_origin_product = 'DLT'` |
| **Cloud VM cost of the gateway** | Classic | Not in `system.billing.usage` at all | **No** — see 2.4 |

Two things worth noting:

1. **The DBU rate for managed connector ingestion is the serverless-jobs rate**, not a DLT rate. Lakeflow Connect is not "DLT with a different label" from a pricing standpoint; the SaaS/query-based path is priced as serverless jobs compute.
2. **The gateway is priced as DLT Advanced.** That is a *different, lower* per-DBU rate than serverless jobs, but it runs on classic compute and therefore also carries a cloud VM bill that never shows up in `system.billing.usage`.

## 2.2 List prices

Live from `system.billing.list_prices`, AWS, USD, currently effective (`price_end_time IS NULL`):

| `sku_name` | USD / DBU |
| --- | --- |
| `ENTERPRISE_JOBS_SERVERLESS_COMPUTE_US_WEST_OREGON` | **0.45** |
| `PREMIUM_JOBS_SERVERLESS_COMPUTE_US_WEST_OREGON` | **0.35** |
| `ENTERPRISE_DLT_ADVANCED_COMPUTE` | **0.36** |
| `PREMIUM_DLT_ADVANCED_COMPUTE` | 0.36 |
| `ENTERPRISE_DLT_PRO_COMPUTE` | 0.25 |
| `ENTERPRISE_DLT_CORE_COMPUTE` | 0.20 |

These line up with published Lakeflow Connect "Managed Connectors" pricing of **$0.35/DBU Premium** and **$0.45/DBU Enterprise**, which is what you'd expect given they resolve to the serverless-jobs SKU.

Cloud and tier variation:

- **Serverless jobs SKUs are region-suffixed** (`_US_WEST_OREGON`, `_US_EAST_OHIO`, `_AP_TOKYO`, …), so the price varies by region. Classic DLT SKUs are not region-suffixed.
- **Tier prefix** is `PREMIUM_` or `ENTERPRISE_`. On **Azure Databricks the Premium tier corresponds to Enterprise on AWS/GCP** — so an Azure Premium workspace pays the *Enterprise*-equivalent rate. Do not compare `PREMIUM_` across clouds.
- **`[VERIFY]` for Azure/GCP exact figures.** My live workspace is AWS. Query `system.billing.list_prices` in your own account for authoritative numbers — the docs' published table is the fallback.
- Interactive pricing: <https://www.databricks.com/product/pricing/lakeflow-connect> (JS-rendered; not machine-scrapeable).

### Zerobus

Lakeflow Connect also covers **Zerobus Ingest** (push-based API writing directly to UC Delta tables), which **is** priced per-GB rather than per-DBU: roughly **$0.050/GB Premium, $0.064/GB Enterprise**. The pricing page carries a footnote that **billing for Zerobus OTEL ingest starts 2026-07-28**. Corresponding system tables exist: `system.lakeflow.zerobus_ingest` and `system.lakeflow.zerobus_stream`. **`[VERIFY]`** the Zerobus per-GB numbers against the live pricing page — I could not confirm them from a Databricks-owned source in this pass.

The `usage_type = 'PROCESSED_GB'` rows in the table above are the per-GB dimension showing up under `LAKEFLOW_CONNECT`. **Note this is undocumented:** the official monitor-costs page states `usage_type` is "Set to `COMPUTE_TIME`" and lists only `dlt_pipeline_id` / `uc_table_*` metadata. Empirically `PROCESSED_GB` also appears, with `usage_unit` of both `DBU` and `GB`. **Do not assume `usage_type = 'COMPUTE_TIME'`** in a cost query or you will silently drop the per-GB component.

## 2.3 System tables

### `system.lakeflow` exists — confirmed. Contents:

```
system.lakeflow.job_run_timeline
system.lakeflow.job_task_run_timeline
system.lakeflow.job_tasks
system.lakeflow.jobs
system.lakeflow.pipeline_update_timeline
system.lakeflow.pipelines
system.lakeflow.zerobus_ingest
system.lakeflow.zerobus_stream
```

### `system.lakeflow.pipelines` — actual columns

```
account_id      STRING
workspace_id    STRING
pipeline_id     STRING     -- join key to usage_metadata.dlt_pipeline_id
pipeline_type   STRING
name            STRING
created_by      STRING
run_as          STRING
tags            MAP
settings        STRUCT
configuration   MAP
create_time     TIMESTAMP
change_time     TIMESTAMP  -- SCD marker
delete_time     TIMESTAMP  -- SCD marker
```

`pipeline_type` observed values, with row counts:

| `pipeline_type` | rows |
| --- | --- |
| `ETL_PIPELINE` | 90,723 |
| `INGESTION_PIPELINE` | 22,091 |
| `MATERIALIZED_VIEW` | 17,273 |
| `DATABASE_TABLE_SYNC` | 13,063 |
| `INGESTION_GATEWAY` | 7,936 |
| `STREAMING_TABLE` | 5,337 |

This is the clean way to separate gateway spend from ingestion spend: filter `pipeline_type IN ('INGESTION_PIPELINE', 'INGESTION_GATEWAY')`.

> **⚠️ Bug in the official sample queries.** `system.lakeflow.pipelines` is a **slowly-changing table** — one row per pipeline *per configuration change*, with `change_time`/`delete_time`. The docs' queries do a plain `JOIN system.lakeflow.pipelines p ON u.usage_metadata.dlt_pipeline_id = p.pipeline_id`, which **fans out usage rows by the number of historical versions of each pipeline and inflates every DBU and dollar total**. Always deduplicate to the latest row per `pipeline_id` first. The query in 2.5 does this.

### Billing identification fields — confirmed

| Column | Value for Lakeflow Connect |
| --- | --- |
| `billing_origin_product` | **`LAKEFLOW_CONNECT`** — "Costs associated with Lakeflow Connect managed connectors" |
| `sku_name` | Shared SKUs. `ENTERPRISE_JOBS_SERVERLESS_COMPUTE_<REGION>`, `ENTERPRISE_DLT_ADVANCED_COMPUTE`. **No** `INGESTION`/`LAKEFLOW` substring. |
| `usage_type` | `COMPUTE_TIME` (documented) and `PROCESSED_GB` (observed, undocumented) |
| `usage_unit` | `DBU`, `MILLISECOND`, `GB` |
| `usage_metadata.dlt_pipeline_id` | The ingestion pipeline ID. **This is the pipeline identifier** — there is no `ingestion_pipeline_id` field. |
| `usage_metadata.dlt_update_id` | The pipeline update (run) ID. |
| `usage_metadata.uc_table_catalog` / `uc_table_schema` / `uc_table_name` | Destination table. |
| `usage_metadata.usage_policy_id` | Serverless usage policy, if tagged. Tagging managed ingestion pipelines with usage policies is **API-only**. |
| `custom_tags['<key>']` | Pipeline-level tags (`tags:` in the bundle) propagate here. |

**Always filter on `usage_unit = 'DBU'`.** Without it you double-count, because the same usage appears as `MILLISECOND`/`GB` rows too, and `list_prices` only prices the DBU rows.

## 2.4 What is *not* in `system.billing.usage`

- **Cloud infrastructure for the gateway.** The gateway is classic compute, so its EC2/VM, EBS, and egress costs land on your cloud provider bill, not Databricks'. Rule of thumb from FinOps practice: infrastructure adds roughly **50–100% on top of the DBU bill** for classic compute. This is the single largest blind spot in a Lakeflow Connect cost estimate for CDC database sources, and it's why a SaaS connector (serverless, all-in DBU price) is more predictable than a database CDC connector.
- **Cross-region / egress data transfer** from the source.
- **Storage** for staging volumes and destination Delta tables (separate `DATABRICKS_STORAGE`-family SKUs).
- **Private Link / public connectivity** charges, which show up as the `PRIVATE_CONNECTIVITY_*` and `PUBLIC_CONNECTIVITY_*` SKUs found earlier — relevant if the gateway reaches the source over Private Link.

## 2.5 Runnable query: per-pipeline monthly Lakeflow Connect cost, last 3 months

Validated end-to-end on a live workspace.

```sql
-- Per-pipeline monthly Lakeflow Connect cost in USD, last 3 full months + current.
-- Verified against system.billing.usage / list_prices / lakeflow.pipelines.
WITH pipelines_latest AS (
  -- system.lakeflow.pipelines is slowly-changing: dedupe to the latest row
  -- per pipeline_id, or the join fans out and inflates every total.
  SELECT pipeline_id, name, pipeline_type, run_as, tags
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY pipeline_id ORDER BY change_time DESC) AS rn
    FROM system.lakeflow.pipelines
  )
  WHERE rn = 1
),

priced_usage AS (
  SELECT
    DATE_TRUNC('MONTH', u.usage_date)          AS usage_month,
    u.usage_metadata.dlt_pipeline_id           AS pipeline_id,
    u.workspace_id,
    u.sku_name,
    u.usage_type,
    u.usage_quantity                           AS dbus,
    u.usage_quantity * lp.pricing.effective_list.default AS list_cost_usd
  FROM system.billing.usage AS u
  JOIN system.billing.list_prices AS lp
    ON  lp.sku_name      = u.sku_name
    AND lp.currency_code = 'USD'
    AND u.usage_end_time >= lp.price_start_time
    AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
  WHERE u.billing_origin_product = 'LAKEFLOW_CONNECT'
    AND u.usage_unit = 'DBU'          -- required: MILLISECOND/GB rows would double-count
    AND u.record_type = 'ORIGINAL'    -- exclude retractions/restatements
    AND u.usage_date >= ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -3)
)

SELECT
  pu.usage_month,
  pu.pipeline_id,
  COALESCE(p.name, '<deleted or unknown pipeline>') AS pipeline_name,
  COALESCE(p.pipeline_type, '<unknown>')            AS pipeline_type,
  pu.workspace_id,
  ROUND(SUM(pu.dbus), 2)                            AS total_dbus,
  ROUND(SUM(pu.list_cost_usd), 2)                   AS list_cost_usd,
  ROUND(SUM(CASE WHEN pu.usage_type = 'COMPUTE_TIME' THEN pu.list_cost_usd ELSE 0 END), 2) AS compute_usd,
  ROUND(SUM(CASE WHEN pu.usage_type = 'PROCESSED_GB' THEN pu.list_cost_usd ELSE 0 END), 2) AS processed_gb_usd,
  CONCAT_WS(', ', COLLECT_SET(pu.sku_name))         AS skus
FROM priced_usage AS pu
LEFT JOIN pipelines_latest AS p
  ON pu.pipeline_id = p.pipeline_id
GROUP BY
  pu.usage_month, pu.pipeline_id, p.name, p.pipeline_type, pu.workspace_id
ORDER BY
  pu.usage_month DESC, list_cost_usd DESC;
```

Notes on the shape of this query:

- `LEFT JOIN` to pipelines, not `INNER`. An inner join silently drops spend from deleted pipelines and from usage rows where `dlt_pipeline_id` is null — on my test workspace those unattributed rows were a real, non-trivial slice (e.g. ~$17.83 in one month). An inner join would have hidden them.
- `pricing.effective_list.default` is **list price**. Actual invoiced cost differs with account discounts, commitments, and entitlements. Treat output as an upper bound and a trend indicator, not an invoice.
- The result includes a `<deleted or unknown pipeline>` bucket by design — if that bucket is large, investigate before trusting per-pipeline attribution.

### Quick rollup: total Lakeflow Connect spend by month and component

```sql
SELECT
  DATE_TRUNC('MONTH', u.usage_date) AS usage_month,
  u.sku_name,
  u.usage_type,
  ROUND(SUM(u.usage_quantity), 2)                                        AS dbus,
  ROUND(SUM(u.usage_quantity * lp.pricing.effective_list.default), 2)    AS list_cost_usd
FROM system.billing.usage AS u
JOIN system.billing.list_prices AS lp
  ON  lp.sku_name      = u.sku_name
  AND lp.currency_code = 'USD'
  AND u.usage_end_time >= lp.price_start_time
  AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
WHERE u.billing_origin_product = 'LAKEFLOW_CONNECT'
  AND u.usage_unit = 'DBU'
  AND u.usage_date >= ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -3)
GROUP BY ALL
ORDER BY usage_month DESC, list_cost_usd DESC;
```

### Separating gateway from ingestion spend

```sql
WITH pipelines_latest AS (
  SELECT pipeline_id, name, pipeline_type
  FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY pipeline_id ORDER BY change_time DESC) rn
        FROM system.lakeflow.pipelines)
  WHERE rn = 1
)
SELECT
  p.pipeline_type,                            -- INGESTION_GATEWAY vs INGESTION_PIPELINE
  u.sku_name,
  ROUND(SUM(u.usage_quantity), 2) AS dbus,
  ROUND(SUM(u.usage_quantity * lp.pricing.effective_list.default), 2) AS list_cost_usd
FROM system.billing.usage u
JOIN pipelines_latest p ON u.usage_metadata.dlt_pipeline_id = p.pipeline_id
JOIN system.billing.list_prices lp
  ON  lp.sku_name = u.sku_name AND lp.currency_code = 'USD'
  AND u.usage_end_time >= lp.price_start_time
  AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
WHERE u.billing_origin_product = 'LAKEFLOW_CONNECT'
  AND u.usage_unit = 'DBU'
  AND u.usage_date >= DATE_SUB(CURRENT_DATE(), 90)
GROUP BY ALL
ORDER BY list_cost_usd DESC;
```

### Maintenance vs processing

The official heuristic: bucket hourly DBU totals per pipeline, and treat hours under **0.1 DBU/hr** as maintenance. It's a rule of thumb Databricks tells you to tune, not a real billing distinction — there is no SKU or metadata field that flags maintenance. Managed ingestion pipelines accrue maintenance charges (infrastructure, metadata, change tracking) even when not actively ingesting, so an idle pipeline is not a free pipeline.

## 2.6 Estimating cost *before* you run — Databricks publishes nothing

**Plainly: there is no published DBU-per-GB or DBU-per-row rule of thumb for Lakeflow Connect. A cost estimate must be empirical.**

Databricks' own position, from the serverless compute FAQ (<https://docs.databricks.com/aws/en/compute/serverless/>), under "How do I estimate costs for serverless?":

> "Databricks recommends running and benchmarking a representative or specific workload and then analyzing the billing system table."

That is the entire official guidance, and Lakeflow Connect ingestion pipelines run on serverless. The `monitor-costs` page is exclusively retrospective — every query on it reads `system.billing.usage`. There is no forward-looking sizing model, no per-connector DBU coefficient, and no throughput table.

What you actually have to work with:

1. **Benchmark.** Deploy the bundle to a dev target against a representative subset of tables, run for a few days, then run the query in 2.5 scoped to that pipeline. Extrapolate on table count, row volume, and schedule frequency.
2. **Databricks DBU Calculator** (<https://www.databricks.com/product/pricing/product-pricing/instance-types>) models generic DBU consumption by workload/region/instance type. It has no Lakeflow-Connect-specific mode, excludes all cloud infrastructure cost, and needs workload parameters you don't have pre-production. Order-of-magnitude only.
3. **Bound the gateway separately.** For CDC sources, the gateway is continuous classic compute — its cost is roughly deterministic and you can compute it directly: `hours × DBU/hr for the chosen node types × $0.36` plus the cloud VM bill. The docs recommend the smallest workers with a large driver (example: `r5n.16xlarge` driver, `m5n.large` workers, minimum 8 cores). A continuously-running gateway is often the floor of your bill regardless of data volume.
4. **Sizing levers that reduce the estimate,** all expressible in the bundle: `include_columns` / `exclude_columns` to cut column width, `row_filter` to cut row count, `scd_type: SCD_TYPE_1` instead of `SCD_TYPE_2` (history tracking materially increases write volume), and a less aggressive job `schedule` — ingestion frequency is usually the biggest single lever.
5. **Instrument before you scale.** Set `budget_policy_id` or `usage_policy_id` (API-only for managed ingestion) and pipeline `tags:` on day one, so `custom_tags` attribution works retroactively when someone asks what it cost.

Practical framing for a pre-sales or budgeting conversation: give a range derived from a 3–7 day pilot on a representative table subset, state the gateway floor separately, and explicitly call out that classic-compute cloud infrastructure is not in the DBU number.

---

## Sources

**Docs**

- Managed connectors overview — <https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/>
- Monitor managed ingestion pipeline cost — <https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/monitor-costs>
- Ingest from SQL Server (bundle example, gateway cluster policy) — <https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-pipeline>
- Create a MySQL ingestion pipeline (bundle example) — <https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/mysql-pipeline>
- Bundle resources reference — <https://docs.databricks.com/aws/en/dev-tools/bundles/resources>
- Billable usage system table reference — <https://docs.databricks.com/aws/en/admin/system-tables/billing>
- Serverless compute FAQ (cost estimation stance) — <https://docs.databricks.com/aws/en/compute/serverless/>
- Connections API — <https://docs.databricks.com/api/workspace/connections/create>
- Lakeflow Connect pricing (JS-rendered) — <https://www.databricks.com/product/pricing/lakeflow-connect>
- DBU calculator / instance types — <https://www.databricks.com/product/pricing/product-pricing/instance-types>

**Local / live**

- `databricks bundle schema`, Databricks CLI **v1.1.0** — all Part 1 field names and the resource-type list
- Live SQL against `system.billing.usage`, `system.billing.list_prices`, `system.lakeflow.*`, `system.information_schema.columns` on an AWS Enterprise workspace — all Part 2 SKUs, prices, column names, and enum values

## Open items requiring your verification

| Item | Why |
| --- | --- |
| Azure / GCP list prices | Verified on AWS only. Region-suffixed serverless SKUs mean prices differ; Azure Premium ≡ AWS Enterprise. |
| Zerobus per-GB rates ($0.050 / $0.064) | From a third-party pricing guide, not a Databricks-owned source. |
| `usage_type = 'PROCESSED_GB'` semantics | Observed live but absent from docs. Confirm whether it is Zerobus-only or applies to managed connectors generally. |
| Gateway `billing_origin_product` attribution | `ENTERPRISE_DLT_ADVANCED_COMPUTE` appeared under `LAKEFLOW_CONNECT` in a 180-day window but not a 90-day one. Confirm in your own account whether gateway spend is consistently tagged `LAKEFLOW_CONNECT` or sometimes `DLT`. |
| Terraform `databricks_connection` resource name | Suggested as the IaC alternative to the missing `resources.connections`; not re-confirmed against the current provider. |
| `source_type` on older CLI versions | Absent in v1.1.0. If you pin an older CLI, run `databricks bundle schema \| grep source_type`. |
