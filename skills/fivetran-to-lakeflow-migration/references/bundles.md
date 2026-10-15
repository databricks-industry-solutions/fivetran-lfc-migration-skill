# Lakeflow Connect: Asset Bundles Reference

**Compiled:** 2026-08-24
**Verification basis:**

| Claim type | How it was verified |
| --- | --- |
| Bundle YAML field names | `databricks bundle schema` from **Databricks CLI v1.1.0** installed locally (authoritative — this is the JSON Schema the CLI validates against) |
| Everything else | docs.databricks.com (URLs inline) |

Anything I could not confirm is flagged **`[VERIFY]`**.

---

# Databricks Asset Bundles for Lakeflow Connect

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

## Sources

**Docs**

- Managed connectors overview — <https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/>
- Ingest from SQL Server (bundle example, gateway cluster policy) — <https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-pipeline>
- Create a MySQL ingestion pipeline (bundle example) — <https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/mysql-pipeline>
- Bundle resources reference — <https://docs.databricks.com/aws/en/dev-tools/bundles/resources>
- Connections API — <https://docs.databricks.com/api/workspace/connections/create>

**Local / live**

- `databricks bundle schema`, Databricks CLI **v1.1.0** — all field names and the resource-type list

## Open items requiring your verification

| Item | Why |
| --- | --- |
| Terraform `databricks_connection` resource name | Suggested as the IaC alternative to the missing `resources.connections`; not re-confirmed against the current provider. |
| `source_type` on older CLI versions | Absent in v1.1.0. If you pin an older CLI, run `databricks bundle schema \| grep source_type`. |
