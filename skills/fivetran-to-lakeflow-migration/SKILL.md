---
name: fivetran-to-lakeflow-migration
description: |
  Migrate ingestion pipelines from Fivetran to Databricks Lakeflow Connect within
  the Genie Code migration framework (Assessment, Conversion, Data Migration,
  Reconciliation). Discovers the Fivetran estate, records Fivetran MAR, billing,
  and spend for the Databricks account team, maps each connector to Lakeflow
  Connect, and generates plus deploys replacement pipelines as a Databricks
  Asset Bundle. Use when a customer asks about replacing Fivetran, consolidating
  ingestion onto Databricks, Lakeflow Connect migration, or Genie Code data
  migration.
user-invocable: true
argument-hint: "[--group <fivetran-group-id>] [--dest-catalog <catalog>] [--profile <databricks-profile>]"
allowed-tools: Bash, Read, Write, Grep, Glob
---

# Fivetran to Lakeflow Connect migration

Six stages, each writing a reviewable artifact the next one reads. Run them in
order. **Two mandatory review gates** enforce human approval between phases —
the agent must never silently proceed past them.

Works in **Claude Code** (plugin or local repo) and **Genie Code** (upload this
folder to `/Users/{you}/.assistant/skills/fivetran-to-lakeflow-migration/`).
See [`references/genie-code.md`](references/genie-code.md) for publish steps and
runtime differences.

## Genie Code framework mapping

This skill is the **Data Migration** track for ingestion within Genie Code
agentic orchestration. It chains with warehouse migration skills (`lakehouse-migrator`)
and ends with customer-led reconciliation.

| Genie Code phase | Stages | Gate | Output |
|---|---|---|---|
| **Assessment** | 1 Discover, 2 Measure, 3 Map, 4 Consumption | **GATE 1** | `inventory.json`, `mar.json`, `fivetran_spend.json`, `plan.json`, consumption export (with consent) |
| **Conversion** | 5 Generate | **GATE 2** | `bundle/` |
| **Data Migration** | 6 Deploy (parallel run) | — | live Lakeflow Connect pipelines |
| **Reconciliation** | 6 Deploy steps 2–5 (customer-owned) | — | row-count validation, customer pauses Fivetran |

```
1.   Discover    ${SCRIPTS}/fivetran_discover.py     -> inventory.json   ┐
2.   Measure     ${SCRIPTS}/fivetran_mar.py          -> mar.json         │ Assessment
                 ${SCRIPTS}/fivetran_consumption.py  -> fivetran_spend.json (optional)
3.   Map         ${SCRIPTS}/plan_migration.py        -> plan.json        │
4.   Consumption ${SCRIPTS}/fivetran_consumption.py  -> Gate 1 summary   ┘
                                                                        ▓▓ GATE 1: present & wait
                 ${SCRIPTS}/fivetran_consumption.py  -> CSV export (only if the customer agrees)
5.   Generate    ${SCRIPTS}/generate_bundle.py       -> bundle/           Conversion
                                                                        ▓▓ GATE 2: present & wait
6.   Deploy      databricks bundle                   -> live pipelines    Data Migration + Reconciliation
```

Stages 1–5 are read-only with respect to both Fivetran and Databricks until
stage 6 runs deliberately. The one exception is the consumption export, which
copies files to a Unity Catalog volume only when the customer asks for that at
Gate 1.

## Review gates

There are two mandatory gates. At each one the agent **must stop, present the
summary, and wait for an explicit "proceed"** from the human. Never skip a gate,
even if there are no blockers.

### Gate 1 — Assessment review (after stages 1–4)

Present the following to the customer before any bundle is generated:

1. **Estate summary** — connection count, table count, sources by category
   (SaaS, database CDC, file, streaming, federation, blocked).
2. **Effort breakdown** — how many connections are low / medium / high / blocked,
   and what makes each high or blocked.
3. **Blockers** — every blocker from `plan.json`, with the connection name and
   the concrete obstacle. If any connection is `blocked`, explain the
   `alternative` field (Auto Loader, federation, streaming source).
4. **Warnings** — hashed columns, private networking, browser-only OAuth,
   connectors not yet GA, and database tables Fivetran confirms have no
   primary key. Present Beta / Public Preview notes as something to confirm
   in the target workspace, never as a finding that the connector is off:
   the planner cannot see the workspace.
5. **Fivetran consumption and spend** — the `fivetran_spend` and
   `fivetran_consumption` sections printed by stage 4. These are recorded
   figures, not estimates; if both are empty, say no Fivetran consumption or
   spend was recorded.
6. **Recommendation** — which connections to migrate first (lowest effort,
   highest MAR), which to defer, and which need a different architecture.

**Wait here** and ask two questions:

1. Which connections to migrate: all migratable ones, a subset (record the
   connection ids), or stop. The customer may want to narrow scope, reuse
   Unity Catalog connections that already exist (see stage 3), or ask about
   specific connectors or source prerequisites.
2. Whether to save the Fivetran consumption export as CSV files for their
   Databricks account team: in `${FTLFC_OUT}` only, also copied to a Unity
   Catalog volume they name, or not at all. Run the export (stage 4) only after
   a yes, and give them the folder path.

Do not proceed to stage 5 until the customer explicitly approves the
migration scope.

### Gate 2 — Conversion review (after stage 5)

Present the generated bundle for review before any deployment:

1. **Bundle contents** — list of pipelines, jobs, and connections that will be
   created, with their target schemas and tables.
2. **Excluded connections** — anything blocked or out of scope, and why.
3. **Manual steps required** — which UC connections need browser-based OAuth
   (listed under "Manual connections" in the bundle README and marked `MANUAL`
   in `create_connections.sh`), which source systems need admin setup.
4. **Schedule mapping** — Fivetran sync frequencies and their Quartz cron
   equivalents.

**Wait here.** The customer may want to:
- Adjust destination schemas or table names
- Change sync frequencies
- Remove specific tables from scope
- Review the `create_connections.sh` credential placeholders

Do not proceed to stage 6 until the customer explicitly approves the bundle.

## Preflight (Genie Code — run at the start of every bash step)

Genie Code does not set `CLAUDE_SKILL_DIR`. Each bash step may be a fresh
shell, so resolve paths before any script:

```bash
eval "$(python3 "${SCRIPTS:-scripts}/preflight.py" \
  --skill-dir "${CLAUDE_SKILL_DIR:-/Workspace/Users/<you>/.assistant/skills/fivetran-to-lakeflow-migration}" \
  --profile <databricks-profile> --install-deps --export)"
```

Replace `<you>` with the workspace user from `databricks current-user me`.
Claude Code can skip `--skill-dir` when `CLAUDE_SKILL_DIR` is already set.

Preflight installs `PyYAML`, creates `${FTLFC_OUT}`, and prints JSON checks for
Fivetran credentials and the Databricks CLI. Non-zero exit on missing `SKILL.md`
or PyYAML — stop and report the error.

All commands below assume `${SCRIPTS}` and `${FTLFC_OUT}` are exported.

## Ground rules

Three rules matter more than speed here.

**The skill does not estimate or compare cost.** It records Fivetran
consumption (Platform Connector MAR and billing, and any spend the customer
shares) and reports it as given. It never prices Lakeflow Connect, quotes
Fivetran rates, compares the two, or states a saving. If asked, say the
Databricks account team models cost from the consumption export. Write that
export only after the customer agrees to it at Gate 1.

**Never delete or disable a Fivetran connection.** The migration is additive
until the customer has reconciled row counts themselves. Pause, never delete,
and only after reconciliation passes.

**Connector availability changes constantly.** The catalog in
`${SCRIPTS}/ftlfc/catalog.py` marks entries `UNVERIFIED` where the release state
could not be confirmed. Before committing to a date with a customer, check the
[current connector list](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/)
and the workspace's own Previews page.

## Stage 1: Discover (Assessment)

Needs a Fivetran API key and secret (Account Settings → API Config), or the
Fivetran MCP read tools when configured. Read-only; secret-looking config values
are redacted before anything is written to disk.

```bash
export FIVETRAN_API_KEY=... FIVETRAN_API_SECRET=...
python3 ${SCRIPTS}/fivetran_discover.py -o ${FTLFC_OUT}/inventory.json
```

`--columns` is optional and not needed for planning. It fetches every
column per table (one rate-limited request each) and only adds primary keys
for tables nobody customised. Managed connectors, SaaS and database alike,
read primary keys from the source themselves, so do not re-run discovery or
pull column metadata to "resolve" keys. Excluded and hashed columns already
come back without it.

**Fivetran MCP path:** use `fivetran_list_groups`, `fivetran_list_connections_in_group`,
and `fivetran_get_connection_schema_config`, then assemble `inventory.json`
(`fivetran_get_connection_column_config` is the `--columns` equivalent, and
likewise not needed), or run `fivetran_discover.py` after exporting
API credentials.

## Stage 2: Measure (Assessment)

MAR (Monthly Active Rows) is the unit Fivetran bills on. The Fivetran REST API
has **no endpoint** for MAR, usage, cost, or credits — this was verified by
probing nine candidate paths, all returning 404. The only programmatic source is
the Fivetran **Platform Connector**, which writes usage and billing tables into
the customer's destination warehouse.

`fivetran_mar.py` loads six months of `incremental_mar` per table (every
`free_type`, so paid and total MAR stay separate) plus Fivetran's billed
amounts from `usage_cost` and, on credit-based plans, `credits_used`. A billing
table the account does not have is skipped, not an error. Older installs name
the MAR column `connector_name`; the script retries with it.

Determine which path applies, then run the corresponding command:

### Path A: Platform Connector lands in Databricks

Query it directly using a SQL warehouse. This is the simplest path — the data
is already in the Databricks lakehouse.

```bash
python3 ${SCRIPTS}/fivetran_mar.py --warehouse-id <id> --profile <profile> \
  -o ${FTLFC_OUT}/mar.json
```

### Path B: Platform Connector lands in another warehouse

The Platform Connector often lands in Snowflake, BigQuery, or Redshift —
wherever the customer's primary Fivetran destination is. The skill can query
each of these directly without needing to ask the customer to run SQL
and export a CSV.

**Snowflake** (requires `pip install snowflake-connector-python`):

```bash
python3 ${SCRIPTS}/fivetran_mar.py --snowflake \
  --sf-account <account_identifier> \
  --sf-user <username> \
  --sf-database <FIVETRAN_DB> \
  --sf-warehouse <COMPUTE_WH> \
  -o ${FTLFC_OUT}/mar.json
```

Password via `SNOWFLAKE_PASSWORD` env var or `--sf-password`. For SSO use
`--sf-authenticator externalbrowser`.

**BigQuery** (requires `pip install google-cloud-bigquery`):

```bash
python3 ${SCRIPTS}/fivetran_mar.py --bigquery \
  --bq-project <gcp-project-id> \
  -o ${FTLFC_OUT}/mar.json
```

Uses application default credentials by default. For a service account pass
`--bq-credentials-json <path-to-key.json>`.

**Redshift** (requires `pip install boto3`):

```bash
# Provisioned cluster
python3 ${SCRIPTS}/fivetran_mar.py --redshift \
  --rs-cluster <cluster-id> --rs-database <db> --rs-db-user <user> \
  -o ${FTLFC_OUT}/mar.json

# Serverless
python3 ${SCRIPTS}/fivetran_mar.py --redshift \
  --rs-workgroup <workgroup> --rs-database <db> \
  -o ${FTLFC_OUT}/mar.json
```

Uses AWS credentials from the environment or `~/.aws/credentials`.

**Manual fallback** — If direct credentials are not available for any of the
above, generate the SQL for the customer to run and ingest their CSV export:

```bash
python3 ${SCRIPTS}/fivetran_mar.py --print-sql          # hand to customer
python3 ${SCRIPTS}/fivetran_mar.py --csv mar_export.csv -o ${FTLFC_OUT}/mar.json
```

### Path C: No Platform Connector installed

If the Platform Connector is not installed at all, there is no automated path
to MAR or billing. Skip `fivetran_mar.py`; the spend the customer shares (below)
is the only cost record, and the consumption export says so.

### Fivetran spend (optional)

Once, during this stage, ask whether the customer will share their current
Fivetran spend: the monthly amount in USD, the month it covers, whether it is
an `invoice` (a billed total), `contract` (the committed amount), or `estimate`
(their own figure), and monthly MAR if they know it. Never require it. Record
only what they say; never look up, derive, or compare prices.

```bash
python3 ${SCRIPTS}/fivetran_consumption.py spend --month 2026-09 \
  --monthly-usd 12000 --basis invoice --monthly-mar 25000000 \
  --note "Enterprise plan, renews 2027-03" -o ${FTLFC_OUT}/fivetran_spend.json
```

Recording the same month again replaces the earlier entry.

## Stage 3: Map (Conversion)

```bash
python3 ${SCRIPTS}/plan_migration.py -i ${FTLFC_OUT}/inventory.json -c <dest_catalog> \
  --mar ${FTLFC_OUT}/mar.json -o ${FTLFC_OUT}/plan.json
```

**SQL Server architecture.** By default the planner targets **integrated CDC**:
one serverless pipeline with `connector_type: CDC` that references the UC
connection directly and stages through a Unity Catalog volume, with no gateway.
This removes the continuously running classic-compute gateway that the standard
architecture requires. Pass `--sqlserver-arch gateway` to generate the older
gateway-based pair instead (for example, if the workspace does not yet have the
integrated CDC connector enabled). Integrated CDC must be turned on for the
workspace by the Databricks account team, so confirm it before the customer
commits; the plan emits this as a warning on every SQL Server connection.

The plan classifies every connection by effort, and the distinction that
matters is not "supported" but *who has to do the work*:

| Effort | Meaning | In the bundle? |
|---|---|---|
| `low` | Fully automatable end to end | Yes |
| `medium` | Connection is scriptable once a source-system admin does setup and supplies credentials (Salesforce mTLS, ServiceNow ROPC, Workday, NetSuite, GA4 service account) | Yes |
| `high` | The UC connection needs a one-time interactive browser sign-in (browser-OAuth-only connectors) | Yes; the connection is a manual checklist step |
| `blocked` | No managed connector; needs a different architecture | No |

Source-admin work alone never makes a connection `high`. Where a connector
offers both browser OAuth and a non-interactive path, the plan's
`preferred_auth` names the non-interactive one and the bundle generates it.
Present browser OAuth as the fallback only if the customer declines the
source-side setup.

**Reusing existing connections.** Before running this stage, ask whether any
Unity Catalog connection already exists for a source being migrated. If one
does, pass it rather than hand-editing names in the generated bundle:

```bash
python3 ${SCRIPTS}/plan_migration.py -i ${FTLFC_OUT}/inventory.json -c <dest_catalog> \
  --use-connection pagerduty=<existing_uc_connection> \
  --databricks-profile <profile> -o ${FTLFC_OUT}/plan.json
```

`KEY` is a Fivetran service (applies to every connection of that service) or a
Fivetran connection id (wins over the service). The plan uses that name, marks
it `connection_source: existing`, and the bundle never creates or modifies it.
With `--databricks-profile`, the planner confirms (read-only, via
`databricks connections get`) that the connection exists and has the right
type; a missing or mismatched one becomes a blocker, and a confirmed one
removes the Beta / Public Preview note, since the connector is evidently
usable there. Without a profile the note stays, softened.

Read the blockers and warnings before continuing. Common ones and what they
mean:

- **Browser-only OAuth.** A warning, not a blocker. The connection needs a
  one-time interactive sign-in in Catalog Explorer, but the pipeline and job
  are generated and reference the connection by name. Plan for one human,
  once, per source, before `databricks bundle deploy`.
- **Primary keys.** Managed connectors read keys from the source themselves,
  so the plan does not warn about keys Fivetran did not report, and there is
  no need to re-run discovery or fetch column metadata for them. The one note
  you may see is for a database source where Fivetran confirms some tables
  have no key: those need the connector's keyless-table setup (for SQL
  Server, CDC instead of change tracking).
- **Beta / Public Preview.** The catalog labels release state; it does not
  check the workspace. Beta connectors are turned on from the workspace
  Previews page, some Public Preview ones through the account team. Ask the
  customer to confirm rather than asserting it is disabled.
- **Hashed columns.** Fivetran hashes at ingest; Lakeflow Connect has no
  equivalent. Reproduce with a downstream masking policy, and tell the customer
  the raw value will land in the bronze table.
- **Private networking.** Fivetran's SSH tunnel or PrivateLink setup does not
  carry over. Databricks-side network access must be designed separately.
- **No managed connector.** Check the `alternative` field. Object storage goes
  to Auto Loader, message buses to a streaming source, warehouses to
  federation. A blocker means a different architecture, not necessarily that
  the source cannot move.

Two connectors are never faithful one-to-one migrations, and both need saying
out loud early:

- **Workday.** Fivetran syncs objects directly. Lakeflow Connect ingests one
  custom RaaS report per destination table, so every table needs a
  corresponding Workday report built by a Workday admin.
- **Google Analytics 4.** Fivetran delivers pre-aggregated report tables;
  Lakeflow Connect ingests raw events. Downstream models built on Fivetran's
  aggregates need rewriting.

## Stage 4: Consumption (Assessment)

Print the Gate 1 consumption and spend sections. This writes nothing:

```bash
python3 ${SCRIPTS}/fivetran_consumption.py summary -p ${FTLFC_OUT}/plan.json \
  --mar ${FTLFC_OUT}/mar.json --spend ${FTLFC_OUT}/fivetran_spend.json
```

Omit `--mar` without the Platform Connector and `--spend` if no spend was
recorded. `fivetran_spend` is the customer's figure as recorded;
`fivetran_consumption` is the latest full month's paid MAR across the planned
connections and Fivetran's billed `usage_cost` / `credits_used` for the latest
full month.

**Export for the account team.** Only after the customer says yes at Gate 1:

```bash
python3 ${SCRIPTS}/fivetran_consumption.py export -p ${FTLFC_OUT}/plan.json \
  --mar ${FTLFC_OUT}/mar.json --spend ${FTLFC_OUT}/fivetran_spend.json \
  --warehouse-id <id> --profile <profile> -o ${FTLFC_OUT} \
  [--volume <catalog>.<schema>.<volume>]
```

It writes `fivetran_consumption_<date>/` with:

- `connections.csv` — one row per planned connection in the input shape of the
  Lakeflow Connect cost estimator: `source_type`, `num_rows` per run,
  `num_flows` (tables), `cloud`, `region`, plus `runs_per_month` and paid and
  total MAR. `num_rows` is the latest full month's paid MAR divided by runs per
  month (`43200 / sync_minutes`). MAR counts each key once a month, so it is a
  floor. It is empty for continuous schedules and without MAR.
- `fivetran_billing.csv` — Platform Connector `usage_cost` / `credits_used` for
  the destinations in the plan, plus any spend the customer reported.
- `mar_by_table.csv` — monthly MAR per table and `free_type`, filtered to the
  destinations in the plan.
- `README.txt` — what each file holds.

`cloud` and `region` come from `current_metastore()` on `--warehouse-id`; pass
`--cloud` and `--region` instead when there is no warehouse. `--volume` copies
the folder to an existing volume the customer can write to (`databricks fs cp`);
in Genie Code, `-o /Volumes/<catalog>/<schema>/<volume>` writes there directly.
Tell the customer the folder path, that they can download the files from Catalog
Explorer and send them to their Databricks account team, and that the files hold
connection, schema, and table names and row counts but no credentials or row
data.

## Stage 5: Generate (Conversion)

```bash
python3 ${SCRIPTS}/generate_bundle.py -p ${FTLFC_OUT}/plan.json \
  -o ${FTLFC_OUT}/bundle --name <customer>-lfc \
  --host https://<workspace>.cloud.databricks.com
```

Produces a bundle the customer keeps in their own repo: one ingestion pipeline
per connection, a gateway pipeline where gateway-based CDC needs one, and a
companion job per pipeline. SQL Server integrated CDC connections generate a
single serverless pipeline (`connector_type: CDC`, `data_staging_options`) with
no gateway file. Managed ingestion pipelines have no supported pipeline-level
schedule, which is why every pipeline gets a job carrying the cron translated
from Fivetran's sync frequency.

Unity Catalog connections are *not* bundle resources, because credentials must
not be committed. They are emitted as `scripts/create_connections.sh` with
`REPLACE_ME` placeholders, to be run once before deploying.

Review the generated YAML against the plan before deploying. Connections that
had blockers (no managed connector) are absent from the bundle by design and
listed in its README. Browser-OAuth connections are present; create each one
in Catalog Explorer with the exact name in the README before deploying.

## Stage 6: Deploy (Data Migration + Reconciliation)

The agent's role ends after deploying the bundle. Everything below is a
**customer-owned cutover checklist** — the agent does not execute these steps
and has no write access to Fivetran.

```bash
cd ${FTLFC_OUT}/bundle
./scripts/create_connections.sh <profile>      # once, per workspace
databricks bundle validate --strict -t dev
databricks bundle deploy -t dev
```

Then the customer cuts over in this order (do not compress it):

1. Deploy to `dev` and let one full sync complete.
2. **Reconcile** row counts and spot-check values against the Fivetran-managed
   tables. This is the customer's responsibility.
3. Deploy to `prod` and run both pipelines in parallel for at least one full
   business cycle.
4. **Customer action:** Pause the Fivetran connection. Do not delete it.
   The skill has no Fivetran write APIs and cannot do this automatically.
5. Delete only after the customer confirms reconciliation over a period they
   choose.

## Examples

### Example: Full pipeline from Fivetran API (Claude Code or Genie Code)

After preflight exports `${SCRIPTS}` and `${FTLFC_OUT}`:

```bash
export FIVETRAN_API_KEY=... FIVETRAN_API_SECRET=...
python3 ${SCRIPTS}/fivetran_discover.py -g <group_id> \
  -o ${FTLFC_OUT}/inventory.json
python3 ${SCRIPTS}/fivetran_mar.py --warehouse-id <id> --profile <profile> \
  -o ${FTLFC_OUT}/mar.json
python3 ${SCRIPTS}/plan_migration.py -i ${FTLFC_OUT}/inventory.json \
  -c main_prod --mar ${FTLFC_OUT}/mar.json -o ${FTLFC_OUT}/plan.json
python3 ${SCRIPTS}/fivetran_consumption.py summary -p ${FTLFC_OUT}/plan.json \
  --mar ${FTLFC_OUT}/mar.json
# Gate 1. Only if the customer agrees to the export:
python3 ${SCRIPTS}/fivetran_consumption.py export -p ${FTLFC_OUT}/plan.json \
  --mar ${FTLFC_OUT}/mar.json --warehouse-id <id> --profile <profile> -o ${FTLFC_OUT}
python3 ${SCRIPTS}/generate_bundle.py -p ${FTLFC_OUT}/plan.json \
  -o ${FTLFC_OUT}/bundle --name acme-lfc \
  --host https://<workspace>.cloud.databricks.com
```

Review `plan.json` blockers and warnings before generating the bundle.

### Example: Offline from sample inventory (no Fivetran credentials)

```bash
python3 ${SCRIPTS}/plan_migration.py \
  -i ${SCRIPTS}/../fixtures/inventory.sample.json -c main_prod \
  -o ${FTLFC_OUT}/plan.json
python3 ${SCRIPTS}/fivetran_consumption.py spend --month 2026-09 \
  --monthly-usd 10000 --basis contract -o ${FTLFC_OUT}/fivetran_spend.json
python3 ${SCRIPTS}/fivetran_consumption.py summary -p ${FTLFC_OUT}/plan.json \
  --spend ${FTLFC_OUT}/fivetran_spend.json
python3 ${SCRIPTS}/generate_bundle.py -p ${FTLFC_OUT}/plan.json \
  -o ${FTLFC_OUT}/bundle --name sample-lfc
```

### Example: Genie Code publish then invoke

Upload the skill folder to `/Users/{you}/.assistant/skills/fivetran-to-lakeflow-migration/`
(see `references/genie-code.md`). In Genie Code, ask: *"Run Fivetran to Lakeflow
Connect assessment for group cession_trample"* — the agent should preflight,
discover, measure MAR, and produce `inventory.json` and `plan.json` for review
before any deploy.

## Reference material

Load these only when you need the detail; they are long.

- [`references/genie-code.md`](references/genie-code.md) — Genie Code publish,
  preflight, MCP discovery path, orchestration with other migration skills
- [`references/fivetran-api.md`](references/fivetran-api.md) — endpoints,
  response shapes, pagination, rate limits, Platform Connector MAR and billing
  tables, and how MAR is counted
- [`references/lakeflow-connect-api.md`](references/lakeflow-connect-api.md) —
  `ingestion_definition` and `gateway_definition` schemas, `table_configuration`
  fields, connection creation paths
- [`references/connector-coverage.md`](references/connector-coverage.md) —
  Fivetran connector to Lakeflow Connect mapping with release states
- [`references/bundles.md`](references/bundles.md) — bundle patterns,
  per-environment parameters, and the recreate trap

## Gotchas worth remembering

- `source_type` in `ingestion_definition` is **output only**. Setting it is
  ignored; the type is inferred from the connection or gateway.
- `scd_type` has three values: `SCD_TYPE_1` (default), `SCD_TYPE_2`, and
  `APPEND_ONLY`. Fivetran's `LEGACY` sync mode maps to `APPEND_ONLY`.
- `include_columns` and `exclude_columns` are mutually exclusive per table.
- Salesforce objects resolve under a source schema literally named `objects`,
  not the schema name Fivetran used. The mapper rewrites this and warns.
- Ingestion pipelines cannot run in `continuous` mode; only gateways can, and
  gateways must. (Integrated CDC pipelines can run continuous mode in Beta, but
  the skill schedules them through a triggered companion job.)
- SQL Server has two GA architectures: gateway-based CDC (a continuous gateway
  plus an ingestion pipeline) and integrated CDC (`connector_type: CDC`, one
  serverless pipeline, no gateway, staged through a `data_staging_options`
  volume). The skill generates integrated CDC by default; `--sqlserver-arch
  gateway` selects the pair. Integrated CDC needs the connector enabled on the
  workspace by the Databricks account team.
