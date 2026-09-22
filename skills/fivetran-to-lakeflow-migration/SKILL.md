---
name: fivetran-to-lakeflow-migration
description: |
  Migrate ingestion pipelines from Fivetran to Databricks Lakeflow Connect within
  the Genie Code migration framework (Assessment, Conversion, Data Migration,
  Reconciliation). Discovers the Fivetran estate, collects MAR and spend, maps
  each connector to Lakeflow Connect, compares cost, and generates plus deploys
  replacement pipelines as a Databricks Asset Bundle. Use when a customer asks
  about replacing Fivetran, consolidating ingestion onto Databricks, Fivetran
  cost reduction, Lakeflow Connect migration, or Genie Code data migration.
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
| **Assessment** | 1 Discover, 2 Measure, 2.5 Telemetry, 3 Map, 4 Compare | **GATE 1** | `inventory.json`, `mar.json`, `telemetry.json`, `plan.json`, `cost.json` |
| **Conversion** | 5 Generate | **GATE 2** | `bundle/` |
| **Data Migration** | 6 Deploy (parallel run) | — | live Lakeflow Connect pipelines |
| **Reconciliation** | 6 Deploy steps 2–5 (customer-owned) | — | row-count validation, customer pauses Fivetran |

```
1.   Discover    ${SCRIPTS}/fivetran_discover.py     -> inventory.json   ┐
2.   Measure     ${SCRIPTS}/fivetran_mar.py          -> mar.json         │ Assessment
2.5  Telemetry   ${SCRIPTS}/databricks_telemetry.py  -> telemetry.json   │
3.   Map         ${SCRIPTS}/plan_migration.py        -> plan.json        │
4.   Compare     ${SCRIPTS}/compare_cost.py          -> cost.json        ┘
                                                                        ▓▓ GATE 1: present & wait
5.   Generate    ${SCRIPTS}/generate_bundle.py       -> bundle/           Conversion
                                                                        ▓▓ GATE 2: present & wait
6.   Deploy      databricks bundle                   -> live pipelines    Data Migration + Reconciliation
```

Stages 1–5 are read-only with respect to both Fivetran and Databricks until
stage 6 runs deliberately.

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
4. **Warnings** — unknown primary keys, hashed columns, private networking,
   browser-only OAuth, unverified connector availability.
5. **Cost comparison** — Fivetran MAR spend vs modelled Lakeflow Connect
   scenarios from `cost.json`, with the assumptions and caveats printed by
   stage 4. If stage 2.5 ran, note which rates are grounded from system
   tables vs still using defaults. Never present the modelled number without
   its assumptions.
6. **Recommendation** — which connections to migrate first (lowest effort,
   highest MAR), which to defer, and which need a different architecture.

**Wait here.** The customer may want to:
- Narrow scope (exclude blocked or high-effort connections)
- Re-run discovery with `--columns` to resolve unknown primary keys
- Provide a measured cost from a pilot instead of the modelled estimate
- Ask questions about specific connectors or source prerequisites

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

**Never present a modelled cost as a quote.** Databricks publishes no
DBU-per-row coefficient for managed ingestion, so the Lakeflow Connect side of
any comparison is a scenario built on stated assumptions until a pilot has run.
Stage 4 prints its assumptions and caveats every time; reproduce them alongside
any number you pass on.

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
python3 ${SCRIPTS}/fivetran_discover.py --columns -o ${FTLFC_OUT}/inventory.json
```

Pass `--columns` whenever the plan will be used to generate pipelines.
Fivetran's schema response only returns columns somebody explicitly
overrode, so without it primary keys are unknown for every untouched table and
stage 3 cannot set SCD behaviour safely. It costs one rate-limited request per
table, so for a first look at a large estate run without it, then re-run with
it once the scope is agreed.

**Fivetran MCP path:** use `fivetran_list_groups`, `fivetran_list_connections_in_group`,
`fivetran_get_connection_schema_config`, and `fivetran_get_connection_column_config`
then assemble `inventory.json`, or run `fivetran_discover.py` after exporting
API credentials.

## Stage 2: Measure (Assessment)

MAR (Monthly Active Rows) is the unit Fivetran bills on. The Fivetran REST API
has **no endpoint** for MAR, usage, cost, or credits — this was verified by
probing nine candidate paths, all returning 404. The only programmatic source is
the Fivetran **Platform Connector**, which writes usage and billing tables into
the customer's destination warehouse.

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

If the Platform Connector is not installed at all, there is no automated path.
Ask the customer for their MAR and spend from the Fivetran dashboard
(Settings → Usage) or their invoice/contract. Pass the numbers to the cost
model manually. A known annual spend number beats any model estimate.

## Stage 2.5: Telemetry (Assessment — optional but recommended)

Queries three Databricks system tables to replace hardcoded cost-model
assumptions with the customer's actual rates and any existing Lakeflow Connect
consumption data:

- `system.billing.list_prices` — actual DBU rates for serverless and gateway SKUs
- `system.billing.usage` + `system.lakeflow.pipelines` — real DBU consumption for
  any existing ingestion pipelines or gateways
- Run-duration estimation from billing segments

```bash
python3 ${SCRIPTS}/databricks_telemetry.py --warehouse-id <id> \
  --profile <profile> -o ${FTLFC_OUT}/telemetry.json
```

If no SQL warehouse is available, print the queries for manual execution:

```bash
python3 ${SCRIPTS}/databricks_telemetry.py --print-sql
```

The output `telemetry.json` contains an enriched rate card. Stage 4 consumes it
automatically via `--telemetry`:

```bash
python3 ${SCRIPTS}/compare_cost.py -p ${FTLFC_OUT}/plan.json \
  --mar ${FTLFC_OUT}/mar.json --telemetry ${FTLFC_OUT}/telemetry.json \
  -o ${FTLFC_OUT}/cost.json
```

When the customer has no existing Lakeflow pipelines, the telemetry stage still
grounds the DBU rates from `list_prices` and identifies the remaining defaults
clearly in the enrichment log.

## Stage 3: Map (Conversion)

```bash
python3 ${SCRIPTS}/plan_migration.py -i ${FTLFC_OUT}/inventory.json -c <dest_catalog> \
  --mar ${FTLFC_OUT}/mar.json -o ${FTLFC_OUT}/plan.json
```

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

Read the blockers and warnings before continuing. Common ones and what they
mean:

- **Browser-only OAuth.** A warning, not a blocker. The connection needs a
  one-time interactive sign-in in Catalog Explorer, but the pipeline and job
  are generated and reference the connection by name. Plan for one human,
  once, per source, before `databricks bundle deploy`.
- **Unknown primary keys.** Re-run stage 1 with `--columns`.
- **Hashed columns.** Fivetran hashes at ingest; Lakeflow Connect has no
  equivalent. Reproduce with a downstream masking policy, and tell the customer
  the raw value will land in the bronze table.
- **Private networking.** Fivetran's SSH tunnel or PrivateLink setup does not
  carry over. Databricks-side network access must be designed separately.
- **No managed connector.** Check the `alternative` field. Object storage goes
  to Auto Loader, message buses to a streaming source, warehouses to
  federation. These are often cheaper than the Fivetran connector they replace,
  so a blocker is not necessarily bad news.

Two connectors are never faithful one-to-one migrations, and both need saying
out loud early:

- **Workday.** Fivetran syncs objects directly. Lakeflow Connect ingests one
  custom RaaS report per destination table, so every table needs a
  corresponding Workday report built by a Workday admin.
- **Google Analytics 4.** Fivetran delivers pre-aggregated report tables;
  Lakeflow Connect ingests raw events. Downstream models built on Fivetran's
  aggregates need rewriting.

## Stage 4: Compare

```bash
python3 ${SCRIPTS}/compare_cost.py -p ${FTLFC_OUT}/plan.json \
  --mar ${FTLFC_OUT}/mar.json --telemetry ${FTLFC_OUT}/telemetry.json \
  -o ${FTLFC_OUT}/cost.json
```

If stage 2.5 was skipped, omit `--telemetry` and the model uses defaults.

Read the assumptions block it prints. The two that dominate the result:

- **Gateways run continuously.** Database CDC sources needing a gateway bill
  24/7 on classic compute whether or not data is flowing, plus cloud
  infrastructure that never appears in `system.billing.usage`. This sets a floor
  independent of volume, and consolidating database sources behind fewer
  gateways is usually the single largest saving available.
- **Blocked connections keep costing.** Their Fivetran spend does not go away,
  and Fivetran's platform fees and tier minimums rarely scale down in
  proportion to removed MAR. A partial migration saves less than the MAR
  reduction implies.

To replace the model with a measurement, migrate one representative connection,
let it run three to seven days, then query `system.billing.usage` and pass the
rows with `--measured`. Do this before any number reaches a customer deck.

## Stage 5: Generate (Conversion)

```bash
python3 ${SCRIPTS}/generate_bundle.py -p ${FTLFC_OUT}/plan.json \
  -o ${FTLFC_OUT}/bundle --name <customer>-lfc \
  --host https://<workspace>.cloud.databricks.com
```

Produces a bundle the customer keeps in their own repo: one ingestion pipeline
per connection, a gateway pipeline where CDC needs one, and a companion job per
pipeline. Managed ingestion pipelines have no supported pipeline-level schedule,
which is why every pipeline gets a job carrying the cron translated from
Fivetran's sync frequency.

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
python3 ${SCRIPTS}/fivetran_discover.py --columns -g <group_id> \
  -o ${FTLFC_OUT}/inventory.json
python3 ${SCRIPTS}/fivetran_mar.py --warehouse-id <id> --profile <profile> \
  -o ${FTLFC_OUT}/mar.json
python3 ${SCRIPTS}/databricks_telemetry.py --warehouse-id <id> --profile <profile> \
  -o ${FTLFC_OUT}/telemetry.json
python3 ${SCRIPTS}/plan_migration.py -i ${FTLFC_OUT}/inventory.json \
  -c main_prod --mar ${FTLFC_OUT}/mar.json -o ${FTLFC_OUT}/plan.json
python3 ${SCRIPTS}/compare_cost.py -p ${FTLFC_OUT}/plan.json \
  --mar ${FTLFC_OUT}/mar.json --telemetry ${FTLFC_OUT}/telemetry.json \
  -o ${FTLFC_OUT}/cost.json
python3 ${SCRIPTS}/generate_bundle.py -p ${FTLFC_OUT}/plan.json \
  -o ${FTLFC_OUT}/bundle --name acme-lfc \
  --host https://<workspace>.cloud.databricks.com
```

Review `plan.json` blockers and `cost.json` assumptions before generating the bundle.

### Example: Offline from sample inventory (no Fivetran credentials)

```bash
python3 ${SCRIPTS}/plan_migration.py \
  -i ${SCRIPTS}/../fixtures/inventory.sample.json -c main_prod \
  -o ${FTLFC_OUT}/plan.json
python3 ${SCRIPTS}/compare_cost.py -p ${FTLFC_OUT}/plan.json \
  --fivetran-annual-cost 120000 -o ${FTLFC_OUT}/cost.json
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
  response shapes, pagination, rate limits, and what MAR is *not* available from
- [`references/lakeflow-connect-api.md`](references/lakeflow-connect-api.md) —
  `ingestion_definition` and `gateway_definition` schemas, `table_configuration`
  fields, connection creation paths
- [`references/connector-coverage.md`](references/connector-coverage.md) —
  Fivetran connector to Lakeflow Connect mapping with release states
- [`references/bundles-and-cost.md`](references/bundles-and-cost.md) — bundle
  patterns, billing system tables, and the cost model's derivation

## Gotchas worth remembering

- `source_type` in `ingestion_definition` is **output only**. Setting it is
  ignored; the type is inferred from the connection or gateway.
- `scd_type` has three values: `SCD_TYPE_1` (default), `SCD_TYPE_2`, and
  `APPEND_ONLY`. Fivetran's `LEGACY` sync mode maps to `APPEND_ONLY`.
- `include_columns` and `exclude_columns` are mutually exclusive per table.
- Salesforce objects resolve under a source schema literally named `objects`,
  not the schema name Fivetran used. The mapper rewrites this and warns.
- Ingestion pipelines cannot run in `continuous` mode; only gateways can, and
  gateways must.
- SQL Server has two architectures: gateway-based CDC, and integrated CDC
  (`connector_type: CDC`, Beta) with no separate gateway.
