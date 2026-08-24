---
name: fivetran-to-lakeflow-migration
description: Migrate a customer's ingestion pipelines from Fivetran to Databricks Lakeflow Connect. Discovers the existing Fivetran estate over the REST API, collects MAR and spend, maps each connector to its Lakeflow Connect equivalent, compares cost, and generates plus deploys replacement pipelines as a Databricks Asset Bundle. Use when a customer asks about replacing Fivetran, consolidating ingestion onto Databricks, Fivetran cost reduction, or Lakeflow Connect migration.
---

# Fivetran to Lakeflow Connect migration

Six stages, each writing a reviewable artifact the next one reads. Run them in
order. Stop and talk to the human whenever a stage reports blockers.

```
1. Discover  fivetran_discover.py  -> inventory.json   what they run today
2. Measure   fivetran_mar.py       -> mar.json         what it costs today
3. Map       plan_migration.py     -> plan.json        what it becomes
4. Compare   compare_cost.py       -> cost.json        whether it is worth it
5. Generate  generate_bundle.py    -> bundle/          the replacement
6. Deploy    databricks bundle     -> live pipelines   cut over safely
```

Scripts live in `scripts/`. Run them with the repo's virtualenv, from the skill
directory. All of stages 1-5 are read-only with respect to both Fivetran and
Databricks: nothing is created until you run stage 6 deliberately.

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
`scripts/ftlfc/catalog.py` marks entries `UNVERIFIED` where the release state
could not be confirmed. Before committing to a date with a customer, check the
[current connector list](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/)
and the workspace's own Previews page.

## Stage 1: Discover

Needs a Fivetran API key and secret, which the customer creates under Account
Settings > API Config. Read-only, and secret-looking config values are redacted
before anything is written to disk.

```bash
export FIVETRAN_API_KEY=... FIVETRAN_API_SECRET=...
python3 scripts/fivetran_discover.py --columns -o out/inventory.json
```

Pass `--columns` whenever the plan will be used to generate pipelines.
Fivetran's schema response only returns columns somebody explicitly
overrode, so without it primary keys are unknown for every untouched table and
stage 3 cannot set SCD behaviour safely. It costs one rate-limited request per
table, so for a first look at a large estate run without it, then re-run with
it once the scope is agreed.

## Stage 2: Measure

MAR is the unit Fivetran bills on, and no REST endpoint exposes it. It lives in
the Fivetran Platform Connector's tables inside the customer's destination
warehouse, so this stage reads it there.

```bash
# Destination is Databricks
python3 scripts/fivetran_mar.py --warehouse-id <id> --profile <profile> -o out/mar.json

# Destination is Snowflake, BigQuery, or Redshift: hand the SQL to the customer
python3 scripts/fivetran_mar.py --print-sql
python3 scripts/fivetran_mar.py --csv mar_export.csv -o out/mar.json
```

If the Platform Connector is not installed, ask for the invoice or contract
value instead. A known annual number beats any model.

## Stage 3: Map

```bash
python3 scripts/plan_migration.py -i out/inventory.json -c <dest_catalog> \
    --mar out/mar.json -o out/plan.json
```

The plan classifies every connection by effort, and the distinction that
matters is not "supported" but *who has to do the work*:

| Effort | Meaning |
|---|---|
| `low` | Fully automatable end to end |
| `medium` | Automatable, after a source-system admin does setup |
| `high` | A human must complete a browser or vendor-UI step |
| `blocked` | No managed connector; needs a different architecture |

Read the blockers and warnings before continuing. Common ones and what they
mean:

- **Browser-only OAuth.** SaaS connection types cannot be created with SQL, and
  most need a one-time interactive consent in Catalog Explorer. Pipeline
  creation is scriptable afterwards. Plan for one human, once, per source.
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
python3 scripts/compare_cost.py -p out/plan.json --mar out/mar.json -o out/cost.json
```

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

## Stage 5: Generate

```bash
python3 scripts/generate_bundle.py -p out/plan.json -o out/bundle \
    --name <customer>-lfc --host https://<workspace>.cloud.databricks.com
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
had blockers are absent from the bundle by design and listed in its README.

## Stage 6: Deploy

```bash
cd out/bundle
./scripts/create_connections.sh <profile>      # once, per workspace
databricks bundle validate --strict -t dev
databricks bundle deploy -t dev
```

Then cut over in this order, and do not compress it:

1. Deploy to `dev` and let one full sync complete.
2. Reconcile row counts and spot-check values against the Fivetran-managed
   tables. The customer does this, not you.
3. Deploy to `prod` and run both pipelines in parallel for at least one full
   business cycle.
4. Pause the Fivetran connection. Do not delete it.
5. Delete only after the customer confirms reconciliation over a period they
   choose.

## Reference material

Load these only when you need the detail; they are long.

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
