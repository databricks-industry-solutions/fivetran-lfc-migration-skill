# Fivetran to Lakeflow Connect Migration Skill

An agent skill that migrates a customer's ingestion estate from **Fivetran** to
**Databricks Lakeflow Connect**. It discovers what they run today, records their
Fivetran consumption and spend for the Databricks account team, maps each
connector to its Lakeflow equivalent, and generates deployable replacement
pipelines as a **Databricks Asset Bundle**. The skill does not estimate or
compare cost: the account team models cost separately from the consumption
export.

Built for **Claude Code** (plugin) and **Genie Code** (agentic orchestration).

---

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
  - [Claude Code (laptop)](#claude-code-laptop)
  - [Genie Code (customer or prospect workspace)](#genie-code-customer-or-prospect-workspace)
- [Quick Start](#quick-start)
- [Pipeline Stages](#pipeline-stages)
- [Review Gates](#review-gates)
- [Project Layout](#project-layout)
- [Genie Code Integration](#genie-code-integration)
- [Offline / Demo Mode](#offline--demo-mode)
- [Development](#development)
- [Design Notes](#design-notes)
- [References](#references)
- [License](#license)

---

## Overview

The skill runs a six-stage pipeline. Each stage reads the previous stage's
artifact and writes its own. Artifacts are JSON files on disk — human-readable,
diffable, and versionable — except the optional consumption export, which is a
folder of CSV files. Stages 1 through 5 are **read-only** with respect to both
Fivetran and Databricks. The one exception is the consumption export: written
only if the customer agrees at Gate 1, to the local output folder by default,
and copied to a Unity Catalog volume only when they name one.

```
1.   Discover    ──> inventory.json                (Fivetran API)
2.   Measure     ──> mar.json                      (Platform Connector MAR and billing)
                 ──> fivetran_spend.json           (customer-reported spend, optional)
3.   Map         ──> plan.json                     (connector catalog)
4.   Consumption ──> Gate 1 summary                (Platform Connector MAR/billing + reported spend)
                     ▓▓ GATE 1 ▓▓                  human review + export consent
                 ──> fivetran_consumption_<date>/  (CSV export, only after consent)
5.   Generate    ──> bundle/                       (Asset Bundle YAML)
                     ▓▓ GATE 2 ▓▓                  human review
6.   Deploy      ──> live pipelines                (databricks bundle deploy)
```

Two mandatory **review gates** enforce human approval. The agent stops after
assessment (scope + Fivetran consumption) and again after conversion (generated
bundle). Neither gate can be skipped.

---

## Prerequisites

| Requirement | Version | Purpose |
|---|---|---|
| Python | >= 3.10 | All scripts |
| PyYAML | >= 6.0.1 | Bundle YAML emission (only runtime dependency) |
| Fivetran API key + secret | — | Stage 1 discovery (Account Settings > API Config) |
| Databricks CLI | >= 0.200 | Stage 2 MAR queries, stage 4 export to a volume, stage 6 deploy |
| Databricks SQL warehouse | — | Stage 2 and stage 4 cloud/region lookup (optional; CSV, `--print-sql`, and `--cloud`/`--region` paths exist) |

Optional for development:

| Requirement | Version | Purpose |
|---|---|---|
| pytest | >= 8.0 | Test suite |
| ruff | >= 0.6 | Linting and formatting |
| jsonschema | >= 4.0 | Bundle schema validation tests |

---

## Installation

Pick one runtime. **Claude Code** runs on a laptop against a local clone.
**Genie Code** runs entirely in the customer's (or prospect's) Databricks
workspace — no local Python venv required after the skill folder is installed.

### Claude Code (laptop)

#### 1. Clone the repository

```bash
git clone https://github.com/databricks-industry-solutions/fivetran-lfc-migration-skill.git
cd fivetran-lfc-migration-skill
```

#### 2. Create a virtual environment and install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

For development (tests + linting):

```bash
pip install -e ".[dev]"
```

#### 3. Configure credentials

```bash
export FIVETRAN_API_KEY=<your-api-key>
export FIVETRAN_API_SECRET=<your-api-secret>
```

Get these from the Fivetran dashboard under **Account Settings > API Config**.
The skill only issues GET requests and never modifies Fivetran state.

#### 4. (Optional) Configure Databricks CLI

Required for stage 2 (Databricks path), the stage 4 export's
cloud/region lookup and volume copy, and stage 6. If the CLI is already configured, no extra
setup is needed. Otherwise:

```bash
databricks auth login --profile <profile-name>
```

### Genie Code (customer or prospect workspace)

Use this when the customer wants to run the skill **inside Databricks Genie
Code Agent mode**, on their workspace identity, with credentials that never
leave their environment.

Official skill locations ([Genie Code skills](https://docs.databricks.com/aws/en/genie-code/skills)):

| Scope | Workspace path | Who installs |
|---|---|---|
| **User skill** (typical for a POC) | `/Users/<workspace-user>/.assistant/skills/fivetran-to-lakeflow-migration/` | Any user |
| **Workspace skill** (shared rollout) | `/Workspace/.assistant/skills/fivetran-to-lakeflow-migration/` | Workspace admin |

Upload the **entire** skill folder: `SKILL.md`, `requirements.txt`,
`references/`, `scripts/` (including `scripts/ftlfc/`), and `fixtures/`.

#### 1. Prerequisites in the customer workspace

- Genie Code **Agent mode** enabled
- Permission to write under `.assistant/skills/` (user folder or workspace folder)
- Fivetran **Account Settings > API Config** key + secret (read-only; Standard plan or above), stored by the customer in a **Databricks secret scope** (see step 3)
- A SQL warehouse the user can query, if MAR lives in Databricks (stage 2)
- `WRITE VOLUME` on a Unity Catalog volume, if the customer wants the consumption export saved there

#### 2. Install the skill folder

**Option A — Databricks CLI (from a machine that already has this repo)**

Target the workspace where the skill will run:

```bash
cd fivetran-lfc-migration-skill
SKILL_SRC=skills/fivetran-to-lakeflow-migration
PROFILE=<workspace-profile>

USER=$(databricks current-user me --profile "$PROFILE" -o json \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['userName'])")
TARGET="/Users/${USER}/.assistant/skills/fivetran-to-lakeflow-migration"

find "$SKILL_SRC" -type d | while read -r d; do
  rel="${d#"$SKILL_SRC"/}"
  dest="$TARGET"
  [ "$rel" != "$d" ] && dest="$TARGET/$rel"
  databricks workspace mkdirs "$dest" --profile "$PROFILE"
done

find "$SKILL_SRC" -type f ! -name '.DS_Store' | while read -r f; do
  rel="${f#"$SKILL_SRC"/}"
  databricks workspace import "$TARGET/$rel" \
    --file "$f" --format AUTO --overwrite --profile "$PROFILE"
done

databricks workspace list "$TARGET" --profile "$PROFILE"
databricks workspace list "$TARGET/scripts/ftlfc" --profile "$PROFILE"
```

For a workspace-wide install, set `TARGET=/Workspace/.assistant/skills/fivetran-to-lakeflow-migration`
instead (admin required).

**Option B — Workspace UI (customer self-install)**

1. In Genie Code, open **Settings → Open skills folder** (or create
   `/Users/<you>/.assistant/skills/` in the workspace file browser).
2. Create a folder named `fivetran-to-lakeflow-migration`.
3. Import the contents of `skills/fivetran-to-lakeflow-migration/` from the
   zip or Git checkout: `SKILL.md`, `requirements.txt`,
   `references/`, `scripts/`, `fixtures/`. Keep the same relative layout.

Genie Code picks up new skills on the **next** Agent-mode chat. Edits do not
apply to an already-open chat — start a new one (hard-refresh the tab if the
description looks stale).

#### 3. Run the skill in Genie Code

1. Open **Genie Code → Agent mode**.
2. `@` mention `fivetran-to-lakeflow-migration`, or ask in natural language:

   - *Assess our Fivetran estate for Lakeflow Connect migration*
   - *Migrate Fivetran connectors to Lakeflow Connect*
   - *Run the Fivetran to Lakeflow Connect skill end to end, stop at each review gate*

3. Provide Fivetran credentials from a **Databricks secret scope** so they
   stay in the workspace and never get pasted into a committed notebook.

   The customer creates the scope once (any machine with the workspace CLI
   profile; skip if it already exists):

   ```bash
   PROFILE=<workspace-profile>
   databricks secrets create-scope fivetran --profile "$PROFILE"
   databricks secrets put-secret fivetran api-key \
     --string-value <fivetran-api-key> --profile "$PROFILE"
   databricks secrets put-secret fivetran api-secret \
     --string-value <fivetran-api-secret> --profile "$PROFILE"
   ```

   Then, in the Genie Code session, point the skill at that scope by reading the
   values and exporting the environment variables the stages already use
   (Genie Code runs on Databricks compute, so `dbutils` is available):

   ```bash
   export FIVETRAN_API_KEY=$(python3 -c "from databricks.sdk.runtime import dbutils; print(dbutils.secrets.get('fivetran','api-key'))")
   export FIVETRAN_API_SECRET=$(python3 -c "from databricks.sdk.runtime import dbutils; print(dbutils.secrets.get('fivetran','api-secret'))")
   ```

   Use whatever scope and key names the customer chose. Because Genie Code may
   run each bash step in a fresh shell, re-export these at the start of any step
   that calls the Fivetran API (stage 1 discovery). No skill code changes are
   needed — the scripts read `FIVETRAN_API_KEY` / `FIVETRAN_API_SECRET` as usual.

4. Confirm the agent runs **preflight** first (`scripts/preflight.py --export
   --install-deps`) so `${SCRIPTS}` and `${FTLFC_OUT}` exist. Genie Code does
   not set `CLAUDE_SKILL_DIR`; each bash step may be a fresh shell.
5. Stop at **GATE 1** (assessment) and **GATE 2** (generated bundle). Do not
   deploy until the customer explicitly approves.

Artifacts (`inventory.json`, `mar.json`, `plan.json`, `bundle/`) are written
under the preflight output directory in the workspace. They are customer data
— keep them out of shared Git until secrets are confirmed redacted.

**Compute note:** stage 6 (`databricks bundle deploy`) needs the Databricks
CLI on PATH. Serverless Genie Code often does **not** ship the CLI. For a
fully in-workspace run, attach Agent mode to a **classic cluster** that has
the CLI, or run only stages 1–5 in Genie Code and deploy the bundle from a
laptop or job that already has the CLI.

More runtime detail: [`references/genie-code.md`](skills/fivetran-to-lakeflow-migration/references/genie-code.md).

---

## Quick Start

Laptop / Claude Code. For a customer-run Genie Code session, use
[Installation → Genie Code](#genie-code-customer-or-prospect-workspace) instead.

```bash
source .venv/bin/activate
export FIVETRAN_API_KEY=... FIVETRAN_API_SECRET=...
S=skills/fivetran-to-lakeflow-migration/scripts

# Stage 1: Discover the Fivetran estate (read-only)
python3 $S/fivetran_discover.py -o out/inventory.json

# Stage 2: Collect Fivetran MAR and billing from the Platform Connector
python3 $S/fivetran_mar.py --warehouse-id <id> --profile <profile> \
  -o out/mar.json
# or: python3 $S/fivetran_mar.py --print-sql   (any warehouse)
# or: python3 $S/fivetran_mar.py --csv export.csv -o out/mar.json
# Optional: record the Fivetran spend the customer shares, as given
python3 $S/fivetran_consumption.py spend --month 2026-09 --monthly-usd 12000 \
  --basis invoice -o out/fivetran_spend.json

# Stage 3: Map connectors to Lakeflow Connect
python3 $S/plan_migration.py -i out/inventory.json -c <dest_catalog> \
  --mar out/mar.json -o out/plan.json

# Stage 4: Summarise recorded Fivetran consumption and spend (writes nothing)
python3 $S/fivetran_consumption.py summary -p out/plan.json --mar out/mar.json \
  --spend out/fivetran_spend.json

# ──── GATE 1: Review assessment; ask whether to save the consumption export ────

# Only if the customer agrees: CSV export for the Databricks account team
python3 $S/fivetran_consumption.py export -p out/plan.json --mar out/mar.json \
  --spend out/fivetran_spend.json --warehouse-id <id> --profile <profile> -o out

# Stage 5: Generate Databricks Asset Bundle
python3 $S/generate_bundle.py -p out/plan.json -o out/bundle \
  --name <customer>-lfc --host https://<workspace>.cloud.databricks.com

# ──── GATE 2: Review bundle before deploying ────

# Stage 6: Deploy
cd out/bundle
./scripts/create_connections.sh <profile>
databricks bundle validate --strict -t dev
databricks bundle deploy -t dev
```

Run `--help` on any script for the full set of options.

---

## Pipeline Stages

### Stage 1: Discover

Calls the Fivetran REST API to inventory all connections, schemas, and tables.
`--columns` (one request per table, rate-limited) is optional: managed
connectors read primary keys from the source, so planning does not need it.

### Stage 2: Measure

Reads MAR (Monthly Active Rows) per table, plus Fivetran's billed `usage_cost`
and (on credit-based plans) `credits_used`, from the Fivetran Platform Connector
tables. Supports direct Databricks, Snowflake, BigQuery, and Redshift queries,
CSV import, or SQL print for other warehouses. Without the Platform Connector,
the spend the customer shares is recorded as given in `fivetran_spend.json`.

### Stage 3: Map

Maps each Fivetran connector to a Lakeflow Connect target using a catalog of 70+
service IDs. Classifies every connection by effort level (low / medium / high /
blocked) across three dimensions: connector availability, connection
scriptability, and source-side prerequisites.

### Stage 4: Consumption

Prints the recorded Fivetran consumption and spend for Gate 1. If the customer
agrees, writes a CSV export for their Databricks account team:
`connections.csv` (one row per connection with the Lakeflow Connect cost
estimator inputs: `source_type`, `num_rows` per run, `num_flows`, `cloud`,
`region`), `fivetran_billing.csv`, `mar_by_table.csv`, and a `README.txt`. The
skill never prices Lakeflow Connect, compares costs, or states a saving.

### Stage 5: Generate

Emits a Databricks Asset Bundle with ingestion pipelines, gateway pipelines
(for CDC sources), companion jobs, and a UC connection creation script.

### Stage 6: Deploy

Validates and deploys the bundle. Includes a five-step cutover protocol with
parallel-run validation before pausing Fivetran.

---

## Review Gates

### Gate 1 — Assessment Review (after stages 1–4)

The agent presents: estate summary, effort breakdown, blockers, warnings,
recorded Fivetran consumption and spend, and a migration recommendation. The
customer may narrow scope or name existing UC connections to reuse
(`--use-connection`). The agent also asks whether to save the consumption
export, and writes it only after a yes.

### Gate 2 — Conversion Review (after stage 5)

The agent presents: bundle contents, excluded connections, manual steps
(browser-OAuth connections listed under "Manual connections" in the bundle
README), and schedule mapping. The customer may adjust schemas,
frequencies, or table scope.

Neither gate can be skipped. Do not proceed until the customer explicitly
approves.

---

## Project Layout

```
fivetran-lfc-migration-skill/
├── .claude-plugin/
│   └── plugin.json              # Claude Code plugin manifest
├── skills/fivetran-to-lakeflow-migration/
│   ├── SKILL.md                 # Skill definition (Genie Code + Claude Code)
│   ├── requirements.txt         # Runtime deps for Genie Code upload
│   ├── references/
│   │   ├── genie-code.md        # Genie Code integration guide
│   │   ├── fivetran-api.md      # Fivetran API endpoints and rate limits
│   │   ├── lakeflow-connect-api.md  # Connection types, pipelines, gateways
│   │   ├── connector-coverage.md    # Fivetran → LFC mapping with release states
│   │   └── bundles.md               # Bundle YAML patterns and commands
│   ├── scripts/
│   │   ├── fivetran_discover.py     # Stage 1: Discovery
│   │   ├── fivetran_mar.py          # Stage 2: MAR and billing collection
│   │   ├── plan_migration.py        # Stage 3: Migration planning
│   │   ├── fivetran_consumption.py  # Stages 2 and 4: spend, summary, export
│   │   ├── generate_bundle.py       # Stage 5: Bundle generation
│   │   ├── preflight.py             # Genie Code environment setup
│   │   └── ftlfc/                   # Shared library
│   │       ├── __init__.py
│   │       ├── fivetran.py          # Read-only Fivetran REST client
│   │       ├── inventory.py         # Inventory normalization
│   │       ├── mar.py               # MAR and billing collection
│   │       ├── catalog.py           # Connector mapping catalog (70+ entries)
│   │       ├── mapping.py           # Migration plan builder
│   │       ├── consumption.py       # Spend record and consumption export
│   │       └── bundle.py            # Asset Bundle emitter
│   └── fixtures/
│       └── inventory.sample.json    # Sample data for offline testing
├── tests/
│   ├── test_fivetran.py
│   ├── test_mar.py
│   ├── test_mapping.py
│   ├── test_consumption.py
│   ├── test_bundle.py
│   ├── test_bundle_schema.py
│   └── test_preflight.py
├── pyproject.toml               # Project metadata, ruff + pytest config
├── requirements.txt             # pip requirements (runtime + dev)
└── README.md
```

---

## Genie Code Integration

Install and invoke steps for a customer-run Agent-mode session are under
[Installation → Genie Code](#genie-code-customer-or-prospect-workspace).

This skill maps to the Genie Code migration framework:

| Genie Code Phase | Skill Stages | Gate | Artifacts |
|---|---|---|---|
| **Assessment** | Discover, Measure, Map, Consumption | **GATE 1** | `inventory.json`, `mar.json`, `fivetran_spend.json`, `plan.json`, consumption export (with consent) |
| **Conversion** | Generate | **GATE 2** | `bundle/` |
| **Data Migration** | Deploy (parallel run) | — | live pipelines |
| **Reconciliation** | Deploy cutover | — | row-count validation |

See [`references/genie-code.md`](skills/fivetran-to-lakeflow-migration/references/genie-code.md)
for runtime differences vs Claude Code, preflight, Fivetran MCP, and
troubleshooting.

---

## Offline / Demo Mode

No Fivetran credentials? Start from the sample inventory:

```bash
S=skills/fivetran-to-lakeflow-migration/scripts

python3 $S/plan_migration.py \
  -i skills/fivetran-to-lakeflow-migration/fixtures/inventory.sample.json \
  -c main_prod -o out/plan.json

python3 $S/fivetran_consumption.py summary -p out/plan.json

python3 $S/generate_bundle.py -p out/plan.json -o out/bundle --name demo-lfc
```

This exercises stages 3 through 5 end-to-end with realistic fixture data.

---

## Development

### Run tests

```bash
source .venv/bin/activate
python3 -m pytest tests/ -q
```

The tests cover inventory normalization, connector catalog resolution, plan
generation, MAR and billing loading, the consumption export, bundle emission,
and DAB schema validation.

### Lint and format

```bash
python3 -m ruff check .
python3 -m ruff format --check .
```

### Bundle schema validation

One test generates a bundle from the sample inventory and validates it against
the Databricks Asset Bundle JSON schema from `databricks bundle schema`
(verified on CLI v1.1.0). The test auto-skips when the CLI is absent.

---

## Design Notes

**Discovery is strictly read-only.** The client issues only GET requests. It
never calls `POST /schemas/reload`, which can alter which tables a customer has
selected.

**Credentials are needed from two places.** The Fivetran REST API gives complete
topology but exposes no usage data. MAR and billing live only in the Platform
Connector tables inside the customer's destination warehouse.

**Support is three independent questions, not one flag.** Whether a managed
connector exists, whether its UC connection can be created without a browser,
and whether the source system's prerequisites can be scripted. These three
dimensions produce the four effort levels (low / medium / high / blocked).
Only `blocked` (no managed connector) keeps a connection out of the bundle;
browser-OAuth-only connectors are `high` and still get a pipeline and job.

**The skill records cost; it does not estimate it.** It captures Fivetran MAR,
billed amounts, and any spend the customer shares, exactly as given, and exports
them in the input shape of the Lakeflow Connect cost estimator. The Databricks
account team runs any cost modelling outside the skill. The export is written
only after the customer agrees to it.

**Inventory files are customer data.** Secret-looking config values are redacted
before anything is written. `out/` is gitignored. Connect Card tokens are
credentials and are never captured.

---

## References

Detailed API references live alongside the skill and are loaded on demand:

- [`fivetran-api.md`](skills/fivetran-to-lakeflow-migration/references/fivetran-api.md)
  — Fivetran endpoints, response shapes, rate limits, Platform Connector MAR and billing tables
- [`lakeflow-connect-api.md`](skills/fivetran-to-lakeflow-migration/references/lakeflow-connect-api.md)
  — Connection types, auth modes, `ingestion_definition`, gateways, scheduling
- [`connector-coverage.md`](skills/fivetran-to-lakeflow-migration/references/connector-coverage.md)
  — Fivetran connector to Lakeflow Connect mapping with release states
- [`bundles.md`](skills/fivetran-to-lakeflow-migration/references/bundles.md)
  — Bundle YAML patterns, per-environment parameters, bundle commands
- [`genie-code.md`](skills/fivetran-to-lakeflow-migration/references/genie-code.md)
  — Genie Code publish, preflight, framework mapping, troubleshooting

---

## License

&copy; 2026 Databricks, Inc. All rights reserved. The source in this project is
provided subject to the Databricks License [https://databricks.com/db-license-source].
All included or referenced third-party libraries are subject to the licenses set
forth below.

| Package | License | Copyright |
|---------|---------|-----------|
| [PyYAML](https://pyyaml.org/) | MIT | Copyright (c) 2017-2021 Ingy döt Net; Copyright (c) 2006-2016 Kirill Simonov |
| [pytest](https://docs.pytest.org/) | MIT | Copyright (c) 2004 Holger Krekel and others |
| [ruff](https://docs.astral.sh/ruff/) | MIT | Copyright (c) 2022 Charlie Marsh |
| [jsonschema](https://python-jsonschema.readthedocs.io/) | MIT | Copyright (c) 2013 Julian Berman |

## Project support

Please note the code in this project is provided for your exploration only, and
is not formally supported by Databricks with Service Level Agreements (SLAs).
It is provided AS-IS and we do not make any guarantees of any kind. Please do
not submit a support ticket relating to any issues arising from the use of this
project. Any issues discovered through the use of this project should be filed
as GitHub Issues on the repo. They will be reviewed as time permits, but there
are no formal SLAs for support.
