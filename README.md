# Fivetran to Lakeflow Connect Migration Skill

An agent skill that migrates a customer's ingestion estate from **Fivetran** to
**Databricks Lakeflow Connect**. It discovers what they run today, collects
billing data, maps each connector to its Lakeflow equivalent, models cost, and
generates deployable replacement pipelines as a **Databricks Asset Bundle**.

Built for Databricks Field Engineering. Runs in **Claude Code** (plugin) and
**Genie Code** (agentic orchestration).

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
artifact and writes its own. All artifacts are JSON files on disk — human-readable,
diffable, and versionable. Stages 1 through 5 are **read-only** with respect to
both Fivetran and Databricks.

```
1.   Discover    ──> inventory.json       (Fivetran API)
2.   Measure     ──> mar.json             (Platform Connector tables)
2.5  Telemetry   ──> telemetry.json       (Databricks system tables)
3.   Map         ──> plan.json            (connector catalog)
4.   Compare     ──> cost.json            (billing model)
                     ▓▓ GATE 1 ▓▓         human review
5.   Generate    ──> bundle/              (Asset Bundle YAML)
                     ▓▓ GATE 2 ▓▓         human review
6.   Deploy      ──> live pipelines       (databricks bundle deploy)
```

Two mandatory **review gates** enforce human approval. The agent stops after
assessment (scope + cost) and again after conversion (generated bundle). Neither
gate can be skipped.

---

## Prerequisites

| Requirement | Version | Purpose |
|---|---|---|
| Python | >= 3.10 | All scripts |
| PyYAML | >= 6.0.1 | Bundle YAML emission (only runtime dependency) |
| Fivetran API key + secret | — | Stage 1 discovery (Account Settings > API Config) |
| Databricks CLI | >= 0.200 | Stage 2 MAR queries, stage 2.5 telemetry, stage 6 deploy |
| Databricks SQL warehouse | — | Stages 2, 2.5, and 4 (optional; CSV and `--print-sql` paths exist) |

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
git clone https://github.com/priyal-c/fivetran-lfc-migration-skill.git
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

Required for stages 2, 2.5, and 6. If the CLI is already configured, no extra
setup is needed. Otherwise:

```bash
databricks auth login --profile <profile-name>
```

### Genie Code (customer or prospect workspace)

Use this when the customer wants to run the skill **inside Databricks Genie
Code Agent mode**, on their workspace identity, with credentials that never
leave their environment. Field Eng typically uploads the skill; the customer
runs the chat.

Official skill locations ([Genie Code skills](https://docs.databricks.com/aws/en/genie-code/skills)):

| Scope | Workspace path | Who installs |
|---|---|---|
| **User skill** (typical for a POC) | `/Users/<workspace-user>/.assistant/skills/fivetran-to-lakeflow-migration/` | Any user, or FE using that user's CLI profile |
| **Workspace skill** (shared rollout) | `/Workspace/.assistant/skills/fivetran-to-lakeflow-migration/` | Workspace admin |

Upload the **entire** skill folder: `SKILL.md`, `requirements.txt`,
`references/`, `scripts/` (including `scripts/ftlfc/`), and `fixtures/`.

#### 1. Prerequisites in the customer workspace

- Genie Code **Agent mode** enabled
- Permission to write under `.assistant/skills/` (user folder or workspace folder)
- Fivetran **Account Settings > API Config** key + secret (read-only; Standard plan or above)
- A SQL warehouse the user can query, if MAR lives in Databricks (stage 2) or for system-table telemetry (stage 2.5)
- `SELECT` on `system.billing.list_prices` (and related system tables) if you want grounded DBU rates

#### 2. Install the skill folder

**Option A — Databricks CLI (from a machine that already has this repo)**

Target the customer's workspace profile, not an internal FE workspace:

```bash
cd fivetran-lfc-migration-skill
SKILL_SRC=skills/fivetran-to-lakeflow-migration
PROFILE=<customer-workspace-profile>

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
   zip or Git checkout FE provided: `SKILL.md`, `requirements.txt`,
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

3. When asked, set Fivetran credentials **in that session** (do not paste them
   into notebooks that will be committed):

   ```bash
   export FIVETRAN_API_KEY=...
   export FIVETRAN_API_SECRET=...
   ```

4. Confirm the agent runs **preflight** first (`scripts/preflight.py --export
   --install-deps`) so `${SCRIPTS}` and `${FTLFC_OUT}` exist. Genie Code does
   not set `CLAUDE_SKILL_DIR`; each bash step may be a fresh shell.
5. Stop at **GATE 1** (assessment) and **GATE 2** (generated bundle). Do not
   deploy until the customer explicitly approves.

Artifacts (`inventory.json`, `plan.json`, `cost.json`, `bundle/`) are written
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
python3 $S/fivetran_discover.py --columns -o out/inventory.json

# Stage 2: Collect MAR billing data
python3 $S/fivetran_mar.py --warehouse-id <id> --profile <profile> \
  -o out/mar.json
# or: python3 $S/fivetran_mar.py --print-sql   (any warehouse)
# or: python3 $S/fivetran_mar.py --csv export.csv -o out/mar.json

# Stage 2.5: Ground cost model with Databricks system table telemetry
python3 $S/databricks_telemetry.py --warehouse-id <id> -o out/telemetry.json

# Stage 3: Map connectors to Lakeflow Connect
python3 $S/plan_migration.py -i out/inventory.json -c <dest_catalog> \
  --mar out/mar.json -o out/plan.json

# Stage 4: Compare Fivetran cost vs Lakeflow Connect
python3 $S/compare_cost.py -p out/plan.json --mar out/mar.json \
  --telemetry out/telemetry.json -o out/cost.json

# ──── GATE 1: Review assessment before proceeding ────

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
Pass `--columns` to resolve primary keys (one request per table, rate-limited).

### Stage 2: Measure

Reads MAR (Monthly Active Rows) and spend from the Fivetran Platform Connector
tables. Supports direct Databricks query, CSV import, or SQL print for other
warehouses.

### Stage 2.5: Telemetry

Queries `system.billing.list_prices`, `system.billing.usage`, and
`system.lakeflow.pipelines` to replace hardcoded cost-model defaults with the
customer's actual DBU rates and any existing Lakeflow pipeline consumption data.

### Stage 3: Map

Maps each Fivetran connector to a Lakeflow Connect target using a catalog of 70+
service IDs. Classifies every connection by effort level (low / medium / high /
blocked) across three dimensions: connector availability, connection
scriptability, and source-side prerequisites.

### Stage 4: Compare

Produces a Fivetran-vs-Lakeflow cost comparison. The Fivetran side is
measurable; the Lakeflow side is a scenario with stated assumptions. Every
assumption is printed with every number.

### Stage 5: Generate

Emits a Databricks Asset Bundle with ingestion pipelines, gateway pipelines
(for CDC sources), companion jobs, and a UC connection creation script.

### Stage 6: Deploy

Validates and deploys the bundle. Includes a five-step cutover protocol with
parallel-run validation before pausing Fivetran.

---

## Review Gates

### Gate 1 — Assessment Review (after stages 1–4)

The agent presents: estate summary, effort breakdown, blockers, warnings, cost
comparison with assumptions, and a migration recommendation. The customer may
narrow scope, re-run with `--columns`, or provide measured pilot data.

### Gate 2 — Conversion Review (after stage 5)

The agent presents: bundle contents, excluded connections, manual steps
(browser OAuth), and schedule mapping. The customer may adjust schemas,
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
│   │   └── bundles-and-cost.md      # Bundle YAML, billing tables, cost queries
│   ├── scripts/
│   │   ├── fivetran_discover.py     # Stage 1: Discovery
│   │   ├── fivetran_mar.py          # Stage 2: MAR collection
│   │   ├── databricks_telemetry.py  # Stage 2.5: System table telemetry
│   │   ├── plan_migration.py        # Stage 3: Migration planning
│   │   ├── compare_cost.py          # Stage 4: Cost comparison
│   │   ├── generate_bundle.py       # Stage 5: Bundle generation
│   │   ├── preflight.py             # Genie Code environment setup
│   │   └── ftlfc/                   # Shared library
│   │       ├── __init__.py
│   │       ├── fivetran.py          # Read-only Fivetran REST client
│   │       ├── inventory.py         # Inventory normalization
│   │       ├── mar.py               # MAR and spend collection
│   │       ├── catalog.py           # Connector mapping catalog (70+ entries)
│   │       ├── mapping.py           # Migration plan builder
│   │       ├── costs.py             # Cost comparison model
│   │       ├── system_tables.py     # Databricks system table telemetry
│   │       └── bundle.py            # Asset Bundle emitter
│   └── fixtures/
│       └── inventory.sample.json    # Sample data for offline testing
├── tests/
│   ├── test_fivetran.py
│   ├── test_mar.py
│   ├── test_mapping.py
│   ├── test_costs.py
│   ├── test_system_tables.py
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
| **Assessment** | Discover, Measure, Telemetry, Map, Compare | **GATE 1** | `inventory.json`, `mar.json`, `telemetry.json`, `plan.json`, `cost.json` |
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

python3 $S/compare_cost.py -p out/plan.json -o out/cost.json

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

190 tests cover inventory normalization, connector catalog resolution, plan
generation, cost model arithmetic, system table telemetry, bundle emission,
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
topology but exposes no usage data. MAR and spend live only in the Platform
Connector tables inside the customer's destination warehouse.

**Support is three independent questions, not one flag.** Whether a managed
connector exists, whether its UC connection can be created without a browser,
and whether the source system's prerequisites can be scripted. These three
dimensions produce the four effort levels (low / medium / high / blocked).

**The cost comparison is asymmetric on purpose.** Fivetran spend is measurable.
Lakeflow Connect spend is not predictable in advance — Databricks publishes no
DBU-per-row coefficient. The Lakeflow figure is a scenario whose assumptions
are printed with every number. Stage 2.5 grounds as many assumptions as possible
from system tables; measured pilot usage replaces the model entirely.

**Inventory files are customer data.** Secret-looking config values are redacted
before anything is written. `out/` is gitignored. Connect Card tokens are
credentials and are never captured.

---

## References

Detailed API references live alongside the skill and are loaded on demand:

- [`fivetran-api.md`](skills/fivetran-to-lakeflow-migration/references/fivetran-api.md)
  — Fivetran endpoints, response shapes, rate limits, MAR and pricing model
- [`lakeflow-connect-api.md`](skills/fivetran-to-lakeflow-migration/references/lakeflow-connect-api.md)
  — Connection types, auth modes, `ingestion_definition`, gateways, scheduling
- [`connector-coverage.md`](skills/fivetran-to-lakeflow-migration/references/connector-coverage.md)
  — Fivetran connector to Lakeflow Connect mapping with release states
- [`bundles-and-cost.md`](skills/fivetran-to-lakeflow-migration/references/bundles-and-cost.md)
  — Bundle YAML patterns, billing system tables, runnable cost queries
- [`genie-code.md`](skills/fivetran-to-lakeflow-migration/references/genie-code.md)
  — Genie Code publish, preflight, framework mapping, troubleshooting

---

## License

Internal — Databricks Field Engineering. Not for external distribution.
