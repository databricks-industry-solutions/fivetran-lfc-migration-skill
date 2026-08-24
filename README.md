# Fivetran → Lakeflow Connect migration skill

An agent skill that migrates a customer's ingestion estate from Fivetran to
Databricks Lakeflow Connect: discover what they run today, model the cost
difference, map each connector to its Lakeflow equivalent, and generate plus
deploy the replacement pipelines as a Databricks Asset Bundle.

Built for Field Engineering. Intended to be integrated into Genie Code and
Lakebridge.

## Status

All six stages are implemented, with 162 tests. One of those generates a bundle
from the sample inventory and validates it against the Databricks Asset Bundle
JSON schema emitted by `databricks bundle schema` (verified on CLI v1.1.0; the
test skips when the CLI is absent). Bundles have not yet been through
`databricks bundle validate` against a live workspace, which additionally checks
that referenced connections and catalogs actually exist.

| Stage | Script | Produces |
|---|---|---|
| 1. Discover | `fivetran_discover.py` | `inventory.json` |
| 2. Measure | `fivetran_mar.py` | `mar.json` |
| 3. Map | `plan_migration.py` | `plan.json` |
| 4. Compare | `compare_cost.py` | `cost.json` |
| 5. Generate | `generate_bundle.py` | a Databricks Asset Bundle |
| 6. Deploy | `databricks bundle deploy` | running pipelines |

**Nothing here has been run against a live Fivetran account.** The Fivetran side
comes from official documentation and path probing, not observed responses. The
Databricks side is better grounded: connection types, auth modes, and pipeline
shapes were validated against 1,662 real connections and live pipeline specs in
a workspace. Validate against one real Fivetran account before trusting
generated output.

## Layout

```
skills/fivetran-to-lakeflow-migration/
├── SKILL.md                    # the skill definition the agent loads
├── references/                 # loaded on demand, not held in context
│   ├── fivetran-api.md         # endpoints, MAR, pricing, rate limits
│   ├── lakeflow-connect-api.md # connections, pipelines, scheduling
│   ├── connector-coverage.md   # what exists, and source-side prerequisites
│   └── bundles-and-cost.md     # bundle YAML, billing tables, cost queries
├── scripts/
│   ├── fivetran_discover.py    # stage 1
│   ├── fivetran_mar.py         # stage 2
│   ├── plan_migration.py       # stage 3
│   ├── compare_cost.py         # stage 4
│   ├── generate_bundle.py      # stage 5
│   └── ftlfc/                  # shared library
│       ├── fivetran.py         # read-only REST client
│       ├── inventory.py        # inventory normalisation
│       ├── mar.py              # MAR and spend collection
│       ├── catalog.py          # Fivetran service -> Lakeflow target
│       ├── mapping.py          # migration plan builder
│       ├── costs.py            # cost comparison
│       └── bundle.py           # asset bundle emitter
└── fixtures/                   # sample inventory for offline testing
tests/                          # pytest suite over the library
```

## Quick start

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
export FIVETRAN_API_KEY=... FIVETRAN_API_SECRET=...
export S=skills/fivetran-to-lakeflow-migration/scripts

# 1. Discover the estate. Read-only.
.venv/bin/python $S/fivetran_discover.py --columns -o out/inventory.json

# 2. Collect MAR. Pick whichever path fits the destination warehouse.
.venv/bin/python $S/fivetran_mar.py --print-sql            # any warehouse
.venv/bin/python $S/fivetran_mar.py --csv export.csv -o out/mar.json

# 3. Map onto Lakeflow Connect.
.venv/bin/python $S/plan_migration.py -i out/inventory.json -c main_prod \
  --mar out/mar.json -o out/plan.json

# 4. Compare cost.
.venv/bin/python $S/compare_cost.py -p out/plan.json --mar out/mar.json -o out/cost.json

# 5. Generate the bundle.
.venv/bin/python $S/generate_bundle.py -p out/plan.json -o out/bundle --name acme-lfc

# 6. Deploy.
cd out/bundle && ./scripts/create_connections.sh <profile>
databricks bundle validate --strict -t dev && databricks bundle deploy -t dev
```

To try it without Fivetran credentials, start from the sample inventory at
`skills/fivetran-to-lakeflow-migration/fixtures/inventory.sample.json`. Run
`--help` on any script for full options.

## Design notes

**Discovery is strictly read-only.** The client issues only GET requests. It
never calls `POST /schemas/reload`, which looks like a discovery endpoint but
can change which tables a customer has selected.

**Credentials are needed from two places.** The Fivetran REST API gives complete
topology but exposes no usage data whatsoever — MAR and spend live only in the
Platform Connector tables inside the customer's destination warehouse. Cost is
usually the half the customer cares about, so plan for that second credential up
front rather than discovering it mid-engagement.

**Fivetran reports only overridden columns.** `GET /connections/{id}/schemas`
returns columns that were explicitly customised, not the full column list. A
table with an empty `columns` map is one nobody touched, where every column
syncs. Exclusions and hashing are therefore reliable, but primary keys are not —
a key nobody edited never appears. Records carry `primary_keys_known` so later
stages cannot mistake "unknown" for "none" and emit a pipeline with wrong SCD
behaviour. Pass `--columns` to resolve them properly.

**Inventory files are customer data.** Secret-looking config values are redacted
before anything is written, and `out/` is gitignored. Connect Card tokens are
credentials and are never captured.

**Support is three independent questions, not one flag.** Whether a managed
connector exists, whether its connection can be created without a human in a
browser, and whether the source system's own setup can be scripted. These come
apart constantly: Workday is scriptable on the Databricks side yet every
source-side step is a click in the Workday UI, while S3 has no managed connector
but migrates trivially via Auto Loader. The catalog keeps them separate.

**The cost comparison is asymmetric on purpose.** Fivetran spend is measurable.
Lakeflow Connect spend is not predictable in advance — Databricks publishes no
DBU-per-row coefficient and recommends benchmarking instead. So the Databricks
figure is a scenario whose assumptions are printed with every number, and
measured pilot usage replaces the model entirely when supplied.

## Development

```bash
.venv/bin/python -m pytest tests/ -q
.venv/bin/python -m ruff check . && .venv/bin/python -m ruff format --check .
```

## References

Detailed API references live alongside the skill and are loaded on demand:

- [`fivetran-api.md`](skills/fivetran-to-lakeflow-migration/references/fivetran-api.md)
  — endpoints, response shapes, rate limits, the MAR and pricing model, known gaps.
- [`lakeflow-connect-api.md`](skills/fivetran-to-lakeflow-migration/references/lakeflow-connect-api.md)
  — connection types and auth modes, `ingestion_definition`, gateways, scheduling.
- [`connector-coverage.md`](skills/fivetran-to-lakeflow-migration/references/connector-coverage.md)
  — which managed connectors exist, and per-source prerequisites.
- [`bundles-and-cost.md`](skills/fivetran-to-lakeflow-migration/references/bundles-and-cost.md)
  — bundle YAML, billing system tables, runnable cost queries.
