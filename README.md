# Fivetran → Lakeflow Connect migration skill

An agent skill that migrates a customer's ingestion estate from Fivetran to
Databricks Lakeflow Connect: discover what they run today, model the cost
difference, map each connector to its Lakeflow equivalent, and generate plus
deploy the replacement pipelines as a Databricks Asset Bundle.

Built for Field Engineering. Intended to be integrated into Genie Code and
Lakebridge.

## Status

Early. Stages 1 and 2 are implemented and tested; the rest are in progress.

| Stage | What it does | State |
|---|---|---|
| 1. Discover | Read the full Fivetran estate over the REST API | Implemented |
| 2. Measure | Collect MAR and spend from the Platform Connector | Implemented |
| 3. Map | Match each Fivetran connector to a Lakeflow Connect equivalent | In progress |
| 4. Compare | Model Fivetran cost against projected Lakeflow Connect cost | In progress |
| 5. Generate | Emit UC connections and ingestion pipelines as a bundle | In progress |
| 6. Deploy | Validate and deploy the bundle, then verify the first sync | In progress |

**Nothing here has been run against a live Fivetran account yet.** Every API
shape comes from official documentation and path probing, not observed
responses. Validate against one real account before trusting generated output.

## Layout

```
skills/fivetran-to-lakeflow-migration/
├── SKILL.md                  # the skill definition the agent loads
├── references/               # API references, loaded on demand
│   └── fivetran-api.md
├── scripts/
│   ├── fivetran_discover.py  # stage 1 entrypoint
│   ├── fivetran_mar.py       # stage 2 entrypoint
│   └── ftlfc/                # shared library
│       ├── fivetran.py       # read-only REST client
│       ├── inventory.py      # inventory normalisation
│       └── mar.py            # MAR and spend collection
└── fixtures/
tests/                        # pytest suite over the library
```

## Quick start

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

export FIVETRAN_API_KEY=... FIVETRAN_API_SECRET=...

# Stage 1: discover the estate. Read-only.
.venv/bin/python skills/fivetran-to-lakeflow-migration/scripts/fivetran_discover.py \
  --columns -o out/inventory.json

# Stage 2: collect MAR. Pick whichever path fits the destination warehouse.
.venv/bin/python skills/fivetran-to-lakeflow-migration/scripts/fivetran_mar.py --print-sql
.venv/bin/python skills/fivetran-to-lakeflow-migration/scripts/fivetran_mar.py \
  --csv mar_export.csv -o out/mar.json
```

Run `--help` on either script for the full options.

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

## Development

```bash
.venv/bin/python -m pytest tests/ -q
.venv/bin/python -m ruff check .
```

## References

- [`references/fivetran-api.md`](skills/fivetran-to-lakeflow-migration/references/fivetran-api.md)
  — endpoints, response shapes, rate limits, pricing model, and known gaps.
