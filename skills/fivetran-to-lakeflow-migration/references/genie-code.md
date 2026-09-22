# Genie Code integration

How to run this skill inside [Databricks Genie Code](https://learn.microsoft.com/en-us/azure/databricks/genie-code/use-genie-code) agentic orchestration, alongside Lakebridge warehouse migrations.

## Framework mapping

Genie Code migrations are organised into four phases. This skill covers the **Data Migration** phase for ingestion (Fivetran → Lakeflow Connect). Assessment and conversion are upstream; reconciliation is the tail of deploy.

| Genie Code phase | Skill stages | Gate | Artifact |
|---|---|---|---|
| **Assessment** | 1 Discover, 2 Measure, 2.5 Telemetry, 3 Map, 4 Compare | **GATE 1** | `inventory.json`, `mar.json`, `telemetry.json`, `plan.json`, `cost.json` |
| **Conversion** | 5 Generate | **GATE 2** | `bundle/` |
| **Data Migration** | 6 Deploy (parallel run) | — | live Lakeflow Connect pipelines |
| **Reconciliation** | 6 Deploy steps 2–5 | — | customer row-count validation |

The agent **must stop at each gate**, present the assessment or bundle to the customer, and wait for explicit approval before proceeding. See `SKILL.md` § Review gates for what to present at each gate.

## Publish to the workspace

Genie Code loads skills from:

```
/Workspace/Users/<userName>/.assistant/skills/fivetran-to-lakeflow-migration/
```

Upload the **entire** skill folder (`SKILL.md`, `requirements.txt`, `references/`, `scripts/`, `fixtures/`). Use the Databricks CLI:

```bash
PROFILE=<profile>
USER=$(databricks current-user me --profile "$PROFILE" -o json | python3 -c "import json,sys; print(json.load(sys.stdin)['userName'])")
TARGET="/Users/${USER}/.assistant/skills/fivetran-to-lakeflow-migration"
SKILL="/path/to/fivetran-lfc-migration-skill/skills/fivetran-to-lakeflow-migration"

databricks workspace mkdirs "$TARGET/scripts" --profile "$PROFILE"
databricks workspace import "$TARGET/SKILL.md" \
  --file "$SKILL/SKILL.md" --format AUTO --overwrite --profile "$PROFILE"
# Repeat for requirements.txt, references/*, scripts/*, fixtures/*
```

After upload, invoke in Genie Code with natural language: *"migrate Fivetran connectors to Lakeflow Connect"* or *"assess this customer's Fivetran estate for Lakeflow migration"*.

## Runtime differences from Claude Code

| Topic | Claude Code | Genie Code |
|---|---|---|
| `CLAUDE_SKILL_DIR` | Set automatically | **Not set** — resolve via preflight |
| Bash steps | Same shell session | Often a **fresh shell** per step — re-export paths |
| Script paths | Relative to repo | Use `${SCRIPTS}/…` after preflight |
| Dependencies | Repo venv | `pip install` from skill `requirements.txt` |
| Fivetran API | REST or Fivetran MCP | Prefer **Fivetran MCP** when configured |

## Preflight (run at the start of every bash step)

```bash
eval "$(python3 "${SCRIPTS:-skills/fivetran-to-lakeflow-migration/scripts}/preflight.py" \
  --skill-dir "${CLAUDE_SKILL_DIR:-/Workspace/Users/<you>/.assistant/skills/fivetran-to-lakeflow-migration}" \
  --profile <profile> --install-deps --export)"
```

On Genie Code, replace `<you>` with the workspace user email from `databricks current-user me`. The `--export` form prints:

```
export CLAUDE_SKILL_DIR='…'
export SCRIPTS='…'
export FTLFC_OUT='…'
```

All stage commands then use `${SCRIPTS}/fivetran_discover.py` and write artifacts under `${FTLFC_OUT}/`.

## Fivetran MCP alternative

When the Fivetran MCP is configured (e.g. via Cursor `user-fivetran`), Stage 1 discovery can use MCP read tools instead of `fivetran_discover.py`:

- `fivetran_list_groups` / `fivetran_list_connections_in_group`
- `fivetran_get_connection` / `fivetran_get_connection_schema_config`
- `fivetran_get_connection_column_config` (per table; the `--columns` equivalent, not needed for planning since managed connectors read keys from the source)

Build `inventory.json` from MCP responses using the schema in `references/fivetran-api.md`, or run `fivetran_discover.py` after exporting API credentials.

## Orchestration with other migration skills

- **Warehouse assessment/conversion:** `lakehouse-migrator` skills (Vertica, Hive, etc.) — run in parallel; ingestion migration is independent.
- **Dashboard migration:** built-in Genie Code `tableauMigrationAgent` / `powerBIMigrationAgent` — different track.
- **Post-migration ETL:** after Lakeflow lands bronze, use downstream transformation frameworks to build silver/gold layers.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `No module named 'yaml'` | `python3 ${SCRIPTS}/preflight.py --install-deps` |
| Scripts not found | Re-run preflight `--export`; confirm skill uploaded to `.assistant/skills/` |
| Fivetran 401 | Set API key/secret or authenticate Fivetran MCP |
| `bundle validate` auth error | `databricks auth login --profile <profile>` |
| MAR stage fails | Platform Connector missing — use `--print-sql` or invoice CSV |
| Telemetry returns no data | No existing Lakeflow pipelines — rates still grounded from `list_prices` |
| `system.billing` access denied | Workspace admin must grant `SELECT` on system tables |
