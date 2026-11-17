# Genie Code integration

How to run this skill inside [Databricks Genie Code](https://learn.microsoft.com/en-us/azure/databricks/genie-code/use-genie-code) agentic orchestration, alongside Lakebridge warehouse migrations.

## Framework mapping

Genie Code migrations are organised into four phases. This skill covers the **Data Migration** phase for ingestion (Fivetran → Lakeflow Connect). Assessment and conversion are upstream; reconciliation is the tail of deploy.

| Genie Code phase | Skill stages | Gate | Artifact |
|---|---|---|---|
| **Assessment** | 1 Discover, 2 Measure, 3 Map, 4 Consumption | **GATE 1** | `inventory.json`, `mar.json`, `fivetran_spend.json`, `plan.json`, consumption export (with consent) |
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
| Fivetran credentials | Unity Catalog secrets through the CLI profile (`--databricks-profile`), or env vars | Unity Catalog secrets or a workspace secret scope, read with `dbutils` (`--secret-store`, `--api-key-ref`, `--api-secret-ref`) |

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

The consumption export (stage 4, only after the customer agrees at Gate 1) can write straight to a Unity Catalog volume in Genie Code: pass `-o /Volumes/<catalog>/<schema>/<volume>` to `fivetran_consumption.py export`.

## Fivetran MCP alternative

When the Fivetran MCP is configured (e.g. via Cursor `user-fivetran`), Stage 1 discovery can use MCP read tools instead of `fivetran_discover.py`:

- `fivetran_list_groups` / `fivetran_list_connections_in_group`
- `fivetran_get_connection` / `fivetran_get_connection_schema_config`
- `fivetran_get_connection_column_config` (per table; the `--columns` equivalent, not needed for planning since managed connectors read keys from the source)

Build `inventory.json` from MCP responses using the schema in `references/fivetran-api.md`, or run `fivetran_discover.py` with `--secret-store` and the secret refs.

## Fivetran credentials

Store the key and secret as Unity Catalog secrets (Catalog Explorer, **Create >
Secret**) or in a workspace secret scope (`databricks secrets put-secret` with
no `--string-value`, so the CLI prompts). Discovery reads them in-process;
never echo them into environment variables or chat. Unity Catalog secrets need
`READ SECRET` on each secret plus `USE CATALOG` and `USE SCHEMA`. `dbutils`
reads them on serverless environment 4+ or DBR 17.3 LTS+; on older compute
discovery calls the Unity Catalog secrets API instead.

If a secret read fails, stop. The error names the ref, the store, and the
reason, and ends with "Stop and ask the user to fix this ref; do not look for
the secret anywhere else." Your next message reports those three things and
the fix from the troubleshooting table below, then waits for the user. Do not
list secret scopes, search catalogs, schemas, or `information_schema`, or try
any other secret, even to suggest one; re-run discovery only after the user
fixes the ref or the secret.

## Orchestration with other migration skills

- **Warehouse assessment/conversion:** `lakehouse-migrator` skills (Vertica, Hive, etc.) — run in parallel; ingestion migration is independent.
- **Dashboard migration:** built-in Genie Code `tableauMigrationAgent` / `powerBIMigrationAgent` — different track.
- **Post-migration ETL:** after Lakeflow lands bronze, use downstream transformation frameworks to build silver/gold layers.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `No module named 'yaml'` | `python3 ${SCRIPTS}/preflight.py --install-deps` |
| Scripts not found | Re-run preflight `--export`; confirm skill uploaded to `.assistant/skills/` |
| Fivetran 401 | The secret holds a revoked or wrong key; update it in place, or authenticate Fivetran MCP |
| `could not read uc secret '…': not found` | Stop and ask the user to correct the ref (`catalog.schema.secret`) or create the secret in Catalog Explorer > **Create > Secret**; do not search for it |
| `could not read … secret '…': permission denied` | Stop and ask the owner to grant `READ SECRET` on the secret and `USE CATALOG` / `USE SCHEMA` on its parents (`READ` on a scope) |
| `scope secret '…' can only be read on Databricks compute` | Run discovery in Genie Code, or switch to Unity Catalog secrets with `--databricks-profile` |
| `bundle validate` auth error | `databricks auth login --profile <profile>` |
| MAR stage fails | Platform Connector missing — use `--print-sql` and `--csv`, or skip it and record the customer's spend with `fivetran_consumption.py spend` |
| No `credits_used` table | Expected on dollar-billed plans; `fivetran_mar.py` skips it and keeps `usage_cost` |
| Export copy to volume fails | The files stay in `${FTLFC_OUT}`; confirm the volume exists and the user has `WRITE VOLUME` on it |
