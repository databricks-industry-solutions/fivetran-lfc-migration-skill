# Fivetran REST API — discovery reference

Everything the migration skill needs to read a customer's Fivetran estate programmatically.

Verified 2026-08-24 against the official docs plus live path probes against `api.fivetran.com`.
The API routes before it authenticates, so an unknown path returns `404` while a real path
returns `401` — that makes path existence testable without credentials. Claims below are
marked **[confirmed]**, **[inferred]**, or **[unverified]**.

---

## 1. Auth and request basics

| Item | Value |
|---|---|
| Base URL | `https://api.fivetran.com` |
| Auth | HTTP Basic, `Authorization: Basic base64(api_key:api_secret)` |
| Required header | `Accept` (see versioning below) |
| Response envelope | `{"code": ..., "message": ..., "data": {...}}` — `data` only on success |

```bash
export FIVETRAN_API_KEY=... FIVETRAN_API_SECRET=...
curl -sS "https://api.fivetran.com/v1/account/info" \
  -H "Accept: application/json" \
  -u "$FIVETRAN_API_KEY:$FIVETRAN_API_SECRET"
```

Build the base64 payload with `printf`, not `echo` — a trailing newline yields `401`.

### Versioning is per-endpoint, not global

This is the single most common way to break a Fivetran client. Sending the wrong `Accept`
value returns **`406 Not Acceptable`**, not a fallback.

| Endpoint | Correct `Accept` |
|---|---|
| `GET /v1/connections` | `application/json;version=2` |
| `GET /v1/destinations/{id}` | `application/json;version=2` |
| `GET /v1/groups` | `application/json` |
| `GET /v1/connections/{id}/schemas` | `application/json` |
| `GET /v1/connections/{id}/schemas/{s}/tables/{t}/columns` | `application/json` |
| `GET /v1/metadata/connector-types` | `application/json` |
| `GET /v1/account/info` | `application/json` |

`ftlfc.fivetran` encodes this in `ACCEPT_BY_PATH`; do not "simplify" it to one global header.

### Pagination

Cursor-based and uniform across collection endpoints. Query params `cursor` and `limit`
(1–1000, default 100). Response carries `data.items[]` and `data.next_cursor`.

**Termination: `next_cursor` is absent, not null or empty.** Loop on key presence.

### Key types

`GET /v1/account/info` returns `user_id` for a scoped key and `system_key_id` for a system
key. Worth capturing: **a scoped key silently returns only what its user can see**, so the
discovered estate may be incomplete with no error. Warn when a scoped key is in use.

### REST API access requires a paid plan

REST API access is a Standard-plan feature. A customer on the **Free plan cannot be assessed
via the API at all** — fall back to manual export.

---

## 2. Endpoints used by discovery

All read-only. The skill never issues a mutating call against a customer's Fivetran account.

| Method | Path | Purpose |
|---|---|---|
| GET | `/v1/account/info` | Validate credentials, capture `account_id`, detect key type |
| GET | `/v1/groups` | List groups (1:1 with destinations) |
| GET | `/v1/destinations/{id}` | Destination platform, region, networking method |
| GET | `/v1/connections` | The bulk of the inventory, paginated |
| GET | `/v1/connections/{id}/schemas` | Schema/table selection tree |
| GET | `/v1/connections/{id}/schemas/{schema}/tables/{table}/columns` | Exhaustive column list for one table |
| GET | `/v1/metadata/connector-types` | Connector catalog: class, status, supported features |
| GET | `/v1/transformations` | dbt Core and Quickstart transformations |
| GET | `/v1/transformation-projects` | dbt Core projects |

### Path notes

- **`/v1/connectors` is a live legacy alias** for `/v1/connections`. Both resolve; no sunset
  date announced [confirmed via probe]. Use `/v1/connections`; keep the alias as a fallback.
- **`/v1/metadata/connectors` also still resolves** and is [inferred] to be a legacy alias for
  `/v1/metadata/connector-types`. Use `connector-types`.
- **`/v1/metadata/connectors` requires auth** despite the Getting Started page claiming it is
  publicly accessible. Live test returns `401 AuthFailed`. Do not build an unauthenticated
  pre-flight on it.
- **`/v1/dbt/*` no longer exists** — all paths return `404`. Use `/v1/transformations`.

### Never call these during discovery

`POST /v1/connections/{id}/schemas/reload` re-reads the schema from the live source. It is
slow, consumes the tight source-interaction rate limit, and its `exclude_mode` parameter can
**mutate which tables are selected**. `GET .../schemas` is the read-only equivalent.

---

## 3. Response shapes

### `GET /v1/groups`

Groups map **1:1 to destinations**, and `group.id`, `destination.id`, and a connection's
`group_id` are all the same string — no lookup table needed.

```json
{"code":"Success","data":{"items":[
  {"id":"schoolmaster_heedless","name":"Production","created_at":"2019-01-08T19:53:52Z"}
],"next_cursor":"eyJza2lwIjozfQ"}}
```

### `GET /v1/destinations/{id}`

```json
{"code":"Success","data":{
  "id":"schoolmaster_heedless","service":"snowflake","region":"AWS_US_EAST_1",
  "setup_status":"CONNECTED","networking_method":"Directly",
  "config":{"host":"acct.snowflakecomputing.com","database":"ANALYTICS","auth":"PASSWORD"}
}}
```

- `service` — destination platform. A customer already on `databricks` has half the target
  built. [unverified] whether the REST value is exactly `databricks`; the Platform Connector's
  `destination_type` enum uses `databricks`, but the two enums differ elsewhere
  (`snowflake` vs `snowflake_db`), so verify against a live account.
- `region` — a closed enum of 44 `AWS_*` / `AZURE_*` / `GCP_*` values. This is where Fivetran
  runs compute, and it is what you map to a Databricks workspace region.
- `networking_method` — `Directly` | `PrivateLink` | `SshTunnel` | `ProxyAgent`. Anything other
  than `Directly` means private-networking work in the migration.
- `config` — shape is entirely destination-specific. Treat as an opaque map; assume no key
  exists. Secrets are masked server-side, but redact again before writing to disk.

### `GET /v1/connections`

```json
{"code":"Success","data":{"items":[{
  "id":"speak_inexpensive","group_id":"schoolmaster_heedless","service":"salesforce",
  "schema":"salesforce","paused":false,
  "status":{"setup_state":"connected","sync_state":"scheduled","update_state":"on_schedule",
            "is_historical_sync":false,"tasks":[],"warnings":[]},
  "sync_frequency":360,"schedule_type":"auto","daily_sync_time":null,
  "data_delay_sensitivity":"NORMAL","data_delay_threshold":0,
  "networking_method":"Directly","destination_schema_names":"FIVETRAN_NAMING",
  "created_at":"2024-12-01T15:43:29Z","succeeded_at":"2026-08-24T14:02:11Z","failed_at":null,
  "config":{"instance_url":"https://acme.my.salesforce.com","api_version":"59.0"},
  "source_sync_details":{}
}],"next_cursor":"..."}}
```

| Field | Notes |
|---|---|
| `service` | Connector type id, e.g. `salesforce`. The join key to both the connector catalog and the Lakeflow Connect mapping. |
| `schema` | Destination schema **and** the connection's display name. Immutable after creation. |
| `status.setup_state` | `incomplete` \| `connected` \| `broken` |
| `status.sync_state` | `scheduled` \| `syncing` \| `paused` \| `rescheduled` |
| `status.update_state` | `on_schedule` \| `delayed` |
| `status.schema_status` | Present **only** for connectors supporting Universal Column Masking. Do not assume. |
| `sync_frequency` | Minutes. One of `1, 5, 15, 30, 60, 120, 180, 360 (default), 480, 720, 1440`. `1` and `5` are Enterprise+. |
| `daily_sync_time` | Meaningful only when `sync_frequency` is `1440`. |
| `data_delay_threshold` | Applies only when `data_delay_sensitivity` is `CUSTOM`. |
| `networking_method` | Plus response-only values `UnmanagedProxyAgent` and `Unknown`. |
| `destination_schema_names` | `FIVETRAN_NAMING` \| `SOURCE_NAMING` — affects target table naming. |
| `source_sync_details` | Read-only, connector-specific, **no stable schema**. May hold source scope ids or per-entity cursors. Capture opaquely. |

**`connect_card.token` is a credential** — a short-lived JWT authorizing reconfiguration of the
connection. Never log, cache, or write it to a migration artifact.

### `GET /v1/connections/{id}/schemas`

The highest-value endpoint for scoping. No pagination; returns the whole tree.

```json
{"code":"Success","data":{
  "enable_new_by_default": true,
  "schema_change_handling": "ALLOW_COLUMNS",
  "schemas": {"public": {
    "name_in_destination":"public","enabled":true,
    "tables": {"customers": {
      "name_in_destination":"customers","enabled":true,"sync_mode":"SOFT_DELETE",
      "supports_columns_config":true,"supports_history_mode":true,
      "enabled_patch_settings":{"allowed":true},
      "columns": {"email": {"name_in_destination":"email","enabled":true,"hashed":true,
                            "is_primary_key":false,"enabled_patch_settings":{"allowed":true}}}
    }}
  }}
}}
```

> **The `columns` map is not the column list.** Fivetran returns "only the columns that were
> explicitly overridden." Unedited columns following table defaults are omitted. A table with
> `"columns": {}` is a table nobody customized, where **every column syncs** — not a table
> with no columns.
>
> Getting this wrong silently under-reports scope and produces a migration plan that drops
> columns. The inventory marks such tables `columns_complete: false`.

To get the true column list, call the per-table endpoint, but only where the table reports
`supports_columns_config: true`:

```
GET /v1/connections/{id}/schemas/{schema}/tables/{table}/columns
```

Three constraints [confirmed]:

1. Works only when the connection is `Connected`.
2. Counts against the **source interaction** rate limit — the binding constraint on a large
   crawl at one call per table.
3. Schema/table names in the path are **case-sensitive**; wrong case gives `404`, not empty.

| Field | Meaning |
|---|---|
| `schema_change_handling` | `ALLOW_ALL` \| `ALLOW_COLUMNS` \| `BLOCK_ALL`. Maps onto Lakeflow schema-evolution policy. |
| `sync_mode` (table) | `SOFT_DELETE` \| `HISTORY` \| `LIVE`. Present **only** if the connector supports switching modes — absence is not a value. `HISTORY` is Fivetran's SCD type 2 and maps to `SCD_TYPE_2`. |
| `hashed` (column) | Column is hashed on write. A compliance requirement that must carry across, not silently drop. |
| `is_primary_key` (column) | Also the unit of MAR billing. |
| `enabled_patch_settings.reason_code` | Why `enabled` is locked. Tables: `SYSTEM_TABLE` \| `DELETED` \| `OTHER`. Columns: `SYSTEM_COLUMN` \| `DELETED` \| `OTHER`. **The two enums differ despite the shared field name.** |

Connector quirks: for Salesforce, Salesforce Sandbox, and NetSuite SuiteAnalytics the `schemas`
map holds exactly one entry, keyed `salesforce` or `netsuite`. The endpoint does not apply to
Magic Folder connectors at all.

### `GET /v1/metadata/connector-types`

```json
{"code":"Success","data":{"items":[{
  "id":"google_ads","name":"Google Ads","type":"Marketing",
  "connector_class":"standard","service_status":"general_availability",
  "link_to_docs":"https://fivetran.com/docs/connectors/applications/google-ads",
  "supported_features":[{"id":"HISTORY"},{"id":"COLUMN_HASHING"}]
}],"next_cursor":"..."}}
```

- `connector_class`: `standard` (full schema and sync coverage) vs `lite` (partial). A `lite`
  source often has no clean Lakeflow analogue.
- `service_status`: `general_availability` | `beta` | `private_preview` | `sunset` | `development`.
- `supported_features[].id`: `CUSTOM_DATA`, `CAPTURE_DELETES`, `DATA_BLOCKING`, `COLUMN_HASHING`,
  `RE_SYNC`, `HISTORY`, `API_CONFIGURABLE`, `PRIORITY_FIRST_SYNC`, `FIVETRAN_DATA_MODELS`,
  `PRIVATE_NETWORKING`, `AUTHORIZATION_VIA_API`, `ROW_FILTERING`. The best signal for which
  Fivetran capabilities are actually in use and therefore need replicating.

**There is no per-connector-type table/column catalog.** Connector metadata covers identity,
features, and config schema only. Table detail exists only per live connection.

### Transformations

`GET /v1/transformations` returns `type` (`DBT_CORE` | `QUICKSTART`), `paused`, `status`,
`output_model_names[]`, `transformation_config.steps[]`, and a `schedule` object.

`schedule.connection_ids[]` is the explicit dependency edge from a transformation back to the
connections feeding it — use it to order the cutover.

`DBT_CORE` projects port to Databricks with modest effort. `QUICKSTART` models are
Fivetran-proprietary and must be rebuilt; flag them as migration scope.

---

## 4. MAR and cost: not available over REST

**Verdict: no REST endpoint exposes MAR, usage, cost, or credits** [confirmed]. Nine candidate
paths (`/v1/usage`, `/v1/billing`, `/v1/account/usage`, `/v1/consumption`, `/v1/credits`,
`/v1/mar`, `/v1/account/mar`, `/v1/metadata/mar`, `/v1/metadata/usage`) all return `404`,
against a control where `/v1/account/info` returns `401`. The API resource index lists no
usage, billing, or MAR resource.

So volume and cost must come from the **Fivetran Platform Connector** tables inside the
customer's own destination warehouse. This is a hard dependency the skill must plan for:
full topology from REST, but volume and cost only with warehouse access — and cost is usually
the half the customer cares about.

### Schema name

**`fivetran_metadata`** for auto-created Platform connections. `fivetran_log` is the historical
name, still present in older accounts and most community tooling. **Probe for both.** Default
sync frequency is 24h, so figures can be a day stale.

### Tables that matter

`INCREMENTAL_MAR` — daily MAR per connection, schema, and table. The core sizing table.

| Column | Notes |
|---|---|
| `destination_id`, `connection_name`, `schema_name`, `table_name` | Composite PK |
| `measured_date` | DATE — **daily grain despite the table name** |
| `sync_type` | `HISTORICAL` \| `INCREMENTAL` \| `UNKNOWN` |
| `free_type` | Billing category — also part of the PK |
| `incremental_rows` | BIGINT, count of new distinct primary keys synced |

> **`free_type` is part of the primary key.** The same table on the same day appears as
> multiple rows, one per billing category. Summing without `WHERE free_type = 'PAID'`
> conflates billable and non-billable volume and inflates the estimate.

Full `free_type` enum: `PAID`, `INITIAL_SYNC`, `CONNECTOR_TRIAL`, `ACCOUNT_TRIAL`,
`TABLE_TRIAL`, `NON_CBP_ACCOUNT`, `FREE_RESYNC`, `PROMOTION`, `SYSTEM`, `PRIVATE_PREVIEW`,
`FREE_DBT`, `FREE_PLAN`, `CAP_PROMOTION`, `LDP_LIMITED_RESYNC`, `REFUND`,
`TRANSFORMATION_TRIAL`, `ACTIVATION_TRIAL`, `FREE_ACTIVATION`.

Other tables:

| Table | Contents |
|---|---|
| `USAGE_COST` | Monthly dollars per destination: `measured_month` (`YYYY-MM`), `group_id`, `amount` |
| `CREDITS_USED` | Monthly credits, credit-billed plans only |
| `TRANSFORMATION_RUNS` | `model_runs` per job per day, with `free_type` and `project_type` |
| `SOURCE_TABLE` | Metadata for **every table ever synced, including disabled ones** — historical scope the REST API cannot give you |
| `TABLE_LINEAGE`, `COLUMN_LINEAGE` | Column-level lineage; the cheapest way to size downstream blast radius before cutover |
| `CONNECTION`, `DESTINATION`, `CONNECTOR_TYPE` | Inventory cross-check |

An account is billed on one basis or the other, so exactly one of `USAGE_COST` and
`CREDITS_USED` will be populated. Check both. `connector_name`/`connector_id` are deprecated in
favour of `connection_name`/`connection_id`. **`ACTIVE_VOLUME` does not exist** — the nearest
thing is `fivetran_platform__usage_history`, a derived model from Fivetran's dbt package.

### Reference query

```sql
SELECT connection_name, schema_name, table_name,
       date_trunc('month', measured_date) AS measured_month,
       SUM(incremental_rows) AS mar
FROM fivetran_metadata.incremental_mar
WHERE free_type = 'PAID'
  AND measured_date >= date_trunc('month', current_date - INTERVAL '6 months')
GROUP BY 1, 2, 3, 4
ORDER BY measured_month, mar DESC;
```

### If the Platform Connector is unavailable

All remaining options are manual: the Billing & Usage page in the Fivetran dashboard, the
Pricing Estimator, or the customer's contract/invoice. None is automatable. Detect whether the
customer already runs Fivetran's [Platform Connector dbt package](https://fivetran.com/docs/transformations/data-models/fivetran-platform-connector-data-model)
(`fivetran_platform__usage_history`, `__mar_table_history`) — if so, the analysis is much easier.

---

## 5. Pricing model

Marked **[public]** from Fivetran's own pages, **[derived]** computed from their published
figures, **[third-party]** where only non-Fivetran sources support it.

### How MAR is counted [public]

MAR is the count of **distinct primary keys** synced per month, counted separately per account,
destination, connection, and table. Fivetran generates a synthetic hashed key when no primary
key exists.

The defining property: **a row counts once per month regardless of how many times it changes.**
Update the same row 30 times and it is 1 MAR. This makes MAR largely insensitive to sync
frequency — which matters when comparing against Lakeflow Connect, where more frequent syncs
*do* cost more compute.

Counted: inserts, updates, and deletes (deletes began counting 2026-01-01). Not counted:
unchanged rows retrieved during re-syncs, and initial syncs.

### Plans [public]

Current lineup is **Free, Standard, Enterprise, Business Critical**. Starter and Private
Deployment were discontinued 2025-03-01 [third-party for the date], though legacy accounts may
remain on them.

- **Free** — capped at 500K connection MAR/month. No REST API.
- **Standard** — 15-min syncs, REST API, dbt Core, RBAC, SSH tunnels.
- **Enterprise** — + 1-min syncs, enterprise DB connectors (Oracle, HVA), SCIM, hybrid deployment.
- **Business Critical** — + CMEK, PCI DSS L1, private networking.

### Rates

**Standard ≈ $500 per million MAR at low volume [derived].** Fivetran publishes no rate card,
but its pricing-page examples back one out consistently:

| Example connector | Median MAR | Listed price | Implied $/M MAR |
|---|---|---|---|
| Facebook Ads | 34,479 | $17.23 | $499.7 |
| Google Analytics 4 | 21,993 | $10.99 | $499.7 |
| Marketo | 847,574 | $423.78 | $500.0 |
| Google Ads | 88,240 | $44.12 | $500.0 |

The curve is effectively flat below 1M MAR; the decline begins above it. Enterprise ≈ $667/M
and Business Critical ≈ $1,067/M are **[third-party, unverified]** — treat as rough.

Pricing moved to **per-connection cost curves** in 2026: each connection has its own declining
rate. Accounts with many low-volume connections saw increases; consolidated high-volume ones
saw decreases. There is also a **$5 base charge** per standard connection between 1 and 1M MAR.

> **[unverified] The $5 base charge contradicts Fivetran's own examples.** The Facebook Ads
> example is exactly 34,479 × $500/M with no $5 added — so either the $5 is a floor rather than
> an addend, or the examples predate the 2026 change. This materially affects accounts with
> many small connections. Confirm against the customer's actual invoice before quoting savings.

**Transformations** [public] — the one published rate table. Only successful runs count.

| Monthly model runs | Rate per run |
|---|---|
| 0 – 5,000 | $0.00 |
| 5,001 – 30,000 | $0.01 |
| 30,001 – 100,000 | $0.007 |
| 100,000+ | $0.002 |

**Annual discounts** [public] run 5% to ~22.6% on Standard by list price, though Fivetran's
pricing blog cites "5% up to 36%" — the discrepancy is unresolved.

**ELAs** [public] are fixed-price with unlimited consumption. **An ELA customer's MAR will not
correlate with their spend**, which changes how you frame the comparison entirely. Detect this
early by reconciling `incremental_mar` against `usage_cost`.

---

## 6. Rate limits and errors

| Limit | Trial | Paid plans |
|---|---|---|
| **Source interaction / minute** | 25 | 500 |
| **Source interaction / hour** | 250 | 5,000 |
| Setup tests / hour | 50 | 2,500 |
| **All requests / hour** | 500 | 20,000 |

"Source interaction" covers the per-table columns endpoint, schema reload, and setup tests —
and setup tests draw from the same pool. This is the binding constraint on a large crawl.

Headers `X-Rate-Limit` and `X-Rate-Limit-Remaining` appear on every response; throttle
pre-emptively rather than waiting for a `429`.

> **On `429`, honour `Retry-After` — it is in seconds and Fivetran's documented example is
> `3600`.** Exponential backoff will burn the hour and get nowhere.

Errors return `code` and `message` only, no `data`. **Branch on `code`, never `message`.**
Note `NotFound_Connector` retains the old naming.

| HTTP | Meaning |
|---|---|
| 401 | Missing, malformed, or invalid credentials |
| 403 | Authenticated but not permitted — likely a scoped key |
| 404 | Resource does not exist |
| 406 | Wrong `Accept` value for that endpoint |
| 429 | Rate limited; read `Retry-After` |

---

## 7. Discovery sequence

Ordered to fail fast on auth and defer the rate-limited calls.

1. `GET /v1/account/info` — validate credentials, capture `account_id`, detect scoped vs system key.
2. `GET /v1/groups` — paginate fully. Each `id` is also a destination id.
3. `GET /v1/destinations/{id}` per group — `service`, `region`, `networking_method`.
4. `GET /v1/connections` — paginate with `limit=1000`. The bulk of the inventory in one pass.
5. `GET /v1/metadata/connector-types` — paginate once, cache, join on `service`.
6. `GET /v1/transformations` + `/v1/transformation-projects` — capture `schedule.connection_ids[]`.
7. `GET /v1/connections/{id}/schemas` per connection — selection tree and `sync_mode`.
8. Per-table columns, **only** where `supports_columns_config` is true and column fidelity
   matters. Watch `X-Rate-Limit-Remaining`; this is where `429` happens.
9. If warehouse access exists, query `fivetran_metadata` (falling back to `fivetran_log`).

Steps 1–7 are cheap and safe. Step 8 is rate-limited. Step 9 needs credentials the Fivetran
API cannot provide — plan for that dependency up front.

---

## 8. Known gaps

- **No OpenAPI spec is reachable.** `fivetran.com/docs/rest-api/openapi-definition` and the
  usual `openapi.json` paths all 404. Every schema here comes from the rendered HTML reference.
  Field *names* and *enums* are reliable; example *values* are illustrative.
- **No response was captured from a live account.** Validate against one real customer before
  trusting generated output.
- **[unverified]** Whether `GET .../schemas` paginates on very large schemas. No cursor
  parameter is documented and the response is a nested map, so it should return everything —
  but a database connection with tens of thousands of tables is exactly where an undocumented
  cap would bite.
- **[unverified]** Behaviour of `GET .../schemas` when `setup_state` is `broken` or
  `incomplete`. The columns endpoint explicitly requires `Connected`; the schemas endpoint's
  requirement is unstated. Handle failure per connection rather than aborting the crawl.
- **[unverified]** Latency of the per-table columns endpoint, which queries the source
  synchronously. On a slow source with thousands of tables a sequential crawl may be
  impractical regardless of rate limits.

## Sources

- [REST API getting started](https://fivetran.com/docs/rest-api/getting-started)
- [Connections reference](https://fivetran.com/docs/rest-api/api-reference/connections)
- [Connection schema config](https://fivetran.com/docs/rest-api/api-reference/connection-schema)
- [Connector metadata](https://fivetran.com/docs/rest-api/api-reference/connector-metadata)
- [Rate limiting](https://fivetran.com/docs/rest-api/getting-started/rate-limiting)
- [Platform Connector table reference](https://fivetran.com/docs/logs/fivetran-platform/table-reference)
- [Usage-based pricing](https://fivetran.com/docs/core-concepts/usage-based-pricing)
