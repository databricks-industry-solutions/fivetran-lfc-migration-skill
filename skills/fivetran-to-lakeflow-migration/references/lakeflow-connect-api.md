# Databricks Lakeflow Connect: UC Connections + Ingestion Pipelines API Reference

Research date: 2026-08-24. Databricks CLI v1.1.0.

## How to read this document

Every claim is tagged with its evidence class. **Do not generate code from anything tagged `INFERRED` without verifying first.**

| Tag | Meaning |
|---|---|
| `[DOCS]` | Stated explicitly on docs.databricks.com or the REST API reference |
| `[SCHEMA]` | Taken from the Databricks Asset Bundle JSON schema emitted by `databricks bundle schema` (CLI v1.1.0). This schema is generated from `databricks-sdk-go`, so field names and enum values are authoritative for the API surface |
| `[OBSERVED]` | Read from a live Unity Catalog metastore (1,662 real connections) and live pipeline specs via `databricks connections list -o json` / `databricks pipelines get` |
| `[INFERRED]` | Not directly confirmed. Flagged individually |

Primary sources:

- <https://docs.databricks.com/api/workspace/connections/create>
- <https://docs.databricks.com/api/workspace/pipelines/create>
- <https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-connection>
- <https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/salesforce-connection>
- <https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/salesforce-pipeline>
- <https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/servicenow-connection>

---

# Part 1 — Unity Catalog connections

## 1.1 Correction up front: SQL `CREATE CONNECTION` cannot create Lakeflow Connect connections

The request asked for `CREATE CONNECTION` syntax for Salesforce, SQL Server, PostgreSQL, ServiceNow, and Workday. Only two of those five are even expressible in SQL, and the docs explicitly tell you not to use SQL for ingestion connections.

`[DOCS]` From the `CREATE CONNECTION` reference:

> `CREATE CONNECTION` is for Lakehouse Federation (federated query) connections. To create connections for managed ingestion pipelines in Lakeflow Connect, use the Connections REST API or the Databricks CLI (`databricks connections create`).

`[DOCS]` The SQL grammar accepts only these eight `connection_type` values:

`DATABRICKS`, `HTTP`, `MYSQL`, `POSTGRESQL`, `REDSHIFT`, `SNOWFLAKE`, `SQLDW`, `SQLSERVER`

So: **there is no `CREATE CONNECTION ... TYPE SALESFORCE`, `TYPE SERVICENOW`, or `TYPE WORKDAY_RAAS`.** Those must go through `POST /api/2.1/unity-catalog/connections`.

### Syntax

`[DOCS]`

```sql
CREATE CONNECTION [IF NOT EXISTS] connection_name
  TYPE connection_type
  OPTIONS ( option value [, ...] )
  [ COMMENT comment ]
```

`SERVER` is accepted as a synonym for `CONNECTION`.

### Required options per SQL-supported type

`[DOCS]`

| Connection type | Required options |
|---|---|
| `DATABRICKS` | `host`, `httpPath`, `personalAccessToken` |
| `HTTP` | `host`, `bearer_token` |
| `MYSQL` | `host`, `port`, `user`, `password` |
| `POSTGRESQL` | `host`, `port`, `user`, `password` |
| `REDSHIFT` | `host`, `port`, `user`, `password` |
| `SNOWFLAKE` | `host`, `port`, `sfWarehouse`, `user`, `password` |
| `SQLDW` | `host`, `port`, `user`, `password` |
| `SQLSERVER` | `host`, `port`, `user`, `password` |

### Working SQL examples (federation only)

`[DOCS]` Always wrap credentials in `secret()` rather than inlining literals.

```sql
-- PostgreSQL (federation)
CREATE CONNECTION postgresql_connection
  TYPE POSTGRESQL
  OPTIONS (
    host     'pg.example.com',
    port     '5432',
    user     secret('my_scope', 'pgUser'),
    password secret('my_scope', 'pgPassword')
  );

-- SQL Server (federation)
CREATE CONNECTION sqlserver_connection
  TYPE SQLSERVER
  OPTIONS (
    host     'mysqlsrv.database.windows.net',
    port     '1433',
    user     secret('my_scope', 'sqlUser'),
    password secret('my_scope', 'sqlPassword')
  );
```

```sql
-- NOT VALID. There is no SQL path for these.
-- CREATE CONNECTION sfdc TYPE SALESFORCE  OPTIONS (...);
-- CREATE CONNECTION snow TYPE SERVICENOW  OPTIONS (...);
-- CREATE CONNECTION wd   TYPE WORKDAY_RAAS OPTIONS (...);
```

`[INFERRED]` The docs' note that SQL is "for Lakehouse Federation" implies a SQL-created `SQLSERVER`/`POSTGRESQL` connection may not be accepted as a Lakeflow Connect ingestion `connection_name`. I did not verify this either way. **Create ingestion connections via the REST API/CLI regardless of type** — it is the documented path and avoids the question entirely.

## 1.2 REST API: `POST /api/2.1/unity-catalog/connections`

### Request body

`[DOCS]` Writable fields:

| Field | Type | Notes |
|---|---|---|
| `name` | string | Connection name, unique within the metastore |
| `connection_type` | string enum | See enum below |
| `options` | map<string,string> | Type-specific. See §1.3 |
| `properties` | map<string,string> | Free-form user metadata |
| `comment` | string | Free-form description |
| `read_only` | boolean | "If the connection is read only" |
| `owner` | string | Username of owner |
| `parent` | string | **Beta.** Schema-level (L2) connections, format `"schemas/{catalog}.{schema}"`. Omit for metastore-level connections |
| `environment_settings` | object | **Public Preview.** `{ "java_dependencies": [...], "environment_version": "..." }` |

Response is a `ConnectionInfo` object. Server-populated read-only fields: `connection_id`, `metastore_id`, `full_name`, `url`, `credential_type`, `securable_type`, `provisioning_info.state`, `created_at`, `created_by`, `updated_at`, `updated_by`.

`[DOCS]` The API reference lists `credential_type` under the request body, but it is derived from the shape of `options` in practice. `[OBSERVED]` Every connection in the sampled metastore had a `credential_type` matching its auth shape, and none had it set to `UNKNOWN_CREDENTIAL_TYPE`. `[INFERRED]` You most likely do not need to send `credential_type`; whether it is *accepted* as an explicit disambiguator when a type supports multiple auth modes is **unverified**.

### `connection_type` enum

`[DOCS]` Complete list from the API reference:

```
UNKNOWN_CONNECTION_TYPE, MYSQL, POSTGRESQL, SNOWFLAKE, REDSHIFT, SQLDW,
SQLSERVER, DATABRICKS, SALESFORCE, BIGQUERY, NETSUITE, WORKDAY_RAAS,
HIVE_METASTORE, GA4_RAW_DATA, SERVICENOW, SALESFORCE_DATA_CLOUD, GLUE,
ORACLE, TERADATA, HTTP, POWER_BI, DYNAMICS365, CONFLUENCE, JDBC,
META_MARKETING, HUBSPOT, ZENDESK, GITHUB, OUTLOOK, SMARTSHEET
```

`[OBSERVED]` The live metastore additionally contained connections with these `connection_type` values, which are **not** in the published enum — they are newer or preview types the doc page has not caught up with:

```
MANAGED_POSTGRESQL, GENERIC_LAKEFLOW_CONNECT, GOOGLE_DRIVE, SHAREPOINT,
SFTP, COMMUNITY, JIRA, KAFKA, GOOGLE_ADS, AWS_SECRETS_MANAGER,
SALESFORCE_DATA_CLOUD_FILE_SHARING, WORKDAY_HCM, SLACK, TIKTOK_ADS
```

Note `WORKDAY_HCM` and `WORKDAY_RAAS` are **distinct types**. `WORKDAY_RAAS` is Workday Reports-as-a-Service (report URL ingestion); `WORKDAY_HCM` is the HCM object connector.

### `credential_type` enum

`[DOCS]`

```
UNKNOWN_CREDENTIAL_TYPE, USERNAME_PASSWORD, OAUTH_U2M, OAUTH_M2M,
OAUTH_REFRESH_TOKEN, OAUTH_ACCESS_TOKEN, OAUTH_RESOURCE_OWNER_PASSWORD,
SERVICE_CREDENTIAL, BEARER_TOKEN, OIDC_TOKEN, PEM_PRIVATE_KEY,
OAUTH_U2M_MAPPING, ANY_STATIC_CREDENTIAL, OAUTH_MTLS, SSWS_TOKEN,
EDGEGRID_AKAMAI
```

`[OBSERVED]` Two more appeared in the live metastore that are not in the published enum: `OAUTH_DCR` (dynamic client registration, on `HTTP`/MCP connections).

### `read_only`

`[OBSERVED]` All 1,662 connections in the sampled metastore had `read_only: true`. `[INFERRED]` Lakeflow Connect ingestion connections appear to be created read-only, either by default or because the UI always sets it. **Set `"read_only": true` explicitly** for ingestion connections; it matches every real example and ingestion never writes to the source.

## 1.3 `options` map keys per connection type

`[OBSERVED]` The following key sets are the union of `options` keys across all real connections of each type in the live metastore, grouped by the observed `credential_type`.

### Critical caveat on secret keys

The `connections list` / `connections get` API **redacts credential values, and omits their keys entirely** from the `options` map it returns. So for every type below, the observed keys are the *non-secret* keys only. The names of the secret keys you must supply on create (`password`, `user`, `client_secret`, `refresh_token`, `pem_private_key`, `client_certificate`, …) **could not be observed** and are `[INFERRED]`.

This is the single highest-risk area for generated code. Verify secret key names by either:

1. Creating one connection of each type through the Databricks UI, then diffing what you know you typed against `GET /api/2.1/unity-catalog/connections/{name}`; or
2. Posting a deliberately incomplete `options` map and reading the `INVALID_PARAMETER_VALUE` error, which names the missing required keys.

### Database sources (fully scriptable)

| Type | Observed non-secret option keys | Observed `credential_type` | n |
|---|---|---|---|
| `SQLSERVER` | `host`, `port`, `trustServerCertificate`, `applicationIntent` | `USERNAME_PASSWORD` (95) | 96 |
| `SQLSERVER` (Entra) | `host`, `port`, `client_id`, `oauth_scope`, `token_endpoint`, `trustServerCertificate`, `access_token_expiration` | `OAUTH_M2M` (1) | |
| `POSTGRESQL` | `host`, `port`, `trustServerCertificate`, `userProvidedServerCertificate` | `USERNAME_PASSWORD` | 141 |
| `MYSQL` | `host`, `port`, `trustServerCertificate`, `userProvidedServerCertificate` | `USERNAME_PASSWORD` | 55 |
| `ORACLE` | `host`, `port`, `service_name`, `encryption_protocol`, `userProvidedServerCertificate` | `USERNAME_PASSWORD` | 70 |
| `TERADATA` | `host`, `port`, `ssl_mode` | `USERNAME_PASSWORD` | 7 |
| `REDSHIFT` | `host`, `port`, `disableSslHostnameVerification` | `USERNAME_PASSWORD` | 33 |
| `SQLDW` | `host`, `port`, `trustServerCertificate` | `USERNAME_PASSWORD` | 1 |
| `SNOWFLAKE` | `host`, `port`, `sfWarehouse`, `sfRole`, `use_proxy`, `proxy_host`, `proxy_port` | `USERNAME_PASSWORD` (53), `PEM_PRIVATE_KEY` (6), `OAUTH_U2M` (6), `OAUTH_ACCESS_TOKEN` (4), `OAUTH_M2M` (1) | 70 |
| `BIGQUERY` | `projectId` | `USERNAME_PASSWORD` | 35 |
| `MANAGED_POSTGRESQL` | *(none — Lakebase-internal)* | `USERNAME_PASSWORD` | 512 |

Note the camelCase inconsistency is real: `trustServerCertificate`, `userProvidedServerCertificate`, `applicationIntent`, `disableSslHostnameVerification`, `sfWarehouse`, `projectId`, `httpPath` are camelCase, while `service_name`, `encryption_protocol`, `ssl_mode`, `client_id`, `oauth_scope`, `token_endpoint`, `instance_url` are snake_case. Do not normalize these.

### SaaS sources

| Type | Observed non-secret option keys | Observed `credential_type` | n |
|---|---|---|---|
| `SALESFORCE` | `instance_url`, `is_sandbox`, `client_id`, `oauth_scope`, `oauth_redirect_uri`, `access_token_expiration`, `refresh_token_expiration` | `OAUTH_U2M` (68), `USERNAME_PASSWORD` (4) | 72 |
| `SERVICENOW` | `instance_url`, `client_id`, `oauth_scope`, `oauth_redirect_uri`, `access_token_expiration`, `refresh_token_expiration` | `OAUTH_U2M` (7), `OAUTH_RESOURCE_OWNER_PASSWORD` (1) | 8 |
| `WORKDAY_HCM` | `instance_url`, `tenant_name` | `USERNAME_PASSWORD` (1) | 1 |
| `WORKDAY_RAAS` | *(none observed)* | `USERNAME_PASSWORD` (1) | 1 |
| `NETSUITE` | `host`, `port`, `account_id`, `role_id`, `data_source` | `USERNAME_PASSWORD` | 1 |
| `SHAREPOINT` | `domain`, `tenant_id`, `client_id`, `oauth_scope`, `oauth_redirect_uri`, `*_expiration` | `OAUTH_U2M` (10), `OAUTH_M2M` (6), `OAUTH_REFRESH_TOKEN` (2) | 18 |
| `DYNAMICS365` | `client_id`, `tenant_id`, `oauth_scope`, `azure_storage_account_name`, `azure_container_name` | `OAUTH_M2M` (1) | 1 |
| `OUTLOOK` | `client_id`, `tenant_id` | `OAUTH_M2M` (1) | 1 |
| `GA4_RAW_DATA` | `client_id`, `oauth_scope`, `oauth_redirect_uri`, `*_expiration` | `OAUTH_U2M` (15), `USERNAME_PASSWORD` (3) | 18 |
| `GOOGLE_DRIVE` | `client_id`, `oauth_scope`, `oauth_redirect_uri`, `*_expiration` | `OAUTH_U2M` | 22 |
| `GOOGLE_ADS` | `client_id`, `oauth_redirect_uri`, `*_expiration` | `OAUTH_U2M` | 3 |
| `JIRA` | `host`, `port`, `client_id`, `jira_deployment_path`, `on_premise`, `oauth_redirect_uri` | `OAUTH_U2M` | 6 |
| `CONFLUENCE` | `domain`, `client_id`, `oauth_scope`, `oauth_redirect_uri` | `OAUTH_U2M` | 5 |
| `ZENDESK` | `subdomain`, `client_id`, `oauth_redirect_uri` | `OAUTH_U2M` | 1 |
| `HUBSPOT` | `client_id`, `oauth_redirect_uri` | `OAUTH_U2M` | 1 |
| `GITHUB` | `host`, `client_id`, `oauth_redirect_uri` | `OAUTH_U2M` | 4 |
| `SMARTSHEET` | `client_id`, `region`, `oauth_scope`, `oauth_redirect_uri` | `OAUTH_U2M` | 1 |
| `TIKTOK_ADS` | `app_id` | `OAUTH_U2M` | 1 |
| `SALESFORCE_DATA_CLOUD` | `instance_url`, `is_sandbox`, `client_id`, `oauth_scope`, `oauth_redirect_uri` | `OAUTH_U2M` | 2 |
| `KAFKA` | `bootstrap_servers`, `sasl_mechanism` | `SERVICE_CREDENTIAL` (3), `USERNAME_PASSWORD` (1) | 4 |
| `SFTP` | `host`, `port`, `key_fingerprint`, `enforce_host_key_fingerprint` | `USERNAME_PASSWORD` (11), `PEM_PRIVATE_KEY` (5) | 16 |
| `GLUE` | `aws_account_id`, `aws_region` | `SERVICE_CREDENTIAL` | 33 |
| `HTTP` | `host`, `port`, `base_path`, `auth_scheme`, `client_id`, `oauth_scope`, `authorization_endpoint`, `token_endpoint`, `oauth_provider`, `oauth_credential_exchange_method`, `oauth_redirect_uri`, `is_mcp_connection`, `mcp_resource` | `BEARER_TOKEN` (135), `OAUTH_U2M_MAPPING` (98), `OAUTH_M2M` (24), `OAUTH_DCR` (4), `OAUTH_U2M` (1) | 262 |
| `JDBC` | `externalOptionsAllowList` | `ANY_STATIC_CREDENTIAL` | 27 |
| `GENERIC_LAKEFLOW_CONNECT` | `sourceName` | `ANY_STATIC_CREDENTIAL` | 65 |

The reliable tell for U2M in observed data: **the presence of `oauth_redirect_uri` plus `refresh_token_expiration`**. Those two only appear on connections whose `credential_type` is `OAUTH_U2M` / `OAUTH_U2M_MAPPING`, and they are artifacts of a completed browser redirect flow.

## 1.4 The critical question: which connectors can be created 100% programmatically?

This is the definitive answer, built from `[DOCS]` on auth modes and `[OBSERVED]` credential types actually present in a real metastore.

### Fully scriptable — no browser step, ever

These use static credentials. `POST /api/2.1/unity-catalog/connections` with `options` containing username/password (or key/token) completes the connection in one call.

| Connector | Mechanism | Evidence |
|---|---|---|
| `SQLSERVER` | user + password, or Entra ID `OAUTH_M2M` (client credentials) | `[OBSERVED]` 95× `USERNAME_PASSWORD`, 1× `OAUTH_M2M` |
| `POSTGRESQL` | user + password | `[OBSERVED]` 141× `USERNAME_PASSWORD` |
| `MYSQL` | user + password | `[OBSERVED]` 55× |
| `ORACLE` | user + password | `[OBSERVED]` 70× |
| `TERADATA` | user + password | `[OBSERVED]` 7× |
| `REDSHIFT`, `SQLDW` | user + password | `[OBSERVED]` |
| `SNOWFLAKE` | user + password, or PEM private key, or OAuth M2M | `[OBSERVED]` 53 / 6 / 1 |
| `BIGQUERY` | service-account JSON | `[OBSERVED]` 35× `USERNAME_PASSWORD` |
| `WORKDAY_HCM` | static credential | `[OBSERVED]` 1× `USERNAME_PASSWORD` |
| `WORKDAY_RAAS` | static credential | `[OBSERVED]` 1× `USERNAME_PASSWORD` |
| `NETSUITE` | static credential | `[OBSERVED]` 1× `USERNAME_PASSWORD` |
| `SERVICENOW` | **ROPC**: `client_id` + `client_secret` + username + password | `[DOCS]` connector page lists "OAuth Resource Owner Password Credentials (ROPC)" as a supported method; `[OBSERVED]` 1× `OAUTH_RESOURCE_OWNER_PASSWORD` |
| `SALESFORCE` | **mTLS + OAuth client credentials** | `[DOCS]` connector page: "Choose this for API-only users"; `[OBSERVED]` 4× `USERNAME_PASSWORD` on `SALESFORCE` |
| `DYNAMICS365`, `OUTLOOK`, `SHAREPOINT` | Entra ID `OAUTH_M2M` (client credentials) | `[OBSERVED]` |
| `KAFKA`, `GLUE`, `AWS_SECRETS_MANAGER` | `SERVICE_CREDENTIAL` (pre-existing UC service credential, referenced by name) | `[OBSERVED]` |
| `HTTP` | `BEARER_TOKEN` | `[DOCS]` + `[OBSERVED]` 135× |
| `SFTP` | password or PEM private key | `[OBSERVED]` |

### Requires a one-time interactive browser consent (U2M) — cannot be fully scripted

For these, the *default and recommended* auth mode is OAuth user-to-machine. The `refresh_token` is minted by a redirect back to `.../api/2.0/ingestion/oauth/redirect` (or `/login/oauth/<source>.html`), and a human must click through the IdP consent screen.

`[OBSERVED]` U2M-dominant connectors, with no scriptable alternative found:

`GOOGLE_DRIVE` (22/22 U2M), `GOOGLE_ADS` (3/3), `JIRA` (6/6), `CONFLUENCE` (5/5), `ZENDESK` (1/1), `HUBSPOT` (1/1), `GITHUB` (4/4), `SMARTSHEET` (1/1), `TIKTOK_ADS` (1/1), `POWER_BI` (13/14), `SALESFORCE_DATA_CLOUD` (2/2), `GA4_RAW_DATA` (15/18)

### The nuanced ones — read carefully

**Salesforce.** `[DOCS]` The connector supports exactly two auth types:

> - **OAuth (user-to-machine):** You sign in to Salesforce interactively to grant Databricks access.
> - **Mutual TLS (mTLS):** Databricks and Salesforce exchange and verify client certificates to establish two-way trust, combined with the OAuth client credentials flow. **Choose this for API-only users** or when your organization requires mTLS.

So the answer for Salesforce is **yes, fully scriptable — but only via mTLS, and only after out-of-band setup in Salesforce.** `[DOCS]` The mTLS path is **Beta** and gated behind a workspace preview flag ("Workspace admins can control access to this feature from the Previews page"). The five values Databricks needs are: Client ID (connected-app consumer key), Client Secret (consumer secret), Instance URL, Client Private Key (PEM), Client Certificate (PEM chain, ordered leaf → intermediate → root). Getting a CA-signed cert uploaded into Salesforce and configuring the connected app for the client-credentials flow is manual, one-time, per-org work. `[INFERRED]` The `options` key names for those five fields are **not documented and could not be observed** (only the UI labels are published). Verify before generating.

If mTLS Beta is not enabled, **Salesforce requires a browser step.**

**ServiceNow.** `[DOCS]` "U2M OAuth (recommended)" and "OAuth Resource Owner Password Credentials (ROPC)". ROPC needs `instance_url`, `client_id`, `client_secret`, username, password, and `oauth_scope` (default `useraccount`). `[OBSERVED]` One real connection with `credential_type: OAUTH_RESOURCE_OWNER_PASSWORD` and `options: {client_id, instance_url}` — the secret/user/password keys were redacted. So: **yes, fully scriptable via ROPC**, and this is a real, working configuration, not a theoretical one.

**Workday.** `[OBSERVED]` Both `WORKDAY_HCM` and `WORKDAY_RAAS` connections in the live metastore are `USERNAME_PASSWORD`, with no `oauth_redirect_uri` anywhere. **Fully scriptable.** `WORKDAY_HCM` needs `instance_url` and `tenant_name` plus credentials. `[INFERRED]` `WORKDAY_RAAS` showed `options: null`, meaning everything it needs is in redacted credential fields — its option key names are unknown. Note that the earlier assumption of `refreshToken` camelCase for Workday is **not supported by any evidence**; every observed Workday key is snake_case.

**SQL Server.** Fully scriptable via `USERNAME_PASSWORD`. For Azure SQL with Entra ID, also scriptable via `OAUTH_M2M`; `[OBSERVED]` a working example uses `client_id`, `oauth_scope: "https://database.windows.net/.default"`, and `token_endpoint: "https://login.microsoftonline.com/<tenant>/oauth2/v2.0/token"`.

### Bottom line for an automated migration tool

| Source | 100% programmatic? |
|---|---|
| SQL Server, PostgreSQL, MySQL, Oracle, Teradata, Redshift, Synapse, Snowflake, BigQuery | **Yes**, unconditionally |
| Workday (HCM and RaaS), NetSuite | **Yes** |
| ServiceNow | **Yes**, via ROPC |
| Salesforce | **Yes, but** requires mTLS Beta enabled + manual cert/connected-app setup in Salesforce first. Otherwise **no** |
| Dynamics 365, Outlook, SharePoint | **Yes**, via Entra client credentials |
| Google Drive, Google Ads, GA4, Jira, Confluence, Zendesk, HubSpot, GitHub, Smartsheet, TikTok Ads, Power BI, Salesforce Data Cloud | **No.** Browser consent required |

For the "no" list, the only viable automation pattern is: have a human create the connection once in the UI, then reference it by `connection_name` from fully automated pipeline creation. Pipeline creation itself is *always* 100% scriptable.

## 1.5 CLI and curl

`[DOCS]` `databricks connections create` flags (CLI v1.1.0, verified locally):

```
--json JSON        either inline JSON string or @path/to/file.json with request body
--comment string   User-provided free-form text description
--read-only        If the connection is read only
```

### SQL Server (fully confirmed key names)

```bash
databricks connections create --json '{
  "name": "sqlserver_prod",
  "connection_type": "SQLSERVER",
  "comment": "Lakeflow Connect CDC source",
  "read_only": true,
  "options": {
    "host": "mysqlsrv.database.windows.net",
    "port": "1433",
    "trustServerCertificate": "false",
    "user": "svc_databricks",
    "password": "REDACTED"
  }
}'
```

`[OBSERVED]` `host`, `port`, `trustServerCertificate`. `[INFERRED]` `user`, `password` — matches the `[DOCS]` SQL `OPTIONS` names for `SQLSERVER`, which is strong but not identical evidence.

### PostgreSQL

```bash
databricks connections create --json '{
  "name": "postgres_prod",
  "connection_type": "POSTGRESQL",
  "read_only": true,
  "options": {
    "host": "pg.example.com",
    "port": "5432",
    "user": "svc_databricks",
    "password": "REDACTED"
  }
}'
```

### ServiceNow via ROPC

```bash
databricks connections create --json '{
  "name": "servicenow_prod",
  "connection_type": "SERVICENOW",
  "read_only": true,
  "options": {
    "instance_url": "dev389702.service-now.com",
    "oauth_scope": "useraccount",
    "client_id": "e7aef40dfd7042c6a38cdde6b58a016f",
    "client_secret": "REDACTED",
    "user": "svc_databricks",
    "password": "REDACTED"
  }
}'
```

`[OBSERVED]` `instance_url`, `client_id`, `oauth_scope`. `[INFERRED]` `client_secret`, `user`, `password` — the docs name these fields "Client secret" / username / password but never give API keys. **Verify these three before shipping.**

Note `instance_url` for ServiceNow was stored **without** a scheme (`dev389702.service-now.com`), while Salesforce `instance_url` includes `https://`. `[OBSERVED]` in both cases.

### Workday HCM

```bash
databricks connections create --json '{
  "name": "workday_hcm_prod",
  "connection_type": "WORKDAY_HCM",
  "read_only": true,
  "options": {
    "instance_url": "wd2-impl-services1.workday.com",
    "tenant_name": "my_tenant",
    "user": "svc_databricks",
    "password": "REDACTED"
  }
}'
```

`[OBSERVED]` `instance_url`, `tenant_name`. `[INFERRED]` `user`, `password`.

### Salesforce via mTLS (Beta)

```bash
databricks connections create --json '{
  "name": "salesforce_prod",
  "connection_type": "SALESFORCE",
  "read_only": true,
  "options": {
    "instance_url": "https://mydomain.my.salesforce.com",
    "is_sandbox": "false",
    "client_id": "3MVG9...",
    "client_secret": "REDACTED",
    "client_private_key": "-----BEGIN PRIVATE KEY-----\n...",
    "client_certificate": "-----BEGIN CERTIFICATE-----\n..."
  }
}'
```

`[OBSERVED]` `instance_url`, `is_sandbox`, `client_id`. `[INFERRED — HIGH RISK]` `client_secret`, `client_private_key`, `client_certificate` are **guesses from UI labels**. They could equally be `clientSecret` / `pem_private_key` / `client_certificate_chain`. Do not ship these without verifying.

### curl equivalent

```bash
curl -sS -X POST "https://${DATABRICKS_HOST}/api/2.1/unity-catalog/connections" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d @connection.json
```

## 1.6 List / get / update / delete

`[DOCS]`

```bash
# Get
GET /api/2.1/unity-catalog/connections/{name_arg}
databricks connections get my_conn

# List (max_results=0 is the recommended paginated form; <=1000)
GET /api/2.1/unity-catalog/connections?max_results=0&page_token=...
databricks connections list --limit 0
```

`[DOCS]` Pagination warning worth heeding: *"a page may contain zero results while still providing a `next_page_token`. Clients must continue reading pages until `next_page_token` is absent."* A naive "stop when the page is empty" loop will silently truncate.

```bash
# Update — PATCH, not PUT
PATCH /api/2.1/unity-catalog/connections/{name_arg}
# body may include: new_name, owner, comment, options, properties, read_only
databricks connections update my_conn --json '{"options": {"host": "newhost", "port": "1433"}}'

# Delete
DELETE /api/2.1/unity-catalog/connections/{name_arg}
databricks connections delete my_conn
```

`[INFERRED]` Whether `PATCH` merges the `options` map or replaces it wholesale is **not stated in the docs**. Assume replace, and send the complete `options` map on every update.

---

# Part 2 — Ingestion pipeline creation

`POST /api/2.0/pipelines`

## 2.1 Two corrections to the assumed field list

**`source_type` is output-only.** `[DOCS]` verbatim from the pipelines API reference:

> `source_type` — The type of the foreign source. The source type will be inferred from the source connection or ingestion gateway. **This field is output only and will be ignored if provided.**

`[OBSERVED]` It does come back in `GET`/`list` responses (real specs showed `"source_type": "SALESFORCE"`, `"WORKDAY_HCM"`, `"SQLSERVER"`), which is why it looks like an input field. Sending it is harmless but pointless. **Do not require it, do not compute it, and do not treat its absence in a create request as a bug.**

`[DOCS]` For reference, the enum is: `INGESTION_SOURCE_TYPE_UNSPECIFIED`, `MYSQL`, `POSTGRESQL`, `SQLSERVER`, `SALESFORCE`, `BIGQUERY`, `NETSUITE`, `WORKDAY_RAAS`, `GA4_RAW_DATA`, `SERVICENOW`, `MANAGED_POSTGRESQL`, `ORACLE`, `TERADATA`, `SHAREPOINT`, `DYNAMICS365`, `JIRA`, `CONFLUENCE`, `META_MARKETING`, `ZENDESK`, `RABBITMQ`. `[SCHEMA]` The SDK enum is much longer (~110 values, including `WORKDAY_HCM`, `HUBSPOT`, `GITHUB`, `KAFKA`, `FOREIGN_CATALOG`, and many marketing sources).

**There is no `query_filter` field.** The row-filtering field is **`row_filter`**. `[DOCS]`:

> `row_filter` — (Optional, Immutable) The row filter condition to be applied to the table. It must not contain the `WHERE` keyword, only the actual filter condition. It must be in DBSQL format.

`[SCHEMA]` confirms `row_filter` and confirms no `query_filter` anywhere in the pipelines surface. Generating `query_filter` will fail.

## 2.2 `ingestion_definition` — full nested shape

`[SCHEMA]` + `[DOCS]`. Field-by-field, all fields of `IngestionPipelineDefinition`:

| Field | Type | Status | Notes |
|---|---|---|---|
| `connection_name` | string | GA-ish (Public Preview) | The UC connection. Used for SaaS connectors (Salesforce, Workday, ServiceNow, …) and for query-based DB connectors. Mutually exclusive in practice with `ingestion_gateway_id` |
| `ingestion_gateway_id` | string | Public Preview | The **pipeline ID** of a gateway pipeline. Used for CDC connectors (SQL Server, etc.) |
| `objects` | array of `IngestionConfig` | **Required** | See §2.3 |
| `table_configuration` | `TableSpecificConfig` | optional | Pipeline-wide defaults. See §2.4 |
| `source_type` | enum | **output only** | Ignored on input |
| `ingest_from_uc_foreign_catalog` | boolean | Public Preview | If true, ingest from UC foreign catalogs with no connection or gateway; `source_catalog` in each object is then read as the foreign catalog name |
| `source_configurations` | array of `SourceConfig` | Public Preview | Top-level per-source config. Contains `catalog` (a `SourceCatalogConfig`) and `google_ads_config` (private) |
| `full_refresh_window` | `OperationTimeWindow` | Public Preview | `{ start_hour: int(0-23) [required], days_of_week: [...], time_zone_id: string }`. Constrains when CDC snapshot queries may run |
| `connector_type` | enum | GA for SQL Server integrated CDC | `CDC` / `QUERY_BASED`. `[DOCS]` The [SQL Server integrated pipeline page](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-integrated-pipeline) documents `connector_type: CDC` as the integrated (single-pipeline, no-gateway) architecture. `[SCHEMA]` still marked `doNotSuggest` in CLI v1.1.0, but the field is valid and validates in a bundle. Set `CDC` for integrated ingestion; if omitted for a DB connection it defaults to `QUERY_BASED` |
| `data_staging_options` | object | GA for SQL Server integrated CDC | `{ "catalog_name": string, "schema_name": string }`. `[DOCS]` The catalog/schema where an integrated CDC pipeline creates its staging volume; the pipeline autocreates one in the destination schema if omitted |
| `netsuite_jar_path` | string | **PRIVATE preview** | |

`[DOCS]` On the `connection_name` / `ingestion_gateway_id` relationship:

> If connection name corresponds to database connectors like Oracle, and `connector_type` is not provided then `connector_type` defaults to `QUERY_BASED`. If `connector_type` is passed as `CDC` we use Combined Cdc Managed Ingestion pipeline. Under certain conditions, this can be replaced with `ingestion_gateway_id` to change the connector to Cdc Managed Ingestion Pipeline with Gateway pipeline.

`[INFERRED]` The docs render both `connection_name` and `ingestion_gateway_id` as "Required" in some sub-schemas, which is a rendering artifact of a `oneOf`. `[OBSERVED]` Real pipelines set exactly one: the Salesforce and Workday pipelines had only `connection_name`; the SQL Server pipeline had only `ingestion_gateway_id`. **Set exactly one.**

## 2.3 `objects[]` — the three variants

`[SCHEMA]` `IngestionConfig` is a `oneOf` over exactly three keys: `schema`, `table`, `report`. Each array element must contain exactly one.

### `{"table": {...}}` — `TableSpec`

| Field | Required | Notes |
|---|---|---|
| `source_table` | **yes** | Table name in the source |
| `destination_catalog` | **yes** | |
| `destination_schema` | **yes** | |
| `source_catalog` | no | "Might be optional depending on the type of source." `[OBSERVED]` set to the SQL Server DB name (`DemoDB`); absent for Salesforce/Workday |
| `source_schema` | no | `[OBSERVED]` `"objects"` for Salesforce, `"default"` for Workday HCM, the real schema for SQL Server |
| `destination_table` | no | Defaults to the source table name. Pipeline fails if the table already exists |
| `table_configuration` | no | Overrides both the schema-level and pipeline-level config |
| `connector_options` | no | Source-specific options wrapper |

### `{"schema": {...}}` — `SchemaSpec`

Ingests all current *and future* tables in a source schema.

| Field | Required | Notes |
|---|---|---|
| `source_schema` | **yes** | |
| `destination_catalog` | **yes** | |
| `destination_schema` | **yes** | Source table names are reused; pipeline fails if a name collides |
| `source_catalog` | no | |
| `table_configuration` | no | Applies to all tables in this schema; overrides pipeline-level |
| `connector_options` | no | |

### `{"report": {...}}` — `ReportSpec`

For report-style sources (Workday RaaS).

| Field | Required | Notes |
|---|---|---|
| `source_url` | **yes** | Report URL in the source system |
| `destination_catalog` | **yes** | |
| `destination_schema` | **yes** | |
| `destination_table` | **yes** in `[SCHEMA]`'s `required` list is `[destination_catalog, destination_schema, source_url]`, but the field description says "Required. … The pipeline fails if a table with that name already exists." | `[INFERRED]` Treat as required in practice |
| `table_configuration` | no | |

Note `ReportSpec` has **no** `connector_options` and no `source_catalog`/`source_schema`.

### `connector_options` (`ConnectorOptions`)

`[SCHEMA]` A wrapper with one sub-object per source. Publicly visible: `confluence_options`, `jira_options`, `meta_ads_options`, `zendesk_support_options`. Marked PRIVATE/`doNotSuggest`: `gdrive_options`, `google_ads_options`, `kafka_options`, `outlook_options`, `sharepoint_options`, `smartsheet_options`, `tiktok_ads_options`. Not relevant to Salesforce/SQL Server/Workday.

## 2.4 `table_configuration` — `TableSpecificConfig` in full

`[SCHEMA]` + `[DOCS]`. This same object appears at four levels, each overriding the one above: pipeline → schema object → table object / report object.

| Field | Type | Notes |
|---|---|---|
| `primary_keys` | array of string | "The primary key of the table used to apply changes." Required for SCD merge behavior |
| `scd_type` | enum | `SCD_TYPE_1`, `SCD_TYPE_2`, **`APPEND_ONLY`** |
| `sequence_by` | array of string | Columns defining logical event order, used to handle out-of-order change events. **Note: array, not a scalar string** |
| `include_columns` | array of string | When set, all other *and future* columns are excluded. Mutually exclusive with `exclude_columns` |
| `exclude_columns` | array of string | When set, all other *and future* columns are included. Mutually exclusive with `include_columns` |
| `row_filter` | string | Optional, **immutable**. DBSQL boolean expression, **without** the `WHERE` keyword |
| `query_based_connector_config` | object | See below |
| `auto_full_refresh_policy` | object | `{ "enabled": bool (required), "min_interval_hours": int }`. Defaults to 24 hours when enabled. Disabled if unspecified |
| `salesforce_include_formula_fields` | boolean | **PRIVATE preview.** See warning below |
| `workday_report_parameters` | object | **PRIVATE preview.** `{ "parameters": {"start_date": "{ coalesce(current_offset(), date(\"2025-02-01\")) }", ...} }`; `incremental` and `report_parameters` are deprecated |

`[SCHEMA]` `APPEND_ONLY` is a third `scd_type` value that the original field list omitted. It exists and is valid.

### `salesforce_include_formula_fields` — do not use

`[SCHEMA]` The field exists in the SDK but is tagged `"x-databricks-preview": "PRIVATE"` and `"doNotSuggest": true`, and it does **not** appear in the public pipelines API reference at all.

`[DOCS]` The publicly documented mechanism for Salesforce formula fields is a **top-level `configuration` flag**, not a `table_configuration` field:

```yaml
configuration:
  pipelines.enableSalesforceFormulaFieldsMVComputation: 'true'
```

`[OBSERVED]` A real production Salesforce pipeline used exactly that `configuration` key and did **not** set `salesforce_include_formula_fields`. Use the `configuration` flag.

### `query_based_connector_config`

`[SCHEMA]` Applies only to query-based (non-CDC) connectors.

| Field | Type | Notes |
|---|---|---|
| `cursor_columns` | array of string | Monotonically non-decreasing columns enabling incremental reads. If data is merged (SCD 1 or 2), these implicitly define `sequence_by`; an explicit `sequence_by` overrides |
| `deletion_condition` | string | SQL `WHERE` condition marking a soft-deleted source row, e.g. `"Operation = 'DELETE'"` or `"is_deleted = true"` |
| `hard_deletion_sync_min_interval_in_seconds` | int64 | Lower bound between primary-key snapshots used to detect physically-removed rows. Unset means hard-delete sync is disabled. Mutable without forcing a full snapshot |

## 2.5 The ingestion gateway (two-pipeline pattern)

### Which sources need it

`[DOCS]` The gateway is for **CDC connectors to databases**, named example: SQL Server. The `connector_type = CDC` path. `[DOCS]` also notes the newer "Combined Cdc Managed Ingestion Pipeline" can replace the gateway with a plain `connection_name` "under certain conditions" — those conditions are not enumerated.

`[INFERRED]` The set of source types requiring a gateway is **not authoritatively enumerated anywhere I found**. SQL Server is confirmed by both docs and a live pipeline. Oracle/MySQL/PostgreSQL CDC are commonly gateway-based but I did not confirm this. **Determine per-source empirically; do not hardcode a list.** SaaS connectors (Salesforce, Workday, ServiceNow) definitively do **not** use a gateway — `[OBSERVED]` live pipelines for all three use bare `connection_name`.

### `gateway_definition` — `IngestionGatewayPipelineDefinition`

`[SCHEMA]`

| Field | Required | Notes |
|---|---|---|
| `connection_name` | **yes** | **Immutable.** The UC connection to the source DB |
| `gateway_storage_catalog` | **yes** | **Immutable.** Catalog for the gateway's staging storage |
| `gateway_storage_schema` | **yes** | **Immutable.** Schema for the gateway's staging storage |
| `gateway_storage_name` | no | UC-compatible name for the staging volume. Auto-created by the system under that catalog/schema. `[DOCS]` For combined-CDC pipelines the default is `__databricks_ingestion_gateway_staging_data-$pipelineId` |
| `connection_id` | no | **Deprecated.** Use `connection_name` |
| `connection_parameters` | no | **PRIVATE**, internal |

### Wiring the two together

`[OBSERVED]` from a real production pair in a live workspace:

1. Create the gateway pipeline (`gateway_definition`, no `ingestion_definition`). It returns a `pipeline_id`.
2. Create the ingestion pipeline with `ingestion_definition.ingestion_gateway_id` set to **that `pipeline_id`** — not a separate "gateway id" resource. Confirmed: gateway `pipeline_id` was `16d1e9f1-e645-4e0a-bb2c-83b569cfd710` and the ingestion pipeline's `ingestion_gateway_id` was the identical string.
3. `[OBSERVED]` The gateway ran with `continuous: true, serverless: false`; the paired ingestion pipeline with `continuous: false, serverless: true`. The gateway continuously drains the source's change stream into staging; the ingestion pipeline is triggered and merges staging into destination tables.

## 2.6 Complete request bodies

### (a) Salesforce ingestion pipeline, SCD Type 2, include/exclude columns

`POST /api/2.0/pipelines`

```json
{
  "name": "salesforce_ingest_prod",
  "catalog": "main",
  "schema": "sfdc_pipeline_meta",
  "serverless": true,
  "channel": "CURRENT",
  "development": false,
  "continuous": false,
  "configuration": {
    "pipelines.enableSalesforceFormulaFieldsMVComputation": "true"
  },
  "notifications": [
    {
      "email_recipients": ["data-eng@example.com"],
      "alerts": ["on-update-failure", "on-flow-failure"]
    }
  ],
  "ingestion_definition": {
    "connection_name": "salesforce_prod",
    "table_configuration": {
      "scd_type": "SCD_TYPE_2"
    },
    "objects": [
      {
        "table": {
          "source_schema": "objects",
          "source_table": "Account",
          "destination_catalog": "main",
          "destination_schema": "salesforce_bronze",
          "destination_table": "account",
          "table_configuration": {
            "scd_type": "SCD_TYPE_2",
            "primary_keys": ["Id"],
            "sequence_by": ["SystemModstamp"],
            "exclude_columns": ["BillingStreet", "Phone"]
          }
        }
      },
      {
        "table": {
          "source_schema": "objects",
          "source_table": "Opportunity",
          "destination_catalog": "main",
          "destination_schema": "salesforce_bronze",
          "destination_table": "opportunity",
          "table_configuration": {
            "scd_type": "SCD_TYPE_2",
            "primary_keys": ["Id"],
            "sequence_by": ["SystemModstamp"],
            "include_columns": [
              "Id", "AccountId", "Name", "StageName",
              "Amount", "CloseDate", "IsWon", "SystemModstamp"
            ],
            "row_filter": "IsDeleted = false"
          }
        }
      },
      {
        "schema": {
          "source_schema": "objects",
          "destination_catalog": "main",
          "destination_schema": "salesforce_bronze_all",
          "table_configuration": {
            "scd_type": "SCD_TYPE_1"
          }
        }
      }
    ]
  }
}
```

Notes on this body:

- `source_schema: "objects"` is the Salesforce convention. `[DOCS]` (the official YAML examples use it) and `[OBSERVED]` (real pipeline).
- `include_columns` and `exclude_columns` are used on *different* tables. They are mutually exclusive **within one `table_configuration`**.
- `catalog` + `schema` at the top level are the **pipeline event log** location, not the data destination. `[DOCS]` the official examples annotate them as "Location of the pipeline event log".
- `sequence_by` is an array. `SystemModstamp` is the conventional Salesforce ordering column; `[INFERRED]` — verify against your org's objects.

### (b) SQL Server: gateway pipeline + paired ingestion pipeline

**Step 1 — gateway.** `POST /api/2.0/pipelines`

```json
{
  "name": "sqlserver_gateway_prod",
  "catalog": "main",
  "schema": "_lfc_staging",
  "serverless": false,
  "channel": "CURRENT",
  "continuous": true,
  "development": false,
  "notifications": [
    {
      "email_recipients": ["data-eng@example.com"],
      "alerts": ["on-update-failure", "on-flow-failure"]
    }
  ],
  "gateway_definition": {
    "connection_name": "sqlserver_prod",
    "gateway_storage_catalog": "main",
    "gateway_storage_schema": "_lfc_staging",
    "gateway_storage_name": "sqlserver_prod_gateway_storage"
  }
}
```

Response contains `{"pipeline_id": "<GATEWAY_PIPELINE_ID>"}`.

**Step 2 — ingestion pipeline.** `POST /api/2.0/pipelines`

```json
{
  "name": "sqlserver_ingest_prod",
  "catalog": "main",
  "schema": "sqlserver_pipeline_meta",
  "serverless": true,
  "channel": "CURRENT",
  "continuous": false,
  "development": false,
  "notifications": [
    {
      "email_recipients": ["data-eng@example.com"],
      "alerts": ["on-update-failure", "on-flow-failure"]
    }
  ],
  "ingestion_definition": {
    "ingestion_gateway_id": "<GATEWAY_PIPELINE_ID>",
    "table_configuration": {
      "scd_type": "SCD_TYPE_1"
    },
    "objects": [
      {
        "table": {
          "source_catalog": "DemoDB",
          "source_schema": "dbo",
          "source_table": "customers",
          "destination_catalog": "main",
          "destination_schema": "sqlserver_bronze",
          "destination_table": "customers",
          "table_configuration": {
            "scd_type": "SCD_TYPE_2",
            "primary_keys": ["customer_id"],
            "sequence_by": ["updated_at"]
          }
        }
      },
      {
        "schema": {
          "source_catalog": "DemoDB",
          "source_schema": "sales",
          "destination_catalog": "main",
          "destination_schema": "sqlserver_bronze_sales",
          "table_configuration": {
            "scd_type": "SCD_TYPE_1"
          }
        }
      }
    ]
  }
}
```

`[OBSERVED]` `source_catalog` set to the SQL Server database name is exactly what the real production pipeline did.

### Equivalent CLI

```bash
databricks pipelines create --json @gateway.json     # capture pipeline_id
databricks pipelines create --json @ingestion.json
```

### (c) SQL Server integrated CDC — single pipeline, no gateway

`[DOCS]` The [SQL Server integrated pipeline page](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-integrated-pipeline) (GA). One `POST /api/2.0/pipelines` with `connector_type: CDC` referencing the UC connection directly. No gateway pipeline, so no `ingestion_gateway_id`. `source_catalog` is the SQL Server database name. `data_staging_options` names where the pipeline creates its staging volume; omit it and the pipeline autocreates one in the destination schema.

```json
{
  "name": "sqlserver_integrated_cdc_prod",
  "catalog": "main",
  "schema": "erp",
  "serverless": true,
  "channel": "CURRENT",
  "ingestion_definition": {
    "connection_name": "sqlserver_prod",
    "connector_type": "CDC",
    "objects": [
      {
        "table": {
          "source_catalog": "ERPProd",
          "source_schema": "dbo",
          "source_table": "Orders",
          "destination_catalog": "main",
          "destination_schema": "erp",
          "destination_table": "orders",
          "table_configuration": { "scd_type": "SCD_TYPE_1" }
        }
      }
    ],
    "data_staging_options": {
      "catalog_name": "main",
      "schema_name": "ingestion_staging"
    }
  }
}
```

`[DOCS]` Notes: integrated CDC runs on serverless (`serverless: true`) or classic compute (the API default `false`). It requires the integrated CDC connector to be enabled on the workspace (account team). `SCD_TYPE_2` requires SQL Server CDC on the source, not change tracking. Only SQL Server and Oracle are supported source types for `connector_type: CDC`. Schedule it with a Lakeflow Job (triggered); continuous mode is Beta. The gateway-based and integrated architectures are mutually exclusive per pipeline, and `connection_name`/`connector_type` are immutable after creation.

## 2.7 Top-level pipeline fields for managed ingestion

`[SCHEMA]` + `[DOCS]`, restricted to what matters here.

| Field | Applies to ingestion? | Notes |
|---|---|---|
| `name` | yes | Friendly identifier |
| `catalog` | yes | `[DOCS]` "cannot be used with `ingestion_definition`" per the `ingestion_definition` description — **but** `[OBSERVED]` every real ingestion pipeline sets both `catalog` and `schema`, and `[DOCS]` official Salesforce YAML examples set both. The doc note is stale/wrong. Set both; they locate the event log |
| `schema` | yes | Same. Prefer `schema` over the deprecated `target` |
| `serverless` | yes | `[OBSERVED]` `true` for ingestion pipelines, `false` for the SQL Server gateway. `[DOCS]` serverless compute is a stated requirement for creating an ingestion pipeline |
| `channel` | yes | `CURRENT` or `PREVIEW`. `[OBSERVED]` `CURRENT` |
| `development` | yes | Defaults to `false`. `[OBSERVED]` `true` on dev-target bundle deployments |
| `continuous` | yes, **but deprecated** | See §3 |
| `photon` | **no practical effect** | `[SCHEMA]` exists on the spec, but ingestion pipelines are serverless and compute is managed. `[OBSERVED]` not set on any real ingestion pipeline. Omit it |
| `configuration` | yes | String→string map. Where `pipelines.enableSalesforceFormulaFieldsMVComputation` goes |
| `notifications` | yes | `[OBSERVED]` `[{ "email_recipients": [...], "alerts": ["on-update-failure","on-flow-failure"] }]` |
| `tags` | yes | String→string map |
| `edition` | probably ignore | `[OBSERVED]` `ADVANCED` appears on real specs but is bundle/legacy-set. Omit for serverless |
| `libraries`, `clusters`, `storage`, `target`, `filters`, `environment` | **no** | `[DOCS]` `ingestion_definition` "cannot be used with the `libraries`, `schema`, `target`, or `catalog` settings" — the `libraries`/`target` half of that is correct |
| `budget_policy_id`, `usage_policy_id` | optional | Serverless cost attribution |
| `restart_window` | continuous only | `{ start_hour (required), days_of_week, time_zone_id }` |
| `trigger` | **deprecated** | See §3 |

---

# Part 3 — Scheduling

## 3.1 There is no supported pipeline-level schedule field

This is the important structural finding. `[DOCS]` from the pipelines API reference:

- `trigger` — *"Which pipeline trigger to use. **Deprecated: Use `continuous` instead.**"* Shape: `{"cron": {"quartz_cron_schedule": "...", "timezone_id": "..."}}` or `{"manual": {}}`.
- `continuous` — *"Whether the pipeline is continuous or triggered. This replaces `trigger`. **Deprecated: wrap the pipeline in a continuous job instead**, which also lets you take advantage of job-level settings such as performance mode. When the pipeline is started by a continuous job, the job's setting takes precedence and this field is ignored."*

So both pipeline-level scheduling mechanisms are deprecated, and the deprecation chain terminates at **Lakeflow Jobs**. `trigger.cron.quartz_cron_schedule` is the only pipeline-level cron field that ever existed, and it is doubly deprecated. Do not generate it.

`[DOCS]` The official Salesforce ingestion page confirms the job-wrapping pattern as the recommended approach, and the UI's "Schedules and notifications → Create schedule" step creates a job behind the scenes.

## 3.2 The supported pattern: a Lakeflow Job with a `pipeline_task`

`[SCHEMA]` `jobs.PipelineTask`:

| Field | Required | Notes |
|---|---|---|
| `pipeline_id` | **yes** | |
| `full_refresh` | no | Triggers a full refresh of the whole pipeline |
| `full_refresh_selection` | no | Array of tables to full-refresh |
| `refresh_selection` | no | Array of tables to refresh without full refresh |
| `refresh_flow_selection` | no | Flow names to selectively refresh; unioned with the other selection options |
| `reset_checkpoint_selection` | no | Streaming flows to reset checkpoints on without clearing data |

`[SCHEMA]` `jobs.CronSchedule` — **both fields are required**:

| Field | Required | Notes |
|---|---|---|
| `quartz_cron_expression` | **yes** | Quartz syntax |
| `timezone_id` | **yes** | Java timezone ID, e.g. `UTC`, `America/New_York` |
| `pause_status` | no | `PAUSED` / `UNPAUSED` |

Note the field is `quartz_cron_expression` on **jobs**, but `quartz_cron_schedule` on the deprecated **pipeline** trigger. Different names for the same concept — an easy bug.

### JSON: cron-triggered ingestion job

`POST /api/2.2/jobs/create`

```json
{
  "name": "sqlserver_ingest_prod__every_30min",
  "schedule": {
    "quartz_cron_expression": "0 0/30 * * * ?",
    "timezone_id": "UTC",
    "pause_status": "UNPAUSED"
  },
  "max_concurrent_runs": 1,
  "email_notifications": {
    "on_failure": ["data-eng@example.com"]
  },
  "tasks": [
    {
      "task_key": "refresh_ingestion_pipeline",
      "pipeline_task": {
        "pipeline_id": "900e7c4c-bee1-4584-8f76-db8b9a0b8c87",
        "full_refresh": false
      }
    }
  ]
}
```

```bash
databricks jobs create --json @job.json
```

### Alternative: `trigger.periodic`

`[DOCS]` The official Salesforce page's DAB example uses this instead of cron:

```yaml
resources:
  jobs:
    sfdc_dab_job:
      name: sfdc_dab_job
      trigger:
        periodic:
          interval: 1
          unit: DAYS
      email_notifications:
        on_failure:
          - data-eng@example.com
      tasks:
        - task_key: refresh_pipeline
          pipeline_task:
            pipeline_id: ${resources.pipelines.pipeline_sfdc.id}
```

`[SCHEMA]` **`PeriodicTriggerConfigurationTimeUnit` accepts only `HOURS`, `DAYS`, `WEEKS`.** There is no `MINUTES`. So `periodic` cannot express any sub-hourly cadence — anything under 60 minutes must use `schedule.quartz_cron_expression`.

`periodic` semantics differ from cron in a useful way: it fires N units after the *last run finished*, so it never queues overlapping runs. Cron fires on wall-clock boundaries regardless.

## 3.3 Continuous mode

Two options, and they are not equivalent:

1. **Continuous pipeline** (`"continuous": true` on the pipeline). Deprecated, per §3.1. Still what the SQL Server *gateway* uses — `[OBSERVED]` `continuous: true` on the real gateway pipeline. Gateways are inherently always-on and are not job-scheduled.
2. **Continuous job** (`"continuous": {"pause_status": "UNPAUSED"}` on the job, no `schedule`). The non-deprecated path. `[SCHEMA]` also supports `task_retry_mode`. `[DOCS]` "the job's setting takes precedence and this field is ignored" when a continuous job drives the pipeline.

For the **gateway**, keep `continuous: true` on the pipeline itself. For the **ingestion** pipeline, use a job.

## 3.4 Translating Fivetran `sync_frequency` (minutes) → Databricks

Fivetran's `sync_frequency` is a single enum of minutes. Databricks needs a Quartz cron expression (or a periodic trigger). Quartz format is `seconds minutes hours day-of-month month day-of-week [year]`.

| Fivetran (min) | Recommended Databricks config | Notes |
|---|---|---|
| 1 | **Continuous job**: `"continuous": {"pause_status": "UNPAUSED"}`, no `schedule` | See caveats below |
| 5 | **Continuous job**, or `"0 0/5 * * * ?"` | See caveats below |
| 15 | `"0 0/15 * * * ?"` | |
| 30 | `"0 0/30 * * * ?"` | |
| 60 | `"0 0 * * * ?"` | or `periodic: {interval: 1, unit: HOURS}` |
| 120 | `"0 0 0/2 * * ?"` | or `periodic: {interval: 2, unit: HOURS}` |
| 180 | `"0 0 0/3 * * ?"` | or `periodic: {interval: 3, unit: HOURS}` |
| 360 | `"0 0 0/6 * * ?"` | or `periodic: {interval: 6, unit: HOURS}` |
| 480 | `"0 0 0/8 * * ?"` | or `periodic: {interval: 8, unit: HOURS}` |
| 720 | `"0 0 0/12 * * ?"` | or `periodic: {interval: 12, unit: HOURS}` |
| 1440 | `"0 0 0 * * ?"` | or `periodic: {interval: 1, unit: DAYS}`. Consider a non-midnight hour to spread load |

All cron expressions above are `[INFERRED]` — standard Quartz that I did not execute against the API. Quartz requires exactly one of day-of-month / day-of-week to be `?`; the `?` in the day-of-week position above is correct. The `0/N` step syntax is Quartz-standard. Validate with a dry-run job creation before bulk migration.

### Where the two models genuinely differ

1. **No pipeline-level schedule.** Fivetran attaches frequency to the connector. Databricks needs a *second object* (a job) that references the pipeline. A migration tool must create N+1 resources per source, not N, and must record the job↔pipeline mapping.

2. **Sub-hourly needs cron, not `periodic`.** `[SCHEMA]` `periodic` has no `MINUTES` unit. Anything under 60 minutes must be a Quartz expression.

3. **Fivetran syncs are serialized; Databricks jobs are not, by default.** Fivetran will not start a sync while one is running. A Databricks cron job will queue or skip depending on configuration. **Always set `"max_concurrent_runs": 1`** on migrated jobs to approximate Fivetran semantics. `[INFERRED]` — whether the resulting behavior is "skip" or "queue" depends on the job's queue settings, which I did not verify.

4. **1-minute has no clean equivalent.** Cron `"0 * * * * ?"` fires every minute, but each fire is a pipeline update with real startup cost, and it will overlap or thrash. Use a continuous job instead. `[INFERRED]` I found **no documented minimum interval** for Lakeflow Connect managed ingestion. There may well be a source-specific floor (API rate limits on Salesforce/ServiceNow especially). **Do not assume 1-minute Fivetran connectors can be migrated 1:1.** Flag them for human review.

5. **The gateway changes the picture for CDC sources.** For SQL Server and friends, the gateway runs continuously and the ingestion pipeline's schedule only controls how often staged changes are merged into destination tables. Latency is therefore `merge interval`, not `poll interval`, and the two are not comparable to a Fivetran sync frequency on the same axis.

6. **Full refresh is an explicit job-task flag**, not a schedule property. `full_refresh`, `full_refresh_selection`, `refresh_selection` live on `pipeline_task`. Fivetran's re-sync is a separate manual action; if you need periodic full refreshes, use `auto_full_refresh_policy` in `table_configuration` (`{"enabled": true, "min_interval_hours": 24}`) or a second job with `"full_refresh": true`.

---

# Appendix A — Open items requiring verification

Ordered by how badly a wrong guess breaks generated code.

| # | Item | Risk |
|---|---|---|
| 1 | Secret `options` key names for every connector: `password`, `user`, `client_secret`, `refresh_token`, `pem_private_key`, `client_certificate`, `client_private_key`. Redacted by the API, so unobservable via `list`/`get` | **High.** Every connection create depends on these |
| 2 | Salesforce mTLS option keys specifically (`client_private_key`? `client_certificate`? something else) | **High** |
| 3 | ServiceNow ROPC key names for the username/password pair | **High** |
| 4 | `WORKDAY_RAAS` option keys — the sampled connection had `options: null`, so nothing is known | **High** |
| 5 | Whether `PATCH` on connections merges or replaces the `options` map | Medium |
| 6 | Whether a SQL-created (`CREATE CONNECTION`) `SQLSERVER`/`POSTGRESQL` connection is usable as an ingestion `connection_name` | Medium — avoidable by always using the REST API |
| 7 | The authoritative list of source types that require a gateway pipeline vs. combined CDC | Medium |
| 8 | Minimum supported ingestion interval, overall and per source | Medium — affects 1/5-minute Fivetran migrations |
| 9 | Whether `credential_type` is accepted (or required) as an explicit input to disambiguate multi-auth types | Low |
| 10 | Exact Quartz expressions above, validated against a real job create | Low |

Fastest way to close items 1–4: create one connection of each type in the UI, then `databricks connections get <name> -o json` and diff. Or post a deliberately incomplete `options` map and read the `INVALID_PARAMETER_VALUE` message, which names missing required keys.

# Appendix B — Reproducing the empirical evidence

```bash
# Field names straight from the SDK-generated schema
databricks bundle schema > dab_schema.json
# then inspect $defs for:
#   pipelines.IngestionPipelineDefinition
#   pipelines.IngestionGatewayPipelineDefinition
#   pipelines.IngestionConfig / TableSpec / SchemaSpec / ReportSpec
#   pipelines.TableSpecificConfig / TableSpecificConfigScdType
#   jobs.PipelineTask / jobs.CronSchedule / jobs.PeriodicTriggerConfigurationTimeUnit

# Real option keys and credential types per connection type
databricks connections list -o json > conns.json

# Real ingestion pipeline specs
databricks pipelines list-pipelines --max-results 0 -o json
databricks pipelines get <pipeline_id> -o json
```
