# Lakeflow Connect: Managed Connector Coverage & Source-Side Prerequisites

**Research date:** 2026-08-24
**Primary sources:** `docs.databricks.com` (AWS variant unless noted). Azure (`learn.microsoft.com/azure/databricks`) and GCP variants of the same pages carry equivalent content.

> **Availability caveat — read this first.** The Lakeflow Connect connector catalog and release states change frequently. The managed SaaS connector index page was last updated **2026-08-16**, the database connector index **2026-08-04**. Databricks' own overview page states only that "managed connectors in Lakeflow Connect are in various release states" — it does **not** publish a consolidated GA/Preview/Beta matrix. Release state is documented per-connector, in an admonition at the top of each connector's page. **Any skill or automation built on this document must re-verify availability against the source-of-truth index pages cited below before relying on it.** Rows below marked `unverified` were not individually confirmed within the research budget.
>
> Source-of-truth pages to re-check:
> - Managed connector overview: https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/
> - Managed SaaS connectors: https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/saas-overview
> - Managed database connectors (CDC): https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/cdc-overview
> - Query-based connectors: https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/query-based-overview
> - Managed file source connectors: https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/file-connectors-overview
> - Standard (non-managed) connectors: https://docs.databricks.com/aws/en/ingestion/

---

## How Databricks now segments Lakeflow Connect

Understanding the taxonomy matters, because "does a connector exist?" has five different answers depending on category:

| Category | What it is | Compute / architecture |
| --- | --- | --- |
| **Managed SaaS connectors** | Fully-managed API-based ingestion from enterprise apps | Connection + ingestion pipeline on serverless. No gateway. |
| **Managed database connectors (CDC)** | Log-based CDC from relational DBs | Connection + **ingestion gateway on classic compute (continuous)** + staging UC volume + ingestion pipeline on serverless |
| **Managed file source connectors** | Unstructured files from enterprise file services | Connection + pipeline on serverless. No gateway. |
| **Query-based connectors** | Direct scheduled query of a source DB using a monotonically increasing cursor column. No CDC config needed. | Serverless. No gateway, no staging storage. |
| **Managed streaming connectors** | Continuous read from message buses | Serverless. No gateway. |
| **Standard connectors** (not managed) | Auto Loader, SFTP, Kafka, Kinesis, Pub/Sub, Pulsar — you author the pipeline | Structured Streaming / Lakeflow pipelines / Databricks SQL |
| **Community + custom connectors** | Open-source community-built, or DIY | Your pipeline |

---

## Part 1 — Connector coverage

Base URL for all `lakeflow-connect/...` paths: `https://docs.databricks.com/aws/en/ingestion/`

### 1a. Requested sources — managed connector status

| Connector | Category | Availability | Gateway required? | Docs URL |
| --- | --- | --- | --- | --- |
| **Salesforce** (Sales Cloud) | SaaS — managed | **GA** (per Databricks product page; not restated in docs index — treat as GA, re-verify) | No | `lakeflow-connect/salesforce-pipeline` |
| **Salesforce Data Cloud** | — | **No managed Lakeflow connector.** Not in the SaaS index. Use Salesforce Data Cloud ↔ Databricks zero-copy / Delta Sharing integration, or DIY. `unverified` (integration exists outside Lakeflow Connect) | n/a | not listed |
| **Salesforce Marketing Cloud** | SaaS — managed | `unverified` (listed as supported; state not confirmed) | No | `lakeflow-connect/saas-overview` |
| **Workday Reports** (custom RaaS reports) | SaaS — managed | **GA** for the connector; **incremental ingestion is Beta** (confirmed on limits page) | No | `lakeflow-connect/workday-reports` |
| **Workday HCM** (standard modules) | SaaS — managed | `unverified` — separate connector from Workday Reports | No | `lakeflow-connect/saas-overview` |
| **ServiceNow** | SaaS — managed | `unverified` (widely marketed; state not confirmed on page) | No | `lakeflow-connect/servicenow-source-setup` |
| **Google Analytics 4** ("Google Analytics Raw Data") | SaaS — managed | `unverified` | No | `lakeflow-connect/google-analytics-connection` |
| **NetSuite** (Oracle NetSuite ERP) | SaaS — managed | `unverified` | No | `lakeflow-connect/netsuite-connection` |
| **Microsoft Dynamics 365** | SaaS — managed | `unverified`. **Ingests via Azure Synapse Link** — not a direct D365 API connector | No (but requires Synapse Link on the source side) | `lakeflow-connect/saas-overview` |
| **SharePoint** | SaaS — managed **and** file-source — managed **and** standard | Two managed paths listed (SaaS "files and list data"; file-source "Microsoft SharePoint (managed)") plus a standard Spark/SQL path. `unverified` which is preferred | No | `lakeflow-connect/file-connectors-overview` |
| **Zendesk Support** | SaaS — managed | `unverified` | No | `lakeflow-connect/saas-overview` |
| **Meta Marketing** → **Meta Ads** | SaaS — managed | `unverified`. Connector name is **Meta Ads** (Facebook + Instagram Ads) | No | `lakeflow-connect/saas-overview` |
| **HubSpot** | SaaS — managed | `unverified` | No | `lakeflow-connect/saas-overview` |
| **Oracle Fusion (ERP/HCM Cloud)** | — | **No managed connector.** Not in the SaaS index. DIY / community / partner. | n/a | not listed |
| **SQL Server** | Database — managed CDC | **GA** (both architectures) | **Yes** in the standard two-pipeline (gateway) architecture; **No** for **integrated CDC** — one serverless pipeline (`connector_type: CDC`), stages through a UC volume, needs the connector enabled on the workspace by the account team. The skill defaults to integrated CDC. | `lakeflow-connect/sql-server-integrated-pipeline` |
| **PostgreSQL** | Database — managed CDC | **Public Preview** — confirmed on page ("Reach out to your Databricks account team to enroll") | **Yes** (gateway on classic compute) | `lakeflow-connect/postgresql-source-setup` |
| **MySQL** | Database — managed CDC | **Public Preview** — confirmed on page ("Contact your Databricks account team to request access") | **Yes** | `lakeflow-connect/mysql-source-setup` |
| **Oracle Database** | Database — managed CDC (LogMiner) | **Beta** — Azure docs variant states "This feature is in Beta… workspace admins control access from the Previews page" | **No** — integrated CDC, single pipeline, no separate gateway | `lakeflow-connect/oracle-integrated-overview` |
| **MariaDB** | Query-based (foreign connection) only | `unverified` | No | `lakeflow-connect/query-based-overview` |
| **Teradata** | **Query-based connector only** — no CDC connector | `unverified` | No | `lakeflow-connect/query-based-overview` |
| **MongoDB** | — | **No managed connector.** Not in any index. Use Lakehouse Federation if supported, partner (Fivetran/Qlik), Spark MongoDB connector, or custom. | n/a | not listed |
| **Snowflake** | — | **No dedicated managed connector.** Covered by **query-based foreign-catalog ingestion over Lakehouse Federation**, or Lakehouse Federation query-only, or Delta Sharing / Iceberg. | No | `lakeflow-connect/query-based-overview` |
| **Amazon Redshift** | — | **No dedicated managed connector.** Same as Snowflake — query-based via Lakehouse Federation foreign catalog. | No | `lakeflow-connect/query-based-overview` |
| **Google BigQuery** | — | **No dedicated managed connector.** Same — query-based via Lakehouse Federation foreign catalog. | No | `lakeflow-connect/query-based-overview` |
| **Apache Kafka** | **Standard connector, not managed** | GA as a standard connector | No | `https://docs.databricks.com/aws/en/ingestion/` |
| **Amazon Kinesis** | **Standard connector, not managed** | GA as a standard connector | No | `https://docs.databricks.com/aws/en/ingestion/` |
| **RabbitMQ** | Managed **streaming** connector | `unverified` — named on the managed-connector overview as the example streaming connector | No | `lakeflow-connect/` |
| **S3 / ADLS / GCS files** | **Auto Loader (standard connector)** — no managed connector | GA | No | `https://docs.databricks.com/aws/en/ingestion/` |
| **SFTP** | **Standard connector** — no managed connector | `unverified` | No | `https://docs.databricks.com/aws/en/ingestion/` |
| **Google Sheets** | — | **No managed connector.** Not in any index. DIY (Sheets API) or partner. | n/a | not listed |
| **Google Drive** | File source — managed | `unverified` | No | `lakeflow-connect/file-connectors-overview` |
| **Confluence** | SaaS — managed | `unverified`. **Browser-OAuth-only** — see Part 2 | No | `lakeflow-connect/saas-overview` |
| **Jira** | SaaS — managed | `unverified`. **Browser-OAuth-only** — see Part 2 | No | `lakeflow-connect/saas-overview` |

### 1b. Full managed SaaS connector list (as of docs page dated 2026-08-16)

Aha!, Amplitude, Anthropic, Confluence, Microsoft Dynamics 365, GitHub, Gmail, Google Ads, Google Analytics, Google Search Console, HubSpot, Jira, LinkedIn Ads, Marketo (Adobe Marketo Engage), Meta Ads, Monday.com, Netskope Logs, NetSuite, Notion, OpenAI, Outlook, PagerDuty, Pendo, Reddit Ads, Salesforce, Salesforce Marketing Cloud, SendGrid, ServiceNow, SharePoint, Smartsheet, Square, Strac, TikTok Ads, Wiz Audit Logs, Workday HCM, Workday Reports, Workiva, Zendesk Support, Zip, Zoho Books.

**Managed database (CDC):** MySQL, PostgreSQL, Microsoft SQL Server, Oracle.
**Managed file source:** Google Drive, Microsoft SharePoint.
**Managed streaming:** RabbitMQ (only one named in docs; `unverified` whether others exist).
**Query-based, foreign connection:** Oracle, Teradata, SQL Server, MySQL, MariaDB, PostgreSQL.
**Query-based, foreign catalog:** all Lakehouse Federation sources.

> **Documentation inconsistency to flag.** The managed-connector overview page lists **Slack** and **Slack Audit Logs** among connectors that use browser-based OAuth, but neither appears in the managed SaaS connector index. Treat Slack as `unverified` — possibly newly added, possibly stale text.

### 1b-bis. A machine-readable list the docs do not give you

The caveat above says Databricks publishes no consolidated connector matrix. That
is true of the *documentation*, but the SDK ships an authoritative list of
connector identifiers that the docs pages do not reproduce:

```bash
databricks bundle schema | jq -r '.["$defs"]["github.com"].databricks["databricks-sdk-go"]
  .service["pipelines.IngestionSourceType"].oneOf[0].enum[]' | sort
```

On CLI v1.1.0 this returns **101 values** against the ~40 connectors named in the
SaaS index — it includes database, file, streaming, and query-based types, and
also names connectors absent from the public index entirely (`ORACLE_FUSION_CLOUD`,
`SAP_SUCCESSFACTORS`, `EPIC_CLARITY`, `GUIDEWIRE`, `VEEVA_VAULT`, and others).

Two caveats on how far to trust it. Enum membership proves the platform knows the
type; it says **nothing** about release state, so a value here may be Private
Preview or not yet enabled in a given workspace. And it is the *source type* enum,
which is output-only on a pipeline — but every connection type observed in a live
metastore was also a member, so it is the right vocabulary for `connection_type`.

Use it as the authority on **spelling** (`MONDAY_COM`, not `MONDAY`;
`SALESFORCE_MARKETING_CLOUD`, not `SALESFORCE`) and as a superset for discovering
connectors, and use the docs index for availability. `tests/test_bundle_schema.py`
pins every catalog entry against this enum.

Named in the docs index but **absent** from the enum: **Gmail** and **Workiva**.
Gmail's nearest member, `GOOGLE_WORKSPACE`, is broader than the connector name, so
both are left unmapped rather than guessed.

### 1c. Non-managed alternatives for uncovered sources

| Need | Use instead |
| --- | --- |
| Cloud object storage (S3, ADLS, GCS) | **Auto Loader** (`cloudFiles`) via Structured Streaming, Lakeflow pipelines, or Databricks SQL streaming tables; `COPY INTO` for small scheduled loads |
| Message buses (Kafka, Kinesis, Pub/Sub, Pulsar) | **Standard streaming connectors** at three customization levels |
| SFTP | **Standard connector** (Python/SQL) |
| Query without copying (Snowflake, Redshift, BigQuery, and others) | **Lakehouse Federation** — and query-based Lakeflow ingestion can read *through* a federation foreign catalog when you do want a copy |
| SAP Business Data Cloud | **SAP BDC connector using OpenSharing / Delta Sharing** — explicitly *not* Lakeflow Connect |
| Cross-org data exchange | **Delta Sharing** |
| Anything with no connector (MongoDB, Google Sheets, Oracle Fusion, Salesforce Data Cloud) | Partner connectors (Fivetran, Qlik, Arcion-class tools), **community connectors**, or **build a custom connector** (`lakeflow-connect/` → "Build a custom connector") |
| High-throughput direct writes from apps | **Zerobus Ingest** |

---

## Part 2 — Source-side prerequisites

| Connector | Source-side prerequisites | Automatable via API/script? | Notes |
| --- | --- | --- | --- |
| **SQL Server** | Version: change tracking needs SQL Server 2012+; CDC needs 2012 SP1 CU3+ (and Enterprise Edition for pre-2016). Enable **change tracking** (preferred for tables *with* a primary key — lightweight) and/or **CDC** (required for tables *without* a PK). Databricks ships a **utility objects script** installing `dbo.lakeflowSetupChangeTracking`, `dbo.lakeflowSetupChangeDataCapture`, `dbo.lakeflowFixPermissions`. These wrap the database- and table-level enablement (equivalent to `sys.sp_cdc_enable_db` / `sp_cdc_enable_table`), create DDL-audit objects (`lakeflowDdlAudit_1_5`), capture-instance management procs, and grant the ingestion user its permissions. Ingestion user needs `SELECT` on target tables, `VIEW CHANGE TRACKING`, and `EXECUTE` on `master` procs `sp_tables`, `sp_columns_100`, `sp_pkeys`, `sp_statistics_100`. Script runner needs `db_owner`; on **Amazon RDS**, the script runner must be the RDS master user or be granted `EXECUTE ON msdb.dbo.rds_cdc_enable_db` (CDC only, not needed for change tracking). Firewall allowlisting for the gateway. | **Yes** — fully T-SQL scriptable | Standard architecture requires a **continuously-running ingestion gateway on classic compute** (you pay for it even when the ingestion pipeline is idle; undersized compute can fail the initial snapshot). The newer **integrated CDC** architecture removes the gateway. Same source config applies to both. Docs: `lakeflow-connect/sql-server-source-setup`, `.../sql-server-utility`, `.../sql-server-utility-reference`, `.../sql-server-privileges` |
| **PostgreSQL** | PostgreSQL **13+**. `wal_level = logical` (**requires a server restart**). Create a dedicated replication user (`CREATE USER … ; GRANT CONNECT/USAGE/SELECT; ALTER USER … WITH REPLICATION`). Set **`REPLICA IDENTITY`** per table: `DEFAULT` if PK and no TOASTable columns; **`FULL`** if PK + large variable-length columns, or if no PK. Create a **publication** per database (`CREATE PUBLICATION … FOR TABLE …` needs table ownership; `FOR ALL TABLES` needs **superuser**). Create a **logical replication slot** per database using the **`pgoutput`** plugin only, created *by the replication user* (`SET ROLE databricks_replication; SELECT pg_create_logical_replication_slot('databricks_slot','pgoutput');`). Publications must exist **before** slots. Server params: `max_replication_slots` ≥ #databases, `max_wal_senders` ≥ #slots, and set `max_slot_wal_keep_size` to a finite value (default `-1` risks unbounded WAL bloat). **RDS/Aurora:** `rds.logical_replication = 1` in the parameter group + `GRANT rds_replication TO <user>`. **Azure Database for PostgreSQL:** enable logical replication in server parameters (portal or CLI); Flexible Server supported. **GCP Cloud SQL:** `cloudsql.logical_decoding = on` (+ `cloudsql.enable_pglogical` if using pglogical). Optional inline DDL tracking uses a Databricks-supplied SQL script creating `lakeflow_ddl_audit_table_1_0` + two event triggers, then `ALTER PUBLICATION … ADD TABLE`. | **Yes** — SQL + cloud-provider API/CLI for parameter groups and flags. Restart is automatable but disruptive. | Setup is done with **admin/superuser/table-owner** credentials; the pipeline itself only stores the **replication user** creds in the UC connection. Requires **gateway on classic compute**. On pipeline deletion you must **manually drop the replication slot**. Inline DDL tracking is currently **on hold for stabilization**. Docs: `lakeflow-connect/postgresql-source-setup` |
| **MySQL** | Versions: RDS 5.7.44+, Aurora 5.7.mysql_aurora.2.12.2+, Aurora Serverless, Azure MySQL Flexible Server 5.7.44+, EC2 5.7.44+, GCP Cloud SQL 5.7.44+. Enable **binary logging**; **`binlog_format = ROW`**; **`binlog_row_image = FULL`**. Create a MySQL user with replication privileges. Network/firewall/security-group/peering config. | **Yes** — SQL grants + cloud parameter-group API/CLI | Binlog **retention** must be long enough that the continuously-running gateway consumes changes before truncation — docs emphasize the gateway runs continuously for exactly this reason, though the MySQL page does not state a specific retention value (`unverified`). **Read replicas supported** for RDS MySQL, Azure MySQL, MySQL on EC2; **not supported for Aurora MySQL read replicas** (must use primary). Requires **gateway on classic compute**. Docs: `lakeflow-connect/mysql-source-setup` |
| **Oracle Database** | Oracle **12c+** (12c, 18c, 19c, 21c, 23ai, 26ai). **`ARCHIVELOG` mode** enabled with adequate log retention. **Supplemental logging** on each replicated table: **primary-key supplemental logging is the minimum**; **full** supplemental logging required for tables receiving `UPDATE`s on PK/unique-key columns. **Minimal supplemental logging alone is insufficient.** Create a replication user with LogMiner privileges — Databricks ships a PL/SQL tool **`DBX_ORACLE_SETUP_UTIL`** with `create_user`, `grant_permissions`, `grant_select_permissions`, `grant_select_on_table`, `validate_setup`, `drop_user`. It auto-selects standard vs. Amazon RDS `rdsadmin` grant paths and sets `CONTAINER_DATA=ALL` for multi-tenant. Multi-tenant requires a **common user in `CDB$ROOT`** with a `C##` prefix. Must be a **primary** (not standby) database. Run steps as `SYSDBA` (or `ADMIN` on RDS). | **Yes** — Databricks-provided PL/SQL package | Uses **LogMiner in uncommitted-transaction mode**; **XStream is not used**. **No gateway** — integrated CDC single pipeline. **Not supported:** Oracle RAC, and TDE-encrypted data with a **closed wallet** (wallet/keystore must be open or LogMiner validation fails). In large environments (100+ schemas / 1,000+ tables), grant `SELECT` only on what you replicate or schema discovery times out. Basic username/password auth only. Docs: `lakeflow-connect/oracle-integrated-setup`, `.../oracle-integrated-overview`, `.../oracle-privileges`, `.../oracle-troubleshoot` |
| **Teradata / MariaDB (query-based)** | A UC connection with credentials, plus a **monotonically increasing cursor column** per table (timestamp/date preferred; numeric or binary allowed). Rows with `NULL` cursor are never ingested; rows at or below the stored high-water mark are never re-ingested. | **Yes** | No CDC config, no gateway, no staging. Hard-delete detection (`hard_deletion_sync_min_interval_in_seconds`) is **Beta and API-only**. Docs: `lakeflow-connect/query-based-overview`, `.../query-based-reference` |
| **Salesforce** | A Salesforce user dedicated to ingestion, **API-enabled**, with access to every object you plan to ingest. The **Databricks connected app** must be usable in the org. Since Sept 2025 Salesforce restricts *uninstalled* connected apps, so the authenticating user needs: if **API Access Control is enabled** → `Customize Application` **and** (`Modify All Data` **or** `Manage Connected Apps`); if **not enabled** → `Approve Uninstalled Connected Apps`. Without these, **a Salesforce admin must install the Databricks connected app**. Auth options: **OAuth U2M** (interactive sign-in) or **mTLS + OAuth client-credentials** (for API-only users / orgs requiring mTLS) — the mTLS path requires creating your own connected app in Salesforce and assigning the client-credentials flow to the mutual-auth-enabled user. | **Partial** | Max **4 connections per authenticating Salesforce user**. Sandbox needs the `Is Sandbox` flag. Formula-field handling: **not confirmed** in the pages reviewed — `unverified`. Docs: `lakeflow-connect/salesforce-connection`, `.../salesforce-pipeline`, `.../salesforce-troubleshoot` |
| **Workday Reports** | 1) Get the **RaaS report URL**: open the custom report → **Web Service > View URLs** → copy the **JSON** URL, preserving `format=json`. 2) Create an **Integration System User (ISU)** via `Create Integration System User` task, with **Do Not Allow UI Sessions** checked. 3) Create a security group of type **Integration System Security Group (Unconstrained)** and add the ISU. 4) **Maintain Domain Permissions** for that group — add the report's required domain security policies with **GET** access (e.g. `Worker Data: Current Staffing Information`, `Worker Data: Public Worker Reports`, `Worker Data: Active and Terminated Workers`, `Worker Data: All Positions`, `Worker Data: Business Title on Worker Profile`, `Person Data: Work Contact Information`, `Worker Data: Workers`, `Workday Accounts`). 5) **Register API Client for Integrations** → Non-Expiring Refresh Token, scope `System` → copy Client ID + Secret. 6) Add the security group's **functional areas** as API-client scopes. 7) **Manage Refresh Token for Integrations** for the ISU → generate refresh token. 8) For incremental ingestion, the report must expose a monotonically increasing cursor and **two inclusive prompts** (`is on or after` / `is on or before`) — set by the report owner when the report is authored. | **No** | This is the clearest all-manual case. Every step is a Workday task in the Workday UI. Limits: reports under **2 GB or 1M records** (your org's API limits may be lower); no duplicate PKs; incremental ingestion is **Beta**; exclusive prompts cause silent data loss; `current_date()` is evaluated as UTC by Databricks but interpreted in the Workday account's timezone. Docs: `lakeflow-connect/workday-reports-source-setup`, `.../workday-reports-limits` |
| **Workday HCM** | `unverified` — separate connector, source setup not reviewed. Expect ISU + security-group + API-client work analogous to Workday Reports. | `unverified` (likely No) | Docs: `lakeflow-connect/saas-overview` |
| **ServiceNow** | Create an **OAuth API endpoint for external clients** under **System OAuth > Application Registry**; Auth Scope `useraccount`; for U2M set Redirect URL to `https://<workspace-url>/login/oauth/servicenow.html`. Copy Client ID + Secret. Authenticating user must be **Active** and have the **`admin`** role; Databricks also recommends **`snc_read_only`** to restrict it to read-only. A least-privilege alternative using table-level ACLs is documented. The **OAuth 2.0 plugin** must be active. To capture **deletes**, the user needs access to `sys_audit_delete` and the ingested table must not have `no_audit_delete=true`. Instance ID = first segment of `https://<instance>.service-now.com`. | **Partial** | Two auth modes: **U2M OAuth** (recommended; **ServiceNow requires MFA by default**, so a human completes an interactive MFA sign-in; token expires every **100 days** by default) or **ROPC** (username/password — no interactive sign-in, no MFA requirement, longer-lived). The Application Registry record is created in the ServiceNow UI in the docs; whether it can be created via the ServiceNow Table API against `oauth_entity` is **unverified**. Docs: `lakeflow-connect/servicenow-source-setup`, `.../servicenow-connection`, `.../servicenow-troubleshoot` |
| **NetSuite** | Token-based authentication credentials: **Consumer Key**, **Consumer Secret**, **Token ID**, **Token Secret**, plus the **internal Role ID of the "Data Warehouse Integrator" role**, and **Host / Port / Account ID** parsed from your NetSuite **JDBC URL**. | **Partial** | Implies enabling token-based auth, creating an integration record, assigning the Data Warehouse Integrator role, and issuing access tokens in NetSuite — the detailed source-setup page was **not reviewed**, so exact steps are `unverified`. NetSuite has SuiteScript/SOAP automation options but token issuance is conventionally UI-driven. Docs: `lakeflow-connect/netsuite-connection` |
| **Google Analytics 4** ("Google Analytics Raw Data") | Two auth modes: **U2M OAuth** (interactive Google sign-in + consent) or **service-account authentication with a JSON key** (surfaced in the UI as "Username and password"). Service-account mode requires the GCP source setup that produces the JSON key. | **Partial** | Service-account creation, key generation, and the BigQuery IAM roles it needs (BigQuery Data Viewer, Job User, Read Session User on the export project) are fully automatable via `gcloud`/GCP APIs. The one Google Analytics Admin action is **linking the GA4 property to BigQuery export**. Docs: `lakeflow-connect/google-analytics-source-setup`. Docs: `lakeflow-connect/google-analytics-connection` |
| **Microsoft Dynamics 365** | Requires **Azure Synapse Link** configured on the source (the connector ingests CRM/ERP data *through* Synapse Link, not directly from the D365 API). Detailed setup page **not reviewed**. | `unverified` (likely Partial) | This is a meaningful architectural dependency — the customer must already run, or stand up, Synapse Link for Dataverse. Docs: `lakeflow-connect/saas-overview` |
| **SharePoint / Google Drive (file source)** | `unverified` — expect an Entra ID app registration (SharePoint) and Google OAuth client/service account (Drive), plus site/folder-level permissions. | `unverified` | Both also have **standard** (Spark/SQL) connector paths as an alternative. Docs: `lakeflow-connect/file-connectors-overview` |
| **Confluence, Jira, HubSpot, Zendesk Support, Google Ads, Meta Ads, TikTok Ads, Slack, Slack Audit Logs** | Whatever the vendor requires for OAuth app consent, **plus** an interactive browser sign-in by a human with sufficient rights in the source app. | **No** (for the Databricks connection step) | See the hard blocker section below. Docs: `lakeflow-connect/` → "Create connections programmatically" |

### Databricks-side automation (for completeness)

Databricks explicitly supports creating connections programmatically for **all database connectors and most SaaS connectors** — via the **Connections REST API**, `databricks connections create --json`, notebooks, or a pre-deploy script in **Declarative Automation Bundles**. Pipelines and gateways are creatable via UI, REST API, SDKs, CLI, and bundles. Row filtering is API-only and *not* supported for SQL Server or Oracle CDC; column selection/deselection is.

---

## The key question: which connectors require an un-automatable human action in the source system?

### Hard blockers — a human must click through a browser, no API alternative

Databricks states this directly: *"Connectors that use browser-based OAuth (OAuth U2M) as their only authentication option cannot be created programmatically. These connectors require interactive sign-in to obtain the initial OAuth token."*

That list, verbatim from the docs:

- **Confluence**
- **Google Ads**
- **HubSpot**
- **Jira**
- **Meta Ads**
- **Slack**
- **Slack Audit Logs**
- **TikTok Ads**
- **Zendesk Support**

For these nine, there is no scriptable path to the initial token. Full stop. The *connection* step must hand off to a human, but only that step: the ingestion pipeline and its job reference the connection by name, so they are still generated in the bundle and deploy normally once the connection exists. (Source: managed-connector overview, "Create connections programmatically".)

### Effectively manual — the source-side config lives entirely in a vendor UI

- **Workday Reports (and, by extension, Workday HCM).** Every prerequisite is a Workday task: create the ISU, create the Integration System Security Group, maintain domain permissions, register the API client, add functional-area scopes, generate the refresh token, and retrieve the RaaS URL from **Web Service > View URLs**. Databricks documents all of it as UI clicks. There is additionally a **report-authoring** dependency that no API can fix: the report's date prompts must be **inclusive**, which the report owner chooses at report-creation time in Workday. **This is the single most human-dependent connector in the catalog.**

- **Salesforce, conditionally.** If the authenticating user lacks `Approve Uninstalled Connected Apps` (or the API-Access-Control equivalents), **a Salesforce admin must install the Databricks connected app** in the org — an admin UI action. The default OAuth U2M path also requires an interactive login. The **mTLS + client-credentials** path removes the interactive login for ongoing operation, but getting there requires creating and configuring a connected app in Salesforce Setup first.

- **ServiceNow, partially.** The **Application Registry** OAuth endpoint is documented as UI-created. If you choose the recommended **U2M OAuth**, a human must complete a **ServiceNow MFA** sign-in, and must re-authorize roughly every **100 days**. Choosing **ROPC** eliminates the interactive sign-in and the MFA requirement — so ServiceNow is automatable *if* you accept ROPC's weaker security posture.

- **Microsoft Dynamics 365.** Depends on **Azure Synapse Link** existing on the source. Standing that up is a Power Platform / Azure admin exercise, largely portal-driven. `unverified` whether it is fully API-drivable.

- **NetSuite.** Token-based auth artifacts (integration record, Data Warehouse Integrator role assignment, access tokens) are conventionally issued through the NetSuite UI. `unverified`.

### Fully automatable — all four database connectors

Every managed database connector's source-side prerequisites are pure SQL or vendor-CLI operations, and Databricks ships first-party scripts for three of them:

| Connector | Automation asset |
| --- | --- |
| SQL Server | **Utility objects script** — `lakeflowSetupChangeTracking`, `lakeflowSetupChangeDataCapture`, `lakeflowFixPermissions` |
| Oracle | **`DBX_ORACLE_SETUP_UTIL`** PL/SQL package, including `validate_setup` |
| PostgreSQL | Plain SQL, plus an optional **`lakeflow_pg_ddl_change_tracking.sql`** script for inline DDL tracking |
| MySQL | Plain SQL grants + cloud parameter-group changes |

Two caveats worth planning around: PostgreSQL's `wal_level = logical` change **requires a database restart**, and RDS/Aurora/Azure/Cloud SQL parameter changes go through the cloud provider's API rather than the database — automatable, but a change-window conversation with the DBA either way.

### Practical implication

If you are building a skill or an automated onboarding flow, the clean split is:

1. **Databases (SQL Server, PostgreSQL, MySQL, Oracle) and query-based sources** → automate end to end, including the Databricks connection via the Connections API.
2. **SaaS with a non-interactive auth path (NetSuite, Workday, ServiceNow-via-ROPC, Google Analytics-via-service-account, Salesforce-via-mTLS, Dynamics 365 / SharePoint via Entra client credentials)** → automate the Databricks side; generate a precise, checklist-style handoff for the source-side admin. The skill rates these `medium` and generates the non-interactive auth path by default.
3. **The nine browser-OAuth-only connectors** → do not attempt automation of the connection step. Generate the pipeline and job anyway, plus a manual-connection checklist naming the exact connection the bundle expects. The skill rates these `high`, not `blocked`.

---

## Confidence and gaps

**Confirmed directly from Databricks docs pages:** the five connector-category taxonomies and their membership lists; PostgreSQL / MySQL / Oracle / SQL Server source prerequisites in detail; the browser-OAuth-only connector list; Salesforce connected-app permission matrix; Workday Reports source setup and limits; ServiceNow auth options and roles; NetSuite connection fields; Google Analytics auth options; gateway-vs-serverless architecture per category; query-based supported sources.

**Not confirmed — flagged `unverified` above:**
- **Per-connector GA / Public Preview / Private Preview / Beta status for most SaaS connectors.** Databricks does not publish a consolidated matrix; status appears in a per-page admonition. Confirmed states: PostgreSQL = Public Preview, MySQL = Public Preview, Oracle = Beta, Workday Reports incremental ingestion = Beta, query-based hard-delete detection = Beta. SQL Server / Salesforce / Workday GA is asserted on the Databricks product marketing page, not restated in the docs index — treat with mild suspicion.
- Whether **Slack / Slack Audit Logs** connectors actually exist (docs contradict themselves).
- **Salesforce formula-field** handling.
- Detailed source setup for **Workday HCM, NetSuite, Microsoft Dynamics 365, SharePoint, Google Drive, Salesforce Marketing Cloud**.
- **MySQL binlog retention** minimum duration.
- Whether **ServiceNow OAuth app registration** and **GA4 property access grants** are API-drivable.
- **Salesforce Data Cloud** integration path (exists, but outside Lakeflow Connect).
- Full **managed streaming connector** list beyond RabbitMQ.
