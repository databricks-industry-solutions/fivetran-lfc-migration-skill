"""Fivetran connector -> Lakeflow Connect target mapping.

Three facts drive every migration decision, and they are independent:

1. Does a managed Lakeflow Connect connector exist for this source at all?
2. Can its Unity Catalog connection be created without a human in a browser?
3. Can the source system's own prerequisites be scripted?

A source can be fully supported and still need two humans (Workday). Another can
have no managed connector yet migrate trivially (S3 via Auto Loader). Collapsing
these into a single "supported" flag produces migration plans that are wrong in
both directions, so they stay separate here.

Availability and scriptability both change as Databricks ships. Entries carry
their confidence, and ``UNVERIFIED`` means the research could not confirm it
against docs -- not that it is false. Re-verify before quoting to a customer:
https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Category(str, Enum):
    """How Databricks ingests this source, which determines the architecture."""

    SAAS = "saas_managed"  # connection + serverless pipeline, no gateway
    DATABASE_CDC = "database_cdc"  # log-based CDC, usually gateway + pipeline
    QUERY_BASED = "query_based"  # scheduled cursor-column query, serverless
    FILE = "file_managed"  # managed file-source connector
    STREAMING = "streaming"  # managed streaming connector
    STANDARD = "standard"  # not managed: Auto Loader, Kafka, SFTP
    FEDERATION = "federation"  # Lakehouse Federation foreign catalog
    NONE = "none"  # no Databricks-native path


class Gateway(str, Enum):
    REQUIRED = "required"
    NOT_REQUIRED = "not_required"
    # The set of sources needing a gateway is not authoritatively published.
    # SQL Server is confirmed; Oracle uses integrated CDC with no gateway.
    UNKNOWN = "unknown"


class Scriptable(str, Enum):
    """Whether the UC connection can be created with no human browser step."""

    YES = "yes"
    NO = "no"  # OAuth U2M only; a human must click consent
    CONDITIONAL = "conditional"  # scriptable only on a specific auth path
    NA = "not_applicable"  # no UC connection involved


class Availability(str, Enum):
    GA = "ga"
    PUBLIC_PREVIEW = "public_preview"
    BETA = "beta"
    UNVERIFIED = "unverified"  # connector exists; release state unconfirmed
    NONE = "none"


class Effort(str, Enum):
    """Overall migration difficulty, derived rather than hand-assigned."""

    LOW = "low"  # fully automatable end to end
    MEDIUM = "medium"  # connection is scriptable once a source admin supplies credentials
    HIGH = "high"  # the UC connection needs a one-time interactive browser sign-in
    BLOCKED = "blocked"  # no managed path; needs a different architecture


@dataclass(frozen=True)
class Target:
    """The Lakeflow Connect landing spot for one Fivetran connector type."""

    connection_type: str | None
    category: Category
    availability: Availability = Availability.UNVERIFIED
    gateway: Gateway = Gateway.NOT_REQUIRED
    scriptable: Scriptable = Scriptable.YES
    #: Why it is not scriptable, or what the scriptable path requires.
    auth_note: str = ""
    #: UC ``credential_type`` of the non-interactive auth path to generate by
    #: default, for connectors that also offer browser OAuth. ``None`` means the
    #: connector has a single auth mode.
    preferred_auth: str | None = None
    #: Source-system work that must happen before ingestion can run.
    source_prerequisites: str = ""
    #: Whether those prerequisites can be scripted.
    prerequisites_automatable: bool = True
    #: What to do instead when there is no managed connector.
    alternative: str = ""
    #: Source schema the connector addresses objects under, when it is fixed by
    #: the connector rather than taken from the source. Salesforce, for example,
    #: exposes every SObject under a schema literally named "objects", so
    #: Fivetran's schema name cannot be carried over.
    fixed_source_schema: str | None = None
    notes: str = ""
    #: Fixed set of source table names the Lakeflow Connect connector supports.
    #: ``None`` means the connector supports dynamic/user-defined tables (databases,
    #: ServiceNow) or the table list has not been cataloged yet.  When populated,
    #: any Fivetran table NOT in this set is excluded from the generated pipeline
    #: and flagged as a coverage gap.
    supported_tables: frozenset[str] | None = None
    #: URL to the connector reference page listing supported tables.
    docs_url: str = ""
    #: ISO date (YYYY-MM-DD) when ``supported_tables`` was last verified against
    #: the documentation.  The plan emits a staleness warning when this is older
    #: than 90 days.
    last_verified: str = ""

    @property
    def has_managed_connector(self) -> bool:
        return self.category in (
            Category.SAAS,
            Category.DATABASE_CDC,
            Category.QUERY_BASED,
            Category.FILE,
            Category.STREAMING,
        )

    @property
    def effort(self) -> Effort:
        if not self.has_managed_connector:
            return Effort.BLOCKED
        if self.scriptable is Scriptable.NO:
            return Effort.HIGH
        # Source-admin work (certificates, connected apps, API users) is a
        # prerequisite for the customer, not a reason the connection cannot be
        # created by automation afterwards.
        if self.scriptable is Scriptable.CONDITIONAL or not self.prerequisites_automatable:
            return Effort.MEDIUM
        return Effort.LOW


# Reusable prerequisite descriptions, kept out of the table for readability.
_SQLSERVER_PREREQ = (
    "Enable change tracking (tables with a PK) or CDC (tables without). Databricks "
    "ships a utility script installing lakeflowSetupChangeTracking / "
    "lakeflowSetupChangeDataCapture / lakeflowFixPermissions. Grant the ingestion user "
    "SELECT, VIEW CHANGE TRACKING, and EXECUTE on master metadata procs."
)
_POSTGRES_PREREQ = (
    "PostgreSQL 13+. Set wal_level=logical (requires a restart). Create a replication "
    "user, set REPLICA IDENTITY per table, create a publication, then a pgoutput "
    "replication slot as that user. On RDS/Aurora set rds.logical_replication=1 and "
    "GRANT rds_replication."
)
_MYSQL_PREREQ = (
    "Enable binary logging with binlog_format=ROW and binlog_row_image=FULL, retained "
    "long enough for the gateway to drain. Create a user with replication privileges. "
    "Aurora MySQL read replicas are not supported; use the primary."
)
_ORACLE_PREREQ = (
    "Oracle 12c+ in ARCHIVELOG mode with supplemental logging (full, for tables taking "
    "updates on key columns). Databricks ships the DBX_ORACLE_SETUP_UTIL PL/SQL package. "
    "Not supported: RAC, and TDE with a closed wallet."
)
_SALESFORCE_PREREQ = (
    "An API-enabled Salesforce user with access to every ingested object. The Databricks "
    "connected app must be installed, which needs admin rights unless the user holds "
    "'Approve Uninstalled Connected Apps'. Max 4 connections per authenticating user."
)
_SALESFORCE_MTLS = (
    "Scriptable via mTLS + OAuth client credentials, after a Salesforce admin uploads a "
    "certificate signed by a Salesforce-trusted public CA (allow lead time) and configures "
    "a connected app for the client credentials flow. Earlier docs marked mTLS Beta behind "
    "a workspace preview; the connection page no longer does, so confirm on the workspace "
    "Previews page. Browser OAuth is the fallback if the customer declines this setup."
)
_WORKDAY_RAAS_PREREQ = (
    "All in the Workday UI: create an Integration System User, an unconstrained "
    "Integration System Security Group, maintain domain permissions, register an API "
    "client, add functional-area scopes, generate a refresh token, and copy the RaaS "
    "report JSON URL. Incremental ingestion also needs the report authored with two "
    "inclusive date prompts."
)
_SERVICENOW_PREREQ = (
    "Register an OAuth endpoint under System OAuth > Application Registry with scope "
    "'useraccount'. The user must be active with the admin role (snc_read_only is "
    "recommended alongside). Capturing deletes needs access to sys_audit_delete."
)

_U2M = (
    "Browser-based OAuth (U2M) is the only auth mode. Databricks states these cannot be "
    "created programmatically. A human creates the connection once in the UI; pipeline "
    "creation against it is then fully scriptable."
)

_FEDERATION_ALT = (
    "No dedicated managed connector. Use query-based ingestion through a Lakehouse "
    "Federation foreign catalog, or Delta Sharing / Iceberg where a copy is not needed."
)

_TYPE_ONLY_VERIFIED = (
    "Connector type confirmed against the SDK IngestionSourceType enum. Release state and "
    "auth mode were not confirmed, so verify both in the workspace before promising a date."
)


def _saas(
    connection_type: str,
    *,
    availability: Availability = Availability.UNVERIFIED,
    scriptable: Scriptable = Scriptable.YES,
    auth_note: str = "",
    preferred_auth: str | None = None,
    prereq: str = "",
    prereq_auto: bool = True,
    fixed_source_schema: str | None = None,
    notes: str = "",
    supported_tables: frozenset[str] | None = None,
    docs_url: str = "",
    last_verified: str = "",
) -> Target:
    return Target(
        connection_type=connection_type,
        category=Category.SAAS,
        availability=availability,
        gateway=Gateway.NOT_REQUIRED,
        scriptable=scriptable,
        auth_note=auth_note,
        preferred_auth=preferred_auth,
        source_prerequisites=prereq,
        prerequisites_automatable=prereq_auto,
        fixed_source_schema=fixed_source_schema,
        notes=notes,
        supported_tables=supported_tables,
        docs_url=docs_url,
        last_verified=last_verified,
    )


def _u2m(connection_type: str, **kwargs) -> Target:
    kwargs.setdefault("scriptable", Scriptable.NO)
    kwargs.setdefault("auth_note", _U2M)
    return _saas(connection_type, **kwargs)


def _none(alternative: str, notes: str = "") -> Target:
    return Target(
        connection_type=None,
        category=Category.NONE,
        availability=Availability.NONE,
        scriptable=Scriptable.NA,
        alternative=alternative,
        notes=notes,
    )


def _federated() -> Target:
    return Target(
        connection_type=None,
        category=Category.FEDERATION,
        availability=Availability.UNVERIFIED,
        scriptable=Scriptable.YES,
        alternative=_FEDERATION_ALT,
    )


# ---------------------------------------------------------------------------
# Supported-table sets for managed SaaS connectors.
#
# Each frozenset lists the source table names the Lakeflow Connect connector
# can ingest.  Fivetran tables not in this set are excluded from the generated
# pipeline and flagged as a coverage gap.  Database CDC, file, and dynamic-
# table connectors (ServiceNow, Salesforce) use ``None`` instead -- any table
# the customer has is potentially valid.
# ---------------------------------------------------------------------------

_PAGERDUTY_TABLES = frozenset({
    "incidents", "log_entries", "services", "users", "teams",
    "priorities", "escalation_policies", "schedules", "oncalls", "audit_records",
})

_HUBSPOT_TABLES = frozenset({
    # Marketing Hub
    "email_events", "email_subscription_change", "marketing_emails",
    "email_campaign", "email_campaign_list", "email_subscriptions",
    "form_submissions", "forms",
    "marketing_campaign_asset", "marketing_campaign_budget",
    "marketing_campaign_spend", "marketing_campaigns",
    "marketing_event_list", "marketing_events",
    # CRM Hub (Beta, behind hubspot_connector_crm_objects preview)
    "calls", "companies", "contacts", "deals", "emails", "leads",
    "line_items", "meetings", "notes", "orders", "products", "tasks", "tickets",
    "deals_pipelines", "owners", "tickets_pipelines",
})

_ZENDESK_TABLES = frozenset({
    # Ticketing API
    "tickets", "ticket_audits", "ticket_comments", "ticket_metric_events",
    "ticket_skips", "ticket_triggers", "trigger_categories",
    "side_conversation_events", "users", "user_identities",
    "organizations", "macros", "deletion_schedules",
    "account_attributes", "automations", "brands", "brand_agents",
    "custom_roles", "custom_statuses", "groups", "group_memberships",
    "group_sla_policies", "locales", "organization_fields",
    "organization_memberships", "organization_subscriptions",
    "schedules", "sla_policies", "suspended_tickets", "tags",
    "ticket_activities", "ticket_fields", "ticket_forms",
    "ticket_metrics", "user_fields",
    # Help Center API
    "articles", "article_attachments", "article_comments",
    "article_votes", "categories", "sections",
    # Community API
    "posts", "post_comments", "post_votes", "topics",
    # Audit Log API
    "audit_logs",
})

_JIRA_TABLES = frozenset({
    "application_roles", "boards", "issue_comments", "issue_field_values",
    "issue_fields", "issue_links", "issue_types", "issue_watchers",
    "issue_worklogs", "issues", "permission_schemes", "priority",
    "project_board", "project_categories", "project_components",
    "project_permissions", "project_role_actor", "project_roles",
    "projects", "resolutions", "security_level", "security_schemes",
    "sprints", "status", "status_category", "user_group", "users", "version",
})

_GITHUB_TABLES = frozenset({
    # Incremental
    "repositories", "audit_logs", "repo_contents",
    # Batch
    "branches", "collaborators", "commits", "deployments",
    "deployment_statuses", "discussions", "issue_comments", "issues",
    "labels", "milestones", "org_members",
    "pull_request_commits", "pull_request_review_comments",
    "pull_request_reviews", "pull_requests",
    "releases", "tags", "team_members", "teams", "workflows",
})

_WORKDAY_HCM_TABLES = frozenset({"workers", "payroll_result"})

_GOOGLE_ADS_TABLES = frozenset({
    # Resource tables
    "customer", "campaign", "campaign_budget", "campaign_criterion",
    "ad_group", "ad_group_criterion", "ad",
    # Report tables
    "keyword_report", "search_query_report",
    "customer_hourly_report", "campaign_hourly_report", "ad_group_hourly_report",
    "audience_report", "click_report", "landing_page_report",
    "search_keyword_report",
    # Event tables
    "call_view", "lead_form_submission", "local_services_lead",
})

_LINKEDIN_ADS_TABLES = frozenset({
    # Entity tables
    "account_history", "campaign_group_history", "campaign_history",
    "creative_history", "account_user_history",
    # Report tables
    "ad_analytics_by_campaign_report", "ad_analytics_by_creative_report",
    "monthly_ad_analytics_by_member_company_size_report",
    "monthly_ad_analytics_by_member_country_report",
    "monthly_ad_analytics_by_member_industry_report",
    "monthly_ad_analytics_by_member_job_function_report",
    "monthly_ad_analytics_by_member_seniority_report",
})

_VERIFIED_DATE = "2026-09-15"

# Fivetran service id -> Lakeflow Connect target.
#
# Fivetran uses one service id per source *variant* (postgres, postgres_rds,
# aurora_postgres all being PostgreSQL), so several ids map to one target.
CATALOG: dict[str, Target] = {
    # -- Databases: managed CDC, fully scriptable both sides ----------------
    **{
        service: Target(
            connection_type="SQLSERVER",
            category=Category.DATABASE_CDC,
            availability=Availability.GA,
            gateway=Gateway.REQUIRED,
            source_prerequisites=_SQLSERVER_PREREQ,
            notes=(
                "Standard architecture needs a gateway on classic compute running "
                "continuously, billed even while the ingestion pipeline is idle. Newer "
                "integrated CDC removes the gateway."
            ),
        )
        for service in ("sql_server", "sql_server_rds", "sql_server_hva", "azure_sql_db")
    },
    **{
        service: Target(
            connection_type="POSTGRESQL",
            category=Category.DATABASE_CDC,
            availability=Availability.PUBLIC_PREVIEW,
            gateway=Gateway.REQUIRED,
            source_prerequisites=_POSTGRES_PREREQ,
            notes=(
                "Public Preview; enrollment via the account team. The replication slot "
                "must be dropped manually when the pipeline is deleted."
            ),
        )
        for service in (
            "postgres",
            "postgres_rds",
            "aurora_postgres",
            "azure_postgres",
            "heroku_postgres",
        )
    },
    **{
        service: Target(
            connection_type="MYSQL",
            category=Category.DATABASE_CDC,
            availability=Availability.PUBLIC_PREVIEW,
            gateway=Gateway.REQUIRED,
            source_prerequisites=_MYSQL_PREREQ,
            notes="Public Preview; enrollment via the account team.",
        )
        for service in ("mysql", "mysql_rds", "aurora", "azure_mysql", "maria", "maria_rds")
    },
    **{
        service: Target(
            connection_type="ORACLE",
            category=Category.DATABASE_CDC,
            availability=Availability.BETA,
            gateway=Gateway.NOT_REQUIRED,
            source_prerequisites=_ORACLE_PREREQ,
            notes=(
                "Beta, enabled from the workspace Previews page. Uses integrated CDC via "
                "LogMiner, so no separate gateway pipeline."
            ),
        )
        for service in ("oracle", "oracle_rds", "oracle_hva", "oracle_ebs")
    },
    "teradata": Target(
        connection_type="TERADATA",
        category=Category.QUERY_BASED,
        source_prerequisites=(
            "Each table needs a monotonically increasing cursor column. Rows with a NULL "
            "cursor are never ingested."
        ),
        notes="Query-based only; there is no Teradata CDC connector.",
    ),
    # -- SaaS: scriptable with static credentials ---------------------------
    "salesforce": _saas(
        "SALESFORCE",
        availability=Availability.GA,
        scriptable=Scriptable.CONDITIONAL,
        auth_note=_SALESFORCE_MTLS,
        preferred_auth="OAUTH_MTLS",
        prereq=_SALESFORCE_PREREQ,
        prereq_auto=False,
        fixed_source_schema="objects",
        notes=(
            "For formula fields set the top-level pipeline configuration flag "
            "pipelines.enableSalesforceFormulaFieldsMVComputation, not the private "
            "salesforce_include_formula_fields table field."
        ),
        docs_url=(
            "https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/"
            "salesforce-reference"
        ),
        last_verified=_VERIFIED_DATE,
    ),
    "salesforce_sandbox": _saas(
        "SALESFORCE",
        availability=Availability.GA,
        scriptable=Scriptable.CONDITIONAL,
        auth_note=f"{_SALESFORCE_MTLS} Set the is_sandbox option.",
        preferred_auth="OAUTH_MTLS",
        prereq=_SALESFORCE_PREREQ,
        prereq_auto=False,
        fixed_source_schema="objects",
        docs_url=(
            "https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/"
            "salesforce-reference"
        ),
        last_verified=_VERIFIED_DATE,
    ),
    "workday": _saas(
        "WORKDAY_RAAS",
        scriptable=Scriptable.YES,
        auth_note="Static credentials; no browser step for the Databricks connection.",
        prereq=_WORKDAY_RAAS_PREREQ,
        prereq_auto=False,
        notes=(
            "Ingested as report objects, not tables. Reports are capped around 2 GB or 1M "
            "records and incremental ingestion is Beta."
        ),
    ),
    "workday_hcm": _saas(
        "WORKDAY_HCM",
        scriptable=Scriptable.YES,
        prereq=_WORKDAY_RAAS_PREREQ,
        prereq_auto=False,
        notes="Distinct connection type from WORKDAY_RAAS. Needs instance_url and tenant_name.",
        supported_tables=_WORKDAY_HCM_TABLES,
        docs_url=(
            "https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/"
            "workday-hcm-reference"
        ),
        last_verified=_VERIFIED_DATE,
    ),
    "servicenow": _saas(
        "SERVICENOW",
        scriptable=Scriptable.CONDITIONAL,
        auth_note=(
            "Scriptable via OAuth Resource Owner Password Credentials, confirmed working "
            "in a live metastore. The recommended U2M path instead needs an interactive "
            "MFA sign-in and re-authorization roughly every 100 days."
        ),
        preferred_auth="OAUTH_RESOURCE_OWNER_PASSWORD",
        prereq=_SERVICENOW_PREREQ,
        prereq_auto=False,
        docs_url=(
            "https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/"
            "servicenow-reference"
        ),
        last_verified=_VERIFIED_DATE,
    ),
    **{
        service: _saas(
            "NETSUITE",
            prereq=(
                "Token-based auth: consumer key/secret, token id/secret, the Data "
                "Warehouse Integrator role id, and host/port/account id from the JDBC URL. "
                "Issued through the NetSuite UI."
            ),
            prereq_auto=False,
        )
        for service in ("netsuite_suiteanalytics", "netsuite")
    },
    **{
        service: _saas(
            "GA4_RAW_DATA",
            scriptable=Scriptable.CONDITIONAL,
            auth_note=(
                "Scriptable with a GCP service-account JSON key (surfaced in the UI as "
                "username/password). The default path is browser OAuth."
            ),
            preferred_auth="USERNAME_PASSWORD",
            prereq=(
                "Link the GA4 property to BigQuery export (a Google Analytics Admin action). "
                "Enable the BigQuery, BigQuery Storage, and Cloud Resource Manager APIs on "
                "the export project, then create a service account with BigQuery Data "
                "Viewer, BigQuery Job User, and BigQuery Read Session User, and a JSON key."
            ),
            prereq_auto=False,
        )
        for service in ("google_analytics_4", "google_analytics", "google_analytics_360")
    },
    "dynamics_365": _saas(
        "DYNAMICS365",
        scriptable=Scriptable.YES,
        auth_note="Entra ID client credentials (OAUTH_M2M).",
        prereq=(
            "Requires Azure Synapse Link for Dataverse configured on the source. The "
            "connector reads through Synapse Link, not the D365 API directly."
        ),
        prereq_auto=False,
        notes=(
            "Meaningful architectural dependency if the customer does not already run Synapse Link."
        ),
    ),
    "sharepoint": Target(
        connection_type="SHAREPOINT",
        category=Category.FILE,
        scriptable=Scriptable.YES,
        auth_note="Entra ID client credentials (OAUTH_M2M) observed alongside U2M.",
        preferred_auth="OAUTH_M2M",
        source_prerequisites="Entra ID app registration plus site or folder permissions.",
        prerequisites_automatable=False,
    ),
    "google_drive": Target(
        connection_type="GOOGLE_DRIVE",
        category=Category.FILE,
        scriptable=Scriptable.NO,
        auth_note=_U2M,
        source_prerequisites="Google OAuth client plus Drive folder permissions.",
        prerequisites_automatable=False,
    ),
    # -- SaaS: browser consent required, connection cannot be scripted ------
    "hubspot": _u2m(
        "HUBSPOT",
        supported_tables=_HUBSPOT_TABLES,
        docs_url="https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/hubspot-reference",
        last_verified=_VERIFIED_DATE,
    ),
    "zendesk": _u2m(
        "ZENDESK",
        supported_tables=_ZENDESK_TABLES,
        docs_url=(
            "https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/"
            "zendesk-support-reference"
        ),
        last_verified=_VERIFIED_DATE,
    ),
    "jira": _u2m(
        "JIRA",
        supported_tables=_JIRA_TABLES,
        docs_url="https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/jira-reference",
        last_verified=_VERIFIED_DATE,
    ),
    "confluence": _u2m("CONFLUENCE"),
    "github": _u2m(
        "GITHUB",
        supported_tables=_GITHUB_TABLES,
        docs_url="https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/github-reference",
        last_verified=_VERIFIED_DATE,
    ),
    "smartsheet": _u2m("SMARTSHEET"),
    "google_ads": _u2m(
        "GOOGLE_ADS",
        supported_tables=_GOOGLE_ADS_TABLES,
        docs_url="https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/google-ads-reference",
        last_verified=_VERIFIED_DATE,
    ),
    "tiktok_ads": _u2m("TIKTOK_ADS"),
    "facebook_ads": _u2m(
        "META_MARKETING", notes="Named Meta Ads in Databricks; covers Facebook and Instagram Ads."
    ),
    "instagram_business": _u2m("META_MARKETING"),
    "salesforce_marketing_cloud": _saas(
        "SALESFORCE_MARKETING_CLOUD", notes="Distinct managed connector; auth mode unverified."
    ),
    # -- SaaS: connector type confirmed against the SDK enum, auth mode not --
    #
    # These are absent from the browser-OAuth-only list Databricks publishes, and
    # Databricks states most SaaS connections can be created programmatically, so
    # they are modelled as scriptable. Neither the release state nor the auth mode
    # was confirmed per connector, so both carry the usual unverified caveat.
    **{
        service: _saas(connection_type, notes=_TYPE_ONLY_VERIFIED)
        for service, connection_type in (
            ("aha", "AHA"),
            ("amplitude", "AMPLITUDE"),
            ("google_search_console", "GOOGLE_SEARCH_CONSOLE"),
            ("marketo", "MARKETO"),
            ("monday", "MONDAY_COM"),
            ("notion", "NOTION"),
            ("outlook", "OUTLOOK"),
            ("pendo", "PENDO"),
            ("reddit_ads", "REDDIT_ADS"),
            ("sendgrid", "SENDGRID"),
            ("square", "SQUARE"),
            ("zoho_books", "ZOHO_BOOKS"),
        )
    },
    "pagerduty": _saas(
        "PAGERDUTY",
        availability=Availability.BETA,
        supported_tables=_PAGERDUTY_TABLES,
        docs_url="https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/pagerduty-reference",
        last_verified=_VERIFIED_DATE,
    ),
    "linkedin_ads": _saas(
        "LINKEDIN_ADS",
        notes=_TYPE_ONLY_VERIFIED,
        supported_tables=_LINKEDIN_ADS_TABLES,
        docs_url="https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/linkedin-ads-reference",
        last_verified=_VERIFIED_DATE,
    ),
    # -- No managed connector: a different architecture is the answer -------
    **{
        service: _none(
            "Auto Loader (cloudFiles) via a Lakeflow pipeline or streaming table, or "
            "COPY INTO for small scheduled loads.",
            notes=(
                "Object storage is not a Lakeflow Connect managed connector, but it is a "
                "well-trodden path."
            ),
        )
        for service in ("s3", "gcs", "azure_blob_storage", "azure_data_lake_storage")
    },
    **{
        service: _none(
            "Standard streaming connector via Structured Streaming or a Lakeflow pipeline."
        )
        for service in ("kafka", "confluent_cloud", "aws_msk", "kinesis", "google_pub_sub")
    },
    **{
        service: _none("Standard SFTP/FTP connector authored as a Lakeflow pipeline.")
        for service in ("sftp", "ftp")
    },
    **{
        service: _federated()
        for service in ("snowflake_db", "redshift_db", "big_query", "azure_synapse")
    },
    **{
        service: _none(
            "No managed connector and no federation path. Keep on Fivetran, use a partner "
            "tool, or build a custom connector."
        )
        for service in (
            "mongo",
            "mongo_sharded",
            "dynamodb",
            "cosmos",
            "google_sheets",
            "oracle_fusion",
        )
    },
    "salesforce_data_cloud": _none(
        "Salesforce Data Cloud zero-copy sharing or Delta Sharing, outside Lakeflow Connect.",
        notes=(
            "A SALESFORCE_DATA_CLOUD connection type exists but is U2M-only and not a "
            "managed ingestion connector."
        ),
    ),
}

# Deliberately unmapped: the Databricks connector index lists Gmail and Workiva
# as managed SaaS connectors, but neither has a matching IngestionSourceType in
# the SDK enum, and Gmail's likeliest match (GOOGLE_WORKSPACE) is broader than
# the connector name suggests. Guessing either would emit a connection that
# fails at create time, so they fall through to UNKNOWN, which tells the reader
# to check the current connector list.

#: Fivetran service ids seen in the wild that have no managed equivalent and no
#: obvious alternative. Kept explicit so the report can name them rather than
#: lumping them into an anonymous "unknown" bucket.
UNKNOWN = Target(
    connection_type=None,
    category=Category.NONE,
    availability=Availability.NONE,
    scriptable=Scriptable.NA,
    alternative=(
        "Not in this catalog. Check the current managed connector list, then fall back to "
        "a partner tool, a community connector, or a custom connector."
    ),
    notes="Unrecognised Fivetran service id.",
)


def lookup(fivetran_service: str) -> Target:
    """Resolve a Fivetran service id to its Lakeflow Connect target."""
    return CATALOG.get((fivetran_service or "").strip().lower(), UNKNOWN)


def is_known(fivetran_service: str) -> bool:
    return (fivetran_service or "").strip().lower() in CATALOG
