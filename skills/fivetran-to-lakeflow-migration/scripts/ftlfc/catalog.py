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
    # SQL Server offers both: gateway-based CDC and integrated CDC (no gateway).
    # Oracle uses integrated CDC with no gateway.
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
    # No managed connector; the alternative (a community connector, Auto Loader,
    # federation) is another path, not a dead end.
    NO_MANAGED_CONNECTOR = "no_managed_connector"


@dataclass(frozen=True)
class Target:
    """The Lakeflow Connect landing spot for one Fivetran connector type."""

    connection_type: str | None
    category: Category
    availability: Availability = Availability.UNVERIFIED
    gateway: Gateway = Gateway.NOT_REQUIRED
    #: Whether the source offers a single-pipeline integrated CDC connector
    #: (``connector_type: CDC``, no separate gateway) as an alternative to the
    #: gateway-based architecture. The plan picks the architecture; this only
    #: records that the choice exists.
    supports_integrated_cdc: bool = False
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
    #: the plan starts from this list and pairs each table with the Fivetran
    #: table whose name matches after ignoring case, underscores, and plural
    #: endings (see ``mapping.name_keys``).
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
            return Effort.NO_MANAGED_CONNECTOR
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
    "a connected app for the client credentials flow. Browser OAuth is the fallback if the "
    "customer declines this setup."
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

_INGESTION_DOCS = "https://docs.databricks.com/aws/en/ingestion/"
_DOCS = f"{_INGESTION_DOCS}lakeflow-connect/"
COMMUNITY_CONNECTORS_URL = f"{_INGESTION_DOCS}community-connectors"

_TYPE_ONLY_VERIFIED = (
    "Connector type confirmed against the SDK IngestionSourceType enum. The auth mode was "
    "not confirmed, so verify it in the workspace before promising a date."
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


def _none(alternative: str, notes: str = "", docs_url: str = "") -> Target:
    return Target(
        connection_type=None,
        category=Category.NONE,
        availability=Availability.NONE,
        scriptable=Scriptable.NA,
        alternative=alternative,
        notes=notes,
        docs_url=docs_url,
    )


def _federated() -> Target:
    return Target(
        connection_type=None,
        category=Category.FEDERATION,
        availability=Availability.UNVERIFIED,
        scriptable=Scriptable.YES,
        alternative=_FEDERATION_ALT,
        docs_url=f"{_DOCS}query-based-overview",
    )


# ---------------------------------------------------------------------------
# Supported-table sets for managed SaaS connectors.
#
# Each frozenset lists the source table names the Lakeflow Connect connector
# can ingest.  The plan pairs these with Fivetran's tables by normalised name;
# enabled Fivetran tables with no pair are reported as not available.  Database
# CDC, file, and dynamic-table connectors (ServiceNow, Salesforce) use ``None``
# instead -- any table the customer has is potentially valid.
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

_WORKDAY_HCM_TABLES = frozenset({
    "absence_input",
    "candidate", "candidate_certification", "candidate_education", "candidate_experience",
    "candidate_language", "candidate_name", "candidate_pool_membership", "candidate_skill",
    "candidate_tag", "candidate_website",
    "compensation_allowance_plan_amount_profile",
    "compensation_allowance_plan_percent_profile",
    "compensation_bonus_plan_coverage_date_eligibility_rule",
    "compensation_bonus_plan_performance_factor_scorecard",
    "compensation_bonus_plan_replacement_percent", "compensation_bonus_tranche",
    "compensation_commission_plan_profile", "compensation_future_payment_bonus_plan",
    "compensation_grade", "compensation_grade_profile",
    "compensation_merit_plan_replacement_percent", "compensation_plan", "compensation_step",
    "compensation_stock_plan_amount_profile", "compensation_stock_plan_grant",
    "compensation_stock_plan_unit_profile", "compensation_stock_profile_grant_split",
    "deferred_compensation_eligibility_rule",
    "ethnicity",
    "external_reference_line", "external_reference_participant",
    "gender_identity",
    "interview", "interview_competency", "interview_questionnaire", "interview_session",
    "interview_session_detail",
    "interviewer",
    "job_family_job_profile", "job_profile", "job_profile_certification",
    "job_profile_classification", "job_profile_company_insider", "job_profile_competency",
    "job_profile_education", "job_profile_exempt", "job_profile_language",
    "job_profile_language_ability", "job_profile_pay_rate", "job_profile_responsibility",
    "job_profile_restrict_to_country", "job_profile_skill", "job_profile_training",
    "job_profile_work_experience", "job_profile_worker_compensation", "job_requisition",
    "job_requisition_additional_job_posting_location",
    "job_requisition_additional_job_profile", "job_requisition_additional_location",
    "job_requisition_allowance_plan", "job_requisition_bonus_plan",
    "job_requisition_calculated_plan", "job_requisition_certification",
    "job_requisition_commission_plan", "job_requisition_compensation",
    "job_requisition_competency", "job_requisition_custom_organization",
    "job_requisition_default_assessment_test", "job_requisition_education",
    "job_requisition_language", "job_requisition_language_ability",
    "job_requisition_merit_plan", "job_requisition_organization",
    "job_requisition_pay_plan", "job_requisition_period_salary_plan",
    "job_requisition_position", "job_requisition_responsibility", "job_requisition_skill",
    "job_requisition_stock_plan", "job_requisition_training",
    "job_requisition_unit_allowance_plan", "job_requisition_unit_salary_plan",
    "job_requisition_work_experience",
    "lgbt_identification",
    "organization", "organization_assignment", "organization_leader", "organization_role",
    "organization_role_worker",
    "override_balance",
    "payroll_line", "payroll_line_pay_component", "payroll_line_related_calculation",
    "payroll_line_withholding", "payroll_local_city_authority_tax_worktag",
    "payroll_local_county_authority_tax_worktag", "payroll_national_id", "payroll_result",
    "payroll_state_authority_tax_worktag",
    "position", "position_bonus_plan", "position_certification", "position_competency",
    "position_education", "position_job_family", "position_job_profile",
    "position_language", "position_location", "position_merit_plan",
    "position_organization", "position_pay_plan", "position_responsibility",
    "position_skill", "position_stock_plan", "position_training",
    "position_work_experience", "position_worker_type_restriction",
    "pronoun",
    "questionnaire", "questionnaire_answer", "questionnaire_setup",
    "sexual_orientation",
    "time_off_plan_balance", "time_off_plan_balance_position",
    "worker_collective_agreement", "worker_leave_status", "worker_position_compensation",
    "worker_position_end_employment_reason", "worker_position_job_classification",
    "worker_position_manager", "worker_position_organization",
    "worker_position_payroll_reporting_code", "worker_positions", "worker_probation_period",
    "worker_secondary_termination_reason",
    "workers",
})

_GOOGLE_ADS_TABLES = frozenset({
    # Resource tables
    "customer", "campaign", "campaign_budget", "campaign_criterion",
    "ad_group", "ad_group_criterion", "ad",
    # Report tables
    "customer_report", "campaign_report", "ad_group_report", "ad_report",
    "keyword_report", "search_query_report",
    "customer_hourly_report", "campaign_hourly_report", "ad_group_hourly_report",
    "audience_report", "click_report", "landing_page_report",
    "search_keyword_report",
    # Event tables
    "call_view", "lead_form_submission", "local_services_lead",
    "local_services_lead_conversation",
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

_ANAPLAN_TABLES = frozenset({"audit_events", "users"})

_VERIFIED_DATE = "2026-11-17"

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
            supports_integrated_cdc=True,
            source_prerequisites=_SQLSERVER_PREREQ,
            docs_url=f"{_DOCS}sql-server-reference",
            last_verified=_VERIFIED_DATE,
            notes=(
                "Two GA architectures. Integrated CDC (connector_type: CDC) runs one "
                "serverless pipeline that references the UC connection directly and stages "
                "through a UC volume, with no separate gateway -- the default the plan "
                "generates. The standard architecture instead runs a continuous gateway on "
                "classic compute plus an ingestion pipeline, billed even while idle; select "
                "it with --sqlserver-arch gateway. Integrated CDC must be enabled on the "
                "workspace by the Databricks account team."
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
                "The replication slot must be dropped manually when the pipeline is deleted."
            ),
            docs_url=f"{_DOCS}postgresql-reference",
            last_verified=_VERIFIED_DATE,
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
            docs_url=f"{_DOCS}mysql-reference",
            last_verified=_VERIFIED_DATE,
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
            notes="Uses integrated CDC via LogMiner, so no separate gateway pipeline.",
            docs_url=f"{_DOCS}oracle-reference",
            last_verified=_VERIFIED_DATE,
        )
        for service in ("oracle", "oracle_rds", "oracle_hva", "oracle_ebs")
    },
    "teradata": Target(
        connection_type="TERADATA",
        category=Category.QUERY_BASED,
        availability=Availability.GA,
        source_prerequisites=(
            "Each table needs a monotonically increasing cursor column. Rows with a NULL "
            "cursor are never ingested."
        ),
        notes="Query-based only; there is no Teradata CDC connector.",
        docs_url=f"{_DOCS}query-based-overview",
        last_verified=_VERIFIED_DATE,
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
        docs_url=f"{_DOCS}salesforce-reference",
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
        docs_url=f"{_DOCS}salesforce-reference",
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
            "records."
        ),
        docs_url=f"{_DOCS}workday-reports-reference",
    ),
    "workday_hcm": _saas(
        "WORKDAY_HCM",
        availability=Availability.BETA,
        scriptable=Scriptable.YES,
        prereq=_WORKDAY_RAAS_PREREQ,
        prereq_auto=False,
        fixed_source_schema="default",
        notes="Distinct connection type from WORKDAY_RAAS. Needs instance_url and tenant_name.",
        supported_tables=_WORKDAY_HCM_TABLES,
        docs_url=f"{_DOCS}workday-hcm-reference",
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
        docs_url=f"{_DOCS}servicenow-reference",
    ),
    **{
        service: _saas(
            "NETSUITE",
            availability=Availability.GA,
            prereq=(
                "Token-based auth: consumer key/secret, token id/secret, the Data "
                "Warehouse Integrator role id, and host/port/account id from the JDBC URL. "
                "Issued through the NetSuite UI."
            ),
            prereq_auto=False,
            docs_url=f"{_DOCS}netsuite-reference",
            last_verified=_VERIFIED_DATE,
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
            docs_url=f"{_DOCS}google-analytics-reference",
        )
        for service in ("google_analytics_4", "google_analytics", "google_analytics_360")
    },
    "dynamics_365": _saas(
        "DYNAMICS365",
        availability=Availability.GA,
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
        docs_url=f"{_DOCS}d365-reference",
        last_verified=_VERIFIED_DATE,
    ),
    "sharepoint": Target(
        connection_type="SHAREPOINT",
        category=Category.FILE,
        availability=Availability.GA,
        scriptable=Scriptable.YES,
        auth_note="Entra ID client credentials (OAUTH_M2M) observed alongside U2M.",
        preferred_auth="OAUTH_M2M",
        source_prerequisites="Entra ID app registration plus site or folder permissions.",
        prerequisites_automatable=False,
        docs_url=f"{_DOCS}sharepoint-reference",
        last_verified=_VERIFIED_DATE,
    ),
    "google_drive": Target(
        connection_type="GOOGLE_DRIVE",
        category=Category.FILE,
        availability=Availability.GA,
        scriptable=Scriptable.NO,
        auth_note=_U2M,
        source_prerequisites="Google OAuth client plus Drive folder permissions.",
        prerequisites_automatable=False,
        docs_url=f"{_DOCS}google-drive-reference",
        last_verified=_VERIFIED_DATE,
    ),
    # -- SaaS: browser consent required, connection cannot be scripted ------
    # Marketing Hub is GA; the CRM Hub tables most Fivetran HubSpot connections
    # sync are Beta, so the connector is labelled by its least mature tables.
    "hubspot": _u2m(
        "HUBSPOT",
        availability=Availability.BETA,
        fixed_source_schema="default",
        supported_tables=_HUBSPOT_TABLES,
        docs_url=f"{_DOCS}hubspot-reference",
        last_verified=_VERIFIED_DATE,
    ),
    "zendesk": _u2m(
        "ZENDESK",
        availability=Availability.GA,
        fixed_source_schema="default",
        supported_tables=_ZENDESK_TABLES,
        docs_url=f"{_DOCS}zendesk-support-reference",
        last_verified=_VERIFIED_DATE,
    ),
    "jira": _u2m(
        "JIRA",
        availability=Availability.BETA,
        fixed_source_schema="default",
        supported_tables=_JIRA_TABLES,
        docs_url=f"{_DOCS}jira-reference",
        last_verified=_VERIFIED_DATE,
    ),
    "confluence": _u2m(
        "CONFLUENCE",
        availability=Availability.GA,
        docs_url=f"{_DOCS}confluence-reference",
        last_verified=_VERIFIED_DATE,
    ),
    "github": _u2m(
        "GITHUB",
        availability=Availability.BETA,
        supported_tables=_GITHUB_TABLES,
        docs_url=f"{_DOCS}github-limits",
        last_verified=_VERIFIED_DATE,
    ),
    "smartsheet": _u2m(
        "SMARTSHEET",
        availability=Availability.BETA,
        docs_url=f"{_DOCS}smartsheet-reference",
        last_verified=_VERIFIED_DATE,
    ),
    "google_ads": _u2m(
        "GOOGLE_ADS",
        availability=Availability.BETA,
        supported_tables=_GOOGLE_ADS_TABLES,
        docs_url=f"{_DOCS}google-ads-reference",
        last_verified=_VERIFIED_DATE,
    ),
    "tiktok_ads": _u2m(
        "TIKTOK_ADS",
        availability=Availability.BETA,
        docs_url=f"{_DOCS}tiktok-ads-reference",
        last_verified=_VERIFIED_DATE,
    ),
    "facebook_ads": _u2m(
        "META_MARKETING",
        availability=Availability.BETA,
        notes="Named Meta Ads in Databricks; covers Facebook and Instagram Ads.",
        docs_url=f"{_DOCS}meta-ads-reference",
        last_verified=_VERIFIED_DATE,
    ),
    "instagram_business": _u2m(
        "META_MARKETING",
        availability=Availability.BETA,
        docs_url=f"{_DOCS}meta-ads-reference",
        last_verified=_VERIFIED_DATE,
    ),
    "salesforce_marketing_cloud": _saas(
        "SALESFORCE_MARKETING_CLOUD",
        availability=Availability.BETA,
        notes="Distinct managed connector; auth mode unverified.",
        docs_url=f"{_DOCS}sfmc-reference",
        last_verified=_VERIFIED_DATE,
    ),
    # -- SaaS: connector type confirmed against the SDK enum, auth mode not --
    #
    # These are absent from the browser-OAuth-only list Databricks publishes, and
    # Databricks states most SaaS connections can be created programmatically, so
    # they are modelled as scriptable. Each reference page marks the connector
    # Beta; the auth mode was not confirmed per connector.
    **{
        service: _saas(
            connection_type,
            availability=Availability.BETA,
            notes=_TYPE_ONLY_VERIFIED,
            docs_url=f"{_DOCS}{slug}-reference",
            last_verified=_VERIFIED_DATE,
        )
        for service, connection_type, slug in (
            ("aha", "AHA", "aha"),
            ("amplitude", "AMPLITUDE", "amplitude"),
            ("google_search_console", "GOOGLE_SEARCH_CONSOLE", "google-search-console"),
            ("marketo", "MARKETO", "marketo"),
            ("monday", "MONDAY_COM", "monday-com"),
            ("notion", "NOTION", "notion"),
            ("outlook", "OUTLOOK", "outlook"),
            ("pendo", "PENDO", "pendo"),
            ("reddit_ads", "REDDIT_ADS", "reddit-ads"),
            ("sendgrid", "SENDGRID", "sendgrid"),
            ("square", "SQUARE", "square"),
            ("zoho_books", "ZOHO_BOOKS", "zoho-books"),
        )
    },
    "anaplan": _saas(
        "ANAPLAN",
        availability=Availability.BETA,
        scriptable=Scriptable.CONDITIONAL,
        auth_note=(
            "Certificate authentication only: an Anaplan admin issues a CA certificate (PEM) "
            "and its private key for the connection. Model data (modules, lists, exports) is "
            "not covered."
        ),
        prereq_auto=False,
        fixed_source_schema="default",
        supported_tables=_ANAPLAN_TABLES,
        docs_url=f"{_DOCS}anaplan-reference",
        last_verified=_VERIFIED_DATE,
    ),
    "pagerduty": _saas(
        "PAGERDUTY",
        availability=Availability.BETA,
        fixed_source_schema="default",
        supported_tables=_PAGERDUTY_TABLES,
        docs_url=f"{_DOCS}pagerduty-reference",
        last_verified=_VERIFIED_DATE,
    ),
    "linkedin_ads": _saas(
        "LINKEDIN_ADS",
        availability=Availability.BETA,
        notes=_TYPE_ONLY_VERIFIED,
        supported_tables=_LINKEDIN_ADS_TABLES,
        docs_url=f"{_DOCS}linkedin-ads-reference",
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
            docs_url=f"{_INGESTION_DOCS}cloud-object-storage/auto-loader/",
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
        service: _none(
            "Standard SFTP/FTP connector authored as a Lakeflow pipeline.",
            docs_url=f"{_INGESTION_DOCS}sftp",
        )
        for service in ("sftp", "ftp")
    },
    **{
        service: _federated()
        for service in ("snowflake_db", "redshift_db", "big_query", "azure_synapse")
    },
    **{
        service: _none(
            "No managed connector or federation path yet. Check Lakeflow Community "
            f"Connectors (open source; {COMMUNITY_CONNECTORS_URL}) for this source, "
            "or build a custom connector; otherwise keep it on Fivetran or use a partner tool."
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
        docs_url=(
            "https://docs.databricks.com/aws/en/query-federation/"
            "salesforce-data-cloud-file-sharing"
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
        "Not in this catalog. Check the current managed connector list and Lakeflow "
        f"Community Connectors (open source; {COMMUNITY_CONNECTORS_URL}), then build "
        "a custom connector or use a partner tool."
    ),
    notes="Unrecognised Fivetran service id.",
)


def lookup(fivetran_service: str) -> Target:
    """Resolve a Fivetran service id to its Lakeflow Connect target."""
    return CATALOG.get((fivetran_service or "").strip().lower(), UNKNOWN)


def is_known(fivetran_service: str) -> bool:
    return (fivetran_service or "").strip().lower() in CATALOG
