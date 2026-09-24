"""Tests for the connector catalog and migration plan builder."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "skills/fivetran-to-lakeflow-migration/scripts"
sys.path.insert(0, str(SCRIPTS))

from ftlfc.catalog import CATALOG, Category, Effort, Gateway, Scriptable, lookup
from ftlfc.mapping import CRON_BY_MINUTES, build_plan, to_identifier


class TestCatalog:
    def test_database_sources_are_fully_automatable(self) -> None:
        target = lookup("sql_server")
        assert target.category is Category.DATABASE_CDC
        assert target.gateway is Gateway.REQUIRED
        assert target.scriptable is Scriptable.YES
        assert target.prerequisites_automatable is True
        assert target.effort is Effort.LOW

    def test_oracle_uses_integrated_cdc_without_a_gateway(self) -> None:
        assert lookup("oracle").gateway is Gateway.NOT_REQUIRED

    def test_sql_server_advertises_integrated_cdc_capability(self) -> None:
        # The catalog records the gateway-based architecture as the standard fact
        # and flags that an integrated CDC path exists; the plan picks between them.
        target = lookup("sql_server")
        assert target.gateway is Gateway.REQUIRED
        assert target.supports_integrated_cdc is True

    def test_non_sqlserver_databases_do_not_advertise_integrated_cdc(self) -> None:
        for service in ("postgres", "mysql"):
            assert lookup(service).supports_integrated_cdc is False, service

    def test_source_variants_share_one_target(self) -> None:
        for service in ("postgres", "postgres_rds", "aurora_postgres"):
            assert lookup(service).connection_type == "POSTGRESQL"

    def test_browser_oauth_connectors_are_high_effort(self) -> None:
        target = lookup("hubspot")
        assert target.scriptable is Scriptable.NO
        assert target.effort is Effort.HIGH
        assert target.has_managed_connector is True

    def test_salesforce_is_conditionally_scriptable(self) -> None:
        target = lookup("salesforce")
        assert target.scriptable is Scriptable.CONDITIONAL
        assert "mTLS" in target.auth_note

    @pytest.mark.parametrize("service", ["salesforce", "salesforce_sandbox"])
    def test_salesforce_prefers_mtls_and_is_medium_effort(self, service: str) -> None:
        target = lookup(service)
        assert target.preferred_auth == "OAUTH_MTLS"
        assert target.effort is Effort.MEDIUM

    def test_workday_is_scriptable_but_needs_manual_source_setup(self) -> None:
        target = lookup("workday")
        assert target.scriptable is Scriptable.YES
        assert target.prerequisites_automatable is False
        # Source admin work is a prerequisite; the connection itself is scriptable.
        assert target.effort is Effort.MEDIUM

    @pytest.mark.parametrize(
        "service,preferred_auth",
        [
            ("servicenow", "OAUTH_RESOURCE_OWNER_PASSWORD"),
            ("google_analytics_4", "USERNAME_PASSWORD"),
            ("sharepoint", "OAUTH_M2M"),
        ],
    )
    def test_multi_auth_connectors_prefer_a_non_interactive_path(
        self, service: str, preferred_auth: str
    ) -> None:
        target = lookup(service)
        assert target.preferred_auth == preferred_auth
        assert target.effort is Effort.MEDIUM

    def test_medium_effort_is_reachable(self) -> None:
        assert any(t.effort is Effort.MEDIUM for t in CATALOG.values())

    def test_only_browser_oauth_only_connectors_are_high_effort(self) -> None:
        for service, target in CATALOG.items():
            if target.effort is Effort.HIGH:
                assert target.scriptable is Scriptable.NO, service

    def test_sources_without_a_connector_are_blocked_with_an_alternative(self) -> None:
        target = lookup("s3")
        assert target.has_managed_connector is False
        assert target.effort is Effort.BLOCKED
        assert "Auto Loader" in target.alternative

    def test_warehouses_route_to_federation(self) -> None:
        assert lookup("snowflake_db").category is Category.FEDERATION

    def test_unknown_service_falls_back_without_raising(self) -> None:
        target = lookup("some_new_connector_2027")
        assert target.has_managed_connector is False
        assert target.connection_type is None

    def test_lookup_is_case_and_whitespace_insensitive(self) -> None:
        assert lookup("  SQL_Server ").connection_type == "SQLSERVER"

    def test_meta_ads_is_reachable_by_its_fivetran_name(self) -> None:
        assert lookup("facebook_ads").connection_type == "META_MARKETING"


class TestToIdentifier:
    @pytest.mark.parametrize(
        "parts,expected",
        [
            (("Salesforce Prod",), "salesforce_prod"),
            (("my-schema", "abc123"), "my_schema_abc123"),
            (("UPPER.case",), "upper_case"),
            (("a...b",), "a_b"),
            (("!!!",), "unnamed"),
        ],
    )
    def test_produces_safe_identifiers(self, parts, expected) -> None:
        assert to_identifier(*parts) == expected


def _connection(**overrides) -> dict:
    base = {
        "id": "speak_inexpensive",
        "service": "sql_server",
        "destination_schema": "sales",
        "paused": False,
        "sync_frequency_minutes": 60,
        "networking_method": "Directly",
        "status": {"setup_state": "connected"},
        "objects": [],
    }
    base.update(overrides)
    return base


def _table(**overrides) -> dict:
    base = {
        "source_schema": "dbo",
        "source_table": "Customers",
        "destination_table": "Customers",
        "enabled": True,
        "retains_history": False,
        "primary_keys": ["Id"],
        "primary_keys_known": True,
        "excluded_columns": [],
        "hashed_columns": [],
    }
    base.update(overrides)
    return base


def _inventory(*connections) -> dict:
    return {"account": {"account_id": "acct"}, "connections": list(connections)}


class TestBuildPlan:
    def test_maps_a_straightforward_database_connection(self) -> None:
        plan = build_plan(_inventory(_connection(objects=[_table()])), "main_prod")
        item = plan["items"][0]
        assert item["target"]["connection_type"] == "SQLSERVER"
        assert item["target"]["destination_catalog"] == "main_prod"
        assert item["blockers"] == []
        assert len(item["objects"]) == 1

    def test_salesforce_source_schema_is_rewritten_to_objects(self) -> None:
        # Fivetran writes Salesforce objects under a schema named after the
        # connector, but the connector only resolves them under "objects".
        plan = build_plan(
            _inventory(
                _connection(
                    service="salesforce",
                    destination_schema="salesforce",
                    objects=[_table(source_schema="salesforce", source_table="Account")],
                )
            ),
            "main_prod",
        )
        obj = plan["items"][0]["objects"][0]
        assert obj["source_schema"] == "objects"
        assert obj["fivetran_source_schema"] == "salesforce"
        assert any("rewritten" in w for w in plan["items"][0]["warnings"])

    def test_database_source_schema_is_preserved(self) -> None:
        plan = build_plan(
            _inventory(_connection(objects=[_table(source_schema="dbo")])), "main_prod"
        )
        obj = plan["items"][0]["objects"][0]
        assert obj["source_schema"] == "dbo"
        assert not any("rewritten" in w for w in plan["items"][0]["warnings"])

    def test_history_mode_becomes_scd_type_2(self) -> None:
        plan = build_plan(
            _inventory(_connection(objects=[_table(retains_history=True)])), "main_prod"
        )
        assert plan["items"][0]["objects"][0]["table_configuration"]["scd_type"] == "SCD_TYPE_2"
        assert plan["summary"]["tables_scd2"] == 1

    def test_default_sync_mode_becomes_scd_type_1(self) -> None:
        plan = build_plan(_inventory(_connection(objects=[_table()])), "main_prod")
        assert plan["items"][0]["objects"][0]["table_configuration"]["scd_type"] == "SCD_TYPE_1"

    def test_excluded_columns_carry_across(self) -> None:
        plan = build_plan(
            _inventory(_connection(objects=[_table(excluded_columns=["ssn", "dob"])])), "main_prod"
        )
        config = plan["items"][0]["objects"][0]["table_configuration"]
        assert config["exclude_columns"] == ["ssn", "dob"]
        # include_columns and exclude_columns are mutually exclusive.
        assert "include_columns" not in config

    def test_hashed_columns_warn_rather_than_silently_dropping(self) -> None:
        plan = build_plan(
            _inventory(_connection(objects=[_table(hashed_columns=["email"])])), "main_prod"
        )
        assert any("hashes" in w for w in plan["items"][0]["warnings"])

    @pytest.mark.parametrize("known", [False, True])
    def test_managed_saas_connector_never_raises_primary_key_warnings(self, known: bool) -> None:
        """PagerDuty defines its own keys; unknown or empty Fivetran keys are not a gap,
        so the plan must not send the agent back to re-run discovery with --columns."""
        tables = [
            _table(
                source_schema="pagerduty",
                source_table=t,
                primary_keys=[],
                primary_keys_known=known,
            )
            for t in ("incidents", "services", "teams")
        ]
        plan = build_plan(_inventory(_connection(service="pagerduty", objects=tables)), "main_prod")
        item = plan["items"][0]
        assert len(item["objects"]) == 3
        assert all(o["table_configuration"]["scd_type"] == "SCD_TYPE_1" for o in item["objects"])
        assert all("primary_keys" not in o["table_configuration"] for o in item["objects"])
        assert not [w for w in item["warnings"] if "primary key" in w.lower()]
        assert not [w for w in item["warnings"] if "--columns" in w]

    def test_unknown_database_primary_keys_are_not_a_warning(self) -> None:
        plan = build_plan(
            _inventory(_connection(objects=[_table(primary_keys=[], primary_keys_known=False)])),
            "main_prod",
        )
        item = plan["items"][0]
        assert item["objects"][0]["table_configuration"]["scd_type"] == "SCD_TYPE_1"
        assert not [w for w in item["warnings"] if "primary key" in w.lower()]

    def test_confirmed_keyless_database_tables_get_one_setup_note(self) -> None:
        tables = [
            _table(source_table=t, primary_keys=[], primary_keys_known=True)
            for t in ("audit_log", "events")
        ] + [_table(source_table="Customers")]
        plan = build_plan(_inventory(_connection(objects=tables)), "main_prod")
        item = plan["items"][0]
        notes = [w for w in item["warnings"] if "primary key" in w.lower()]
        assert len(notes) == 1
        assert "2 table(s)" in notes[0] and "dbo.audit_log" in notes[0]
        assert "CDC instead of change tracking" in notes[0]
        by_table = {o["source_table"]: o["table_configuration"] for o in item["objects"]}
        assert by_table["Customers"]["primary_keys"] == ["Id"]
        assert by_table["audit_log"]["scd_type"] == "SCD_TYPE_1"

    def test_disabled_tables_are_skipped(self) -> None:
        plan = build_plan(
            _inventory(
                _connection(objects=[_table(), _table(source_table="Audit", enabled=False)])
            ),
            "main_prod",
        )
        assert len(plan["items"][0]["objects"]) == 1

    def test_paused_connections_are_excluded_by_default(self) -> None:
        plan = build_plan(_inventory(_connection(paused=True)), "main_prod")
        assert plan["items"] == []

    def test_paused_connections_can_be_included(self) -> None:
        plan = build_plan(_inventory(_connection(paused=True)), "main_prod", include_paused=True)
        assert len(plan["items"]) == 1

    def test_browser_oauth_source_is_a_warning_not_a_blocker(self) -> None:
        plan = build_plan(_inventory(_connection(service="hubspot")), "main_prod")
        item = plan["items"][0]
        assert item["blockers"] == []
        assert any("browser-based OAuth only" in w for w in item["warnings"])
        assert item["target"]["effort"] == "high"
        assert plan["summary"]["connections_blocked"] == 0
        assert plan["summary"]["connections_manual_sign_in"] == 1

    def test_conditional_warning_names_the_generated_auth_path(self) -> None:
        plan = build_plan(_inventory(_connection(service="salesforce")), "main_prod")
        item = plan["items"][0]
        assert item["target"]["preferred_auth"] == "OAUTH_MTLS"
        assert any("generated as OAUTH_MTLS" in w for w in item["warnings"])

    def test_source_without_a_connector_is_blocked_with_guidance(self) -> None:
        plan = build_plan(_inventory(_connection(service="s3")), "main_prod")
        assert "Auto Loader" in plan["items"][0]["blockers"][0]

    def test_private_networking_is_flagged(self) -> None:
        plan = build_plan(
            _inventory(_connection(networking_method="PrivateLink", objects=[_table()])),
            "main_prod",
        )
        assert any("PrivateLink" in w for w in plan["items"][0]["warnings"])

    def test_sql_server_defaults_to_integrated_cdc_without_a_gateway(self) -> None:
        plan = build_plan(_inventory(_connection(objects=[_table()])), "main_prod")
        target = plan["items"][0]["target"]
        assert target["gateway"] == "not_required"
        assert target["connector_type"] == "CDC"
        assert target["architecture"] == "integrated_cdc"
        assert plan["summary"]["gateways_required"] == 0
        assert plan["summary"]["integrated_cdc_pipelines"] == 1

    def test_sql_server_gateway_architecture_is_opt_in(self) -> None:
        plan = build_plan(
            _inventory(_connection(objects=[_table()])), "main_prod", sqlserver_arch="gateway"
        )
        target = plan["items"][0]["target"]
        assert target["gateway"] == "required"
        assert target["connector_type"] is None
        assert target["architecture"] == "gateway"
        assert plan["summary"]["gateways_required"] == 1
        assert plan["summary"]["integrated_cdc_pipelines"] == 0

    def test_integrated_cdc_captures_the_source_database_as_source_catalog(self) -> None:
        plan = build_plan(
            _inventory(_connection(config={"database": "ERPProd"}, objects=[_table()])),
            "main_prod",
        )
        assert plan["items"][0]["objects"][0]["source_catalog"] == "ERPProd"

    def test_integrated_cdc_warns_about_the_workspace_feature_flag(self) -> None:
        item = build_plan(_inventory(_connection(objects=[_table()])), "main_prod")["items"][0]
        assert any("integrated CDC connector must be enabled" in w for w in item["warnings"])
        assert item["blockers"] == []

    def test_postgres_still_uses_a_gateway(self) -> None:
        plan = build_plan(
            _inventory(_connection(service="postgres", objects=[_table()])), "main_prod"
        )
        target = plan["items"][0]["target"]
        assert target["gateway"] == "required"
        assert target["connector_type"] is None
        assert target["architecture"] == "gateway"

    def test_invalid_sqlserver_arch_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="sqlserver_arch"):
            build_plan(_inventory(_connection()), "main_prod", sqlserver_arch="nonsense")

    def test_every_migratable_pipeline_gets_a_job(self) -> None:
        # Databricks has no supported pipeline-level schedule, so the job count
        # tracks the migratable pipeline count exactly.
        plan = build_plan(
            _inventory(
                _connection(objects=[_table()]),
                _connection(id="second", service="postgres", objects=[_table()]),
            ),
            "main_prod",
        )
        assert plan["summary"]["jobs_required"] == 2

    def test_mar_is_joined_by_destination_schema(self) -> None:
        # The Platform Connector keys MAR by connection name, which is the
        # destination schema, not the REST API's connection id.
        plan = build_plan(
            _inventory(_connection(objects=[_table()])),
            "main_prod",
            mar={"by_connection": {"sales": 125000}},
        )
        assert plan["items"][0]["fivetran"]["monthly_mar"] == 125000


class TestSchedule:
    @pytest.mark.parametrize("minutes,expected", sorted(CRON_BY_MINUTES.items()))
    def test_known_frequencies_map_to_cron(self, minutes: int, expected: str) -> None:
        plan = build_plan(_inventory(_connection(sync_frequency_minutes=minutes)), "main_prod")
        schedule = plan["items"][0]["schedule"]
        assert schedule["mode"] == "cron"
        assert schedule["quartz_cron_expression"] == expected

    @pytest.mark.parametrize("minutes", [1, 5])
    def test_sub_five_minute_frequencies_become_continuous(self, minutes: int) -> None:
        plan = build_plan(_inventory(_connection(sync_frequency_minutes=minutes)), "main_prod")
        schedule = plan["items"][0]["schedule"]
        assert schedule["mode"] == "continuous"
        assert "thrash" in schedule["note"]

    def test_unmapped_frequency_falls_back_to_the_nearest(self) -> None:
        plan = build_plan(_inventory(_connection(sync_frequency_minutes=45)), "main_prod")
        schedule = plan["items"][0]["schedule"]
        assert schedule["mode"] == "cron"
        assert "nearest" in schedule["note"]

    def test_concurrency_is_pinned_to_match_fivetran_semantics(self) -> None:
        # Fivetran serialises syncs; an unbounded cron job would not.
        plan = build_plan(_inventory(_connection()), "main_prod")
        assert plan["items"][0]["schedule"]["max_concurrent_runs"] == 1

    def test_missing_frequency_uses_the_fivetran_default(self) -> None:
        plan = build_plan(_inventory(_connection(sync_frequency_minutes=None)), "main_prod")
        assert plan["items"][0]["schedule"]["quartz_cron_expression"] == CRON_BY_MINUTES[360]


class TestSupportedTableFiltering:
    """Verify that unsupported Fivetran tables are excluded from the plan."""

    def test_supported_tables_pass_through(self) -> None:
        plan = build_plan(
            _inventory(
                _connection(
                    service="pagerduty",
                    objects=[
                        _table(source_table="incidents"),
                        _table(source_table="teams"),
                    ],
                )
            ),
            "main_prod",
        )
        tables = [o["source_table"] for o in plan["items"][0]["objects"]]
        assert tables == ["incidents", "teams"]

    def test_unsupported_tables_are_excluded_with_warning(self) -> None:
        plan = build_plan(
            _inventory(
                _connection(
                    service="pagerduty",
                    objects=[
                        _table(source_table="incidents"),
                        _table(source_table="extension"),
                        _table(source_table="extension_schema_option"),
                    ],
                )
            ),
            "main_prod",
        )
        item = plan["items"][0]
        tables = [o["source_table"] for o in item["objects"]]
        assert "incidents" in tables
        assert "extension" not in tables
        assert "extension_schema_option" not in tables
        assert any("no Lakeflow Connect equivalent" in w for w in item["warnings"])
        assert any("extension" in w for w in item["warnings"])

    def test_all_tables_unsupported_leaves_empty_objects_list(self) -> None:
        plan = build_plan(
            _inventory(
                _connection(
                    service="pagerduty",
                    objects=[
                        _table(source_table="fake_table_1"),
                        _table(source_table="fake_table_2"),
                    ],
                )
            ),
            "main_prod",
        )
        item = plan["items"][0]
        assert len(item["objects"]) == 0
        assert any("2 Fivetran table(s)" in w for w in item["warnings"])

    def test_tables_dropped_counted_in_summary(self) -> None:
        plan = build_plan(
            _inventory(
                _connection(
                    service="pagerduty",
                    objects=[
                        _table(source_table="incidents"),
                        _table(source_table="extension"),
                    ],
                )
            ),
            "main_prod",
        )
        assert plan["summary"]["tables_dropped"] >= 1

    def test_connector_without_supported_tables_includes_all(self) -> None:
        """Connectors with supported_tables=None (dynamic table set) include
        all Fivetran tables without filtering."""
        plan = build_plan(
            _inventory(
                _connection(
                    service="sql_server",
                    objects=[
                        _table(source_table="Customers"),
                        _table(source_table="AnyCustomTable"),
                    ],
                )
            ),
            "main_prod",
        )
        tables = [o["source_table"] for o in plan["items"][0]["objects"]]
        assert "Customers" in tables
        assert "AnyCustomTable" in tables

    def test_table_filtering_is_case_insensitive(self) -> None:
        plan = build_plan(
            _inventory(
                _connection(
                    service="pagerduty",
                    objects=[_table(source_table="Incidents")],
                )
            ),
            "main_prod",
        )
        assert len(plan["items"][0]["objects"]) == 1

    def test_docs_url_included_in_dropped_table_warning(self) -> None:
        plan = build_plan(
            _inventory(
                _connection(
                    service="pagerduty",
                    objects=[_table(source_table="extension")],
                )
            ),
            "main_prod",
        )
        dropped_warnings = [
            w for w in plan["items"][0]["warnings"]
            if "no Lakeflow Connect equivalent" in w
        ]
        assert any("pagerduty-reference" in w for w in dropped_warnings)


class TestCatalogSupportedTables:
    """Verify that supported_tables is populated for key connectors."""

    def test_pagerduty_has_supported_tables(self) -> None:
        target = lookup("pagerduty")
        assert target.supported_tables is not None
        assert "incidents" in target.supported_tables
        assert len(target.supported_tables) == 10

    def test_hubspot_has_supported_tables(self) -> None:
        target = lookup("hubspot")
        assert target.supported_tables is not None
        assert "contacts" in target.supported_tables

    def test_zendesk_has_supported_tables(self) -> None:
        target = lookup("zendesk")
        assert target.supported_tables is not None
        assert "tickets" in target.supported_tables

    def test_jira_has_supported_tables(self) -> None:
        target = lookup("jira")
        assert target.supported_tables is not None
        assert "issues" in target.supported_tables

    def test_github_has_supported_tables(self) -> None:
        target = lookup("github")
        assert target.supported_tables is not None
        assert "pull_requests" in target.supported_tables

    def test_database_connectors_have_no_supported_tables(self) -> None:
        for service in ("sql_server", "postgres", "mysql", "oracle"):
            target = lookup(service)
            assert target.supported_tables is None, (
                f"{service} should have supported_tables=None (user-defined tables)"
            )

    def test_salesforce_has_no_supported_tables(self) -> None:
        target = lookup("salesforce")
        assert target.supported_tables is None

    def test_all_managed_connectors_have_docs_url_or_notes(self) -> None:
        for service, target in CATALOG.items():
            if target.has_managed_connector:
                has_context = (
                    target.docs_url
                    or target.notes
                    or target.source_prerequisites
                    or target.auth_note
                )
                assert has_context, (
                    f"{service} ({target.connection_type}) has no docs_url, notes, "
                    "auth_note, or source_prerequisites"
                )


def _pagerduty(connection_id: str = "pd1") -> dict:
    return _connection(
        id=connection_id,
        service="pagerduty",
        objects=[_table(source_schema="pagerduty", source_table="incidents")],
    )


def _availability_warnings(item: dict) -> list[str]:
    return [w for w in item["warnings"] if "Beta" in w or "Public Preview" in w]


class TestAvailabilityWording:
    def test_beta_points_at_the_workspace_previews_page_not_the_account_team(self) -> None:
        item = build_plan(_inventory(_pagerduty()), "main")["items"][0]
        (warning,) = _availability_warnings(item)
        assert "Previews page" in warning
        assert "account team" not in warning
        assert "confirm" in warning

    def test_public_preview_keeps_the_account_team_note(self) -> None:
        item = build_plan(_inventory(_connection(service="postgres", objects=[_table()])), "main")[
            "items"
        ][0]
        (warning,) = _availability_warnings(item)
        assert "account team" in warning


class TestConnectionReuse:
    def test_reused_connection_name_replaces_the_generated_one(self) -> None:
        plan = build_plan(
            _inventory(_pagerduty()), "main", existing_connections={"pagerduty": "pd_prod"}
        )
        target = plan["items"][0]["target"]
        assert target["connection_name"] == "pd_prod"
        assert target["connection_source"] == "existing"
        assert target["connection_verified"] is False
        assert plan["summary"]["connections_reused"] == 1

    def test_connection_id_takes_precedence_over_service(self) -> None:
        plan = build_plan(
            _inventory(_pagerduty("pd1"), _pagerduty("pd2")),
            "main",
            existing_connections={"pagerduty": "pd_shared", "pd2": "pd_other"},
        )
        names = [i["target"]["connection_name"] for i in plan["items"]]
        assert names == ["pd_shared", "pd_other"]

    def test_unverified_reuse_softens_but_keeps_the_beta_note(self) -> None:
        item = build_plan(
            _inventory(_pagerduty()), "main", existing_connections={"pagerduty": "pd_prod"}
        )["items"][0]
        (warning,) = _availability_warnings(item)
        assert "pd_prod" in warning and "--databricks-profile" in warning

    def test_verified_reuse_drops_the_beta_note(self) -> None:
        item = build_plan(
            _inventory(_pagerduty()),
            "main",
            existing_connections={"pagerduty": "pd_prod"},
            lookup_connection=lambda name: {"name": name, "connection_type": "PAGERDUTY"},
        )["items"][0]
        assert item["target"]["connection_verified"] is True
        assert _availability_warnings(item) == []
        assert item["blockers"] == []

    def test_missing_connection_is_a_blocker(self) -> None:
        item = build_plan(
            _inventory(_pagerduty()),
            "main",
            existing_connections={"pagerduty": "pd_typo"},
            lookup_connection=lambda name: None,
        )["items"][0]
        assert any("does not exist" in b for b in item["blockers"])
        # The check already ran, so the note must not ask for it again.
        (warning,) = _availability_warnings(item)
        assert "--databricks-profile" not in warning

    def test_wrong_connection_type_is_a_blocker(self) -> None:
        item = build_plan(
            _inventory(_pagerduty()),
            "main",
            existing_connections={"pagerduty": "sf_prod"},
            lookup_connection=lambda name: {"name": name, "connection_type": "SALESFORCE"},
        )["items"][0]
        assert any("is type SALESFORCE" in b for b in item["blockers"])

    def test_api_source_connection_matches_on_source_name(self) -> None:
        # The shape the PagerDuty ingestion wizard actually stores in Unity Catalog.
        item = build_plan(
            _inventory(_pagerduty()),
            "main",
            existing_connections={"pagerduty": "pd_prod"},
            lookup_connection=lambda name: {
                "name": name,
                "connection_type": "API_SOURCE",
                "options": {"source_name": "pagerduty", "region": "us"},
            },
        )["items"][0]
        assert item["blockers"] == []
        assert item["target"]["connection_verified"] is True
        assert _availability_warnings(item) == []

    def test_api_source_for_another_connector_is_a_blocker(self) -> None:
        item = build_plan(
            _inventory(_pagerduty()),
            "main",
            existing_connections={"pagerduty": "other"},
            lookup_connection=lambda name: {
                "name": name,
                "connection_type": "API_SOURCE",
                "options": {"source_name": "jira"},
            },
        )["items"][0]
        assert any("API_SOURCE (source_name=jira)" in b for b in item["blockers"])

    def test_unmatched_key_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="pagerdooty"):
            build_plan(_inventory(_pagerduty()), "main", existing_connections={"pagerdooty": "pd"})

    def test_reused_browser_oauth_connection_is_not_high_effort_or_manual(self) -> None:
        plan = build_plan(
            _inventory(_connection(service="hubspot", objects=[_table()])),
            "main",
            existing_connections={"hubspot": "hubspot_prod"},
        )
        item = plan["items"][0]
        assert item["target"]["effort"] == "low"
        assert not [w for w in item["warnings"] if "browser-based OAuth" in w]
        assert plan["summary"]["connections_manual_sign_in"] == 0

    def test_reused_saas_connection_drops_credential_setup_effort_and_warnings(self) -> None:
        item = build_plan(
            _inventory(_connection(service="salesforce", objects=[_table()])),
            "main",
            existing_connections={"salesforce": "sf_prod"},
        )["items"][0]
        assert item["target"]["effort"] == "low"
        assert not [w for w in item["warnings"] if "Conditionally scriptable" in w]

    def test_reused_database_connection_keeps_source_setup(self) -> None:
        item = build_plan(
            _inventory(_connection(service="postgres", objects=[_table()])),
            "main",
            existing_connections={"postgres": "pg_prod"},
        )["items"][0]
        assert item["target"]["effort"] == lookup("postgres").effort.value
