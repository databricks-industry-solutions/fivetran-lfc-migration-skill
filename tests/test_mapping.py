"""Tests for the connector catalog and migration plan builder."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "skills/fivetran-to-lakeflow-migration/scripts"
sys.path.insert(0, str(SCRIPTS))

from ftlfc.catalog import Category, Effort, Gateway, Scriptable, lookup  # noqa: E402
from ftlfc.mapping import CRON_BY_MINUTES, build_plan, to_identifier  # noqa: E402


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

    def test_workday_is_scriptable_but_needs_manual_source_setup(self) -> None:
        target = lookup("workday")
        assert target.scriptable is Scriptable.YES
        assert target.prerequisites_automatable is False
        # Scriptable connection, un-scriptable source setup: still needs a human.
        assert target.effort is Effort.HIGH

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

    def test_unknown_primary_keys_warn_and_do_not_force_append_only(self) -> None:
        plan = build_plan(
            _inventory(
                _connection(objects=[_table(primary_keys=[], primary_keys_known=False)])
            ),
            "main_prod",
        )
        item = plan["items"][0]
        # Unknown must not be treated as "no primary key".
        assert item["objects"][0]["table_configuration"]["scd_type"] == "SCD_TYPE_1"
        assert any("unknown primary keys" in w.lower() for w in item["warnings"])

    def test_confirmed_absence_of_a_primary_key_becomes_append_only(self) -> None:
        plan = build_plan(
            _inventory(_connection(objects=[_table(primary_keys=[], primary_keys_known=True)])),
            "main_prod",
        )
        assert plan["items"][0]["objects"][0]["table_configuration"]["scd_type"] == "APPEND_ONLY"

    def test_disabled_tables_are_skipped(self) -> None:
        plan = build_plan(
            _inventory(_connection(objects=[_table(), _table(source_table="Audit", enabled=False)])),
            "main_prod",
        )
        assert len(plan["items"][0]["objects"]) == 1

    def test_paused_connections_are_excluded_by_default(self) -> None:
        plan = build_plan(_inventory(_connection(paused=True)), "main_prod")
        assert plan["items"] == []

    def test_paused_connections_can_be_included(self) -> None:
        plan = build_plan(_inventory(_connection(paused=True)), "main_prod", include_paused=True)
        assert len(plan["items"]) == 1

    def test_browser_oauth_source_is_blocked(self) -> None:
        plan = build_plan(_inventory(_connection(service="hubspot")), "main_prod")
        assert plan["items"][0]["blockers"]
        assert plan["summary"]["connections_blocked"] == 1

    def test_source_without_a_connector_is_blocked_with_guidance(self) -> None:
        plan = build_plan(_inventory(_connection(service="s3")), "main_prod")
        assert "Auto Loader" in plan["items"][0]["blockers"][0]

    def test_private_networking_is_flagged(self) -> None:
        plan = build_plan(
            _inventory(_connection(networking_method="PrivateLink", objects=[_table()])), "main_prod"
        )
        assert any("PrivateLink" in w for w in plan["items"][0]["warnings"])

    def test_gateway_requirement_is_counted(self) -> None:
        plan = build_plan(_inventory(_connection(objects=[_table()])), "main_prod")
        assert plan["summary"]["gateways_required"] == 1

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
