"""Tests for Fivetran MAR loading and aggregation."""

from __future__ import annotations

import datetime as dt
import io
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "skills/fivetran-to-lakeflow-migration/scripts"
sys.path.insert(0, str(SCRIPTS))

from ftlfc.mar import (  # noqa: E402
    MarError,
    MarRecord,
    _normalise_month,
    _rows_from_statement,
    aggregate,
    build_mar_query,
    load_csv,
)


class TestQuery:
    def test_filters_to_paid_only(self) -> None:
        # free_type is part of the primary key, so omitting this filter double
        # counts non-billable volume and inflates the savings estimate.
        assert "free_type = 'PAID'" in build_mar_query("fivetran_metadata")

    def test_targets_the_requested_schema(self) -> None:
        assert "fivetran_log.incremental_mar" in build_mar_query("fivetran_log")

    def test_window_follows_the_months_argument(self) -> None:
        recent = build_mar_query("fivetran_metadata", months=1)
        older = build_mar_query("fivetran_metadata", months=12)
        assert recent != older


class TestNormaliseMonth:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("2026-08", "2026-08"),
            ("2026-08-01", "2026-08"),
            ("2026-08-01T00:00:00Z", "2026-08"),
            (dt.date(2026, 8, 1), "2026-08"),
            (dt.datetime(2026, 8, 1, 12, 30), "2026-08"),
            ("", ""),
            (None, ""),
        ],
    )
    def test_coerces_known_shapes(self, value, expected) -> None:
        assert _normalise_month(value) == expected

    def test_rejects_unparseable(self) -> None:
        with pytest.raises(ValueError):
            _normalise_month("August 2026")


class TestLoadCsv:
    def test_parses_a_well_formed_export(self) -> None:
        csv_text = (
            "connection_name,schema_name,table_name,measured_month,mar\n"
            "salesforce_prod,salesforce,Account,2026-08-01,15000\n"
            "salesforce_prod,salesforce,Contact,2026-08-01,4200\n"
        )
        records = load_csv(io.StringIO(csv_text))
        assert len(records) == 2
        assert records[0].connection_name == "salesforce_prod"
        assert records[0].measured_month == "2026-08"
        assert records[0].mar == 15000
        assert records[0].qualified_table == "salesforce.Account"

    def test_matches_columns_case_insensitively(self) -> None:
        # Snowflake upper-cases identifiers, so exports arrive shouting.
        csv_text = (
            "CONNECTION_NAME,SCHEMA_NAME,TABLE_NAME,MEASURED_MONTH,MAR\n"
            "pg_prod,public,orders,2026-07-01,900\n"
        )
        records = load_csv(io.StringIO(csv_text))
        assert records[0].connection_name == "pg_prod"
        assert records[0].mar == 900

    def test_tolerates_float_formatted_counts(self) -> None:
        csv_text = (
            "connection_name,schema_name,table_name,measured_month,mar\n"
            "pg_prod,public,orders,2026-07-01,900.0\n"
        )
        assert load_csv(io.StringIO(csv_text))[0].mar == 900

    def test_names_the_missing_columns(self) -> None:
        csv_text = "connection_name,mar\nfoo,1\n"
        with pytest.raises(MarError, match="schema_name"):
            load_csv(io.StringIO(csv_text))

    def test_rejects_an_empty_export(self) -> None:
        with pytest.raises(MarError, match="empty"):
            load_csv(io.StringIO(""))


class TestRowsFromStatement:
    def test_zips_columns_to_rows(self) -> None:
        response = {
            "status": {"state": "SUCCEEDED"},
            "manifest": {"schema": {"columns": [{"name": "a"}, {"name": "b"}]}},
            "result": {"data_array": [["1", "2"], ["3", "4"]]},
        }
        assert _rows_from_statement(response) == [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]

    def test_raises_on_a_failed_statement(self) -> None:
        response = {"status": {"state": "FAILED", "error": {"message": "table not found"}}}
        with pytest.raises(MarError, match="table not found"):
            _rows_from_statement(response)

    def test_raises_rather_than_under_reporting_a_truncated_result(self) -> None:
        response = {
            "status": {"state": "SUCCEEDED"},
            "manifest": {"schema": {"columns": [{"name": "a"}]}},
            "result": {"data_array": [["1"]], "truncated": True},
        }
        with pytest.raises(MarError, match="truncated"):
            _rows_from_statement(response)

    def test_handles_an_empty_result(self) -> None:
        response = {
            "status": {"state": "SUCCEEDED"},
            "manifest": {"schema": {"columns": []}},
            "result": {},
        }
        assert _rows_from_statement(response) == []


class TestAggregate:
    @pytest.fixture
    def summary(self) -> dict:
        records = [
            MarRecord("sfdc", "salesforce", "Account", "2026-06", 1000),
            MarRecord("sfdc", "salesforce", "Account", "2026-07", 2000),
            MarRecord("sfdc", "salesforce", "Contact", "2026-07", 500),
            MarRecord("pg", "public", "orders", "2026-07", 300),
        ]
        return aggregate(records)

    def test_totals_across_every_grain(self, summary: dict) -> None:
        assert summary["total_mar"] == 3800
        assert summary["months"] == ["2026-06", "2026-07"]
        assert summary["month_count"] == 2

    def test_latest_month_is_the_comparison_basis(self, summary: dict) -> None:
        assert summary["latest_month"] == "2026-07"
        assert summary["latest_month_mar"] == 2800

    def test_ranks_connections_by_volume(self, summary: dict) -> None:
        assert list(summary["by_connection"]) == ["sfdc", "pg"]
        assert summary["by_connection"]["sfdc"] == 3500

    def test_table_grain_is_namespaced_by_connection(self, summary: dict) -> None:
        assert summary["by_table"]["sfdc|salesforce.Account"] == 3000

    def test_monthly_series_is_chronological(self, summary: dict) -> None:
        assert summary["monthly"] == {"2026-06": 1000, "2026-07": 2800}

    def test_mean_uses_months_present(self, summary: dict) -> None:
        assert summary["mean_monthly_mar"] == 1900

    def test_empty_input_is_not_an_error(self) -> None:
        assert aggregate([])["total_mar"] == 0
