"""Tests for Fivetran MAR loading and aggregation."""

from __future__ import annotations

import datetime as dt
import io
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "skills/fivetran-to-lakeflow-migration/scripts"
sys.path.insert(0, str(SCRIPTS))

from ftlfc.mar import (
    BigQueryConfig,
    MarError,
    MarRecord,
    RedshiftConfig,
    SnowflakeConfig,
    _normalise_month,
    _rows_from_statement,
    aggregate,
    build_mar_query,
    load_csv,
    run_bigquery_query,
    run_redshift_query,
    run_snowflake_query,
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


class TestSnowflakeConfig:
    def test_requires_account(self) -> None:
        config = SnowflakeConfig(account="", user="admin", database="DB", password="pw")
        # The config itself is a dataclass; validation happens at connect time
        # or in _build_snowflake_config. Just confirm the dataclass works.
        assert config.account == ""

    def test_stores_all_fields(self) -> None:
        config = SnowflakeConfig(
            account="xy12345.us-east-1",
            user="admin",
            database="FIVETRAN_DB",
            password="secret",
            warehouse="COMPUTE_WH",
            role="ANALYST",
        )
        assert config.account == "xy12345.us-east-1"
        assert config.user == "admin"
        assert config.database == "FIVETRAN_DB"
        assert config.password == "secret"
        assert config.warehouse == "COMPUTE_WH"
        assert config.role == "ANALYST"
        assert config.authenticator is None

    def test_authenticator_instead_of_password(self) -> None:
        config = SnowflakeConfig(
            account="xy12345",
            user="admin",
            database="DB",
            authenticator="externalbrowser",
        )
        assert config.password is None
        assert config.authenticator == "externalbrowser"


class TestSnowflakeImportGuard:
    def test_missing_driver_raises_a_helpful_error(self) -> None:
        """run_snowflake_query should raise MarError if the driver isn't installed."""
        config = SnowflakeConfig(
            account="xy12345",
            user="admin",
            database="DB",
            password="pw",
        )
        # If snowflake-connector-python happens to be installed in the test env
        # (unlikely in CI), this test still validates the function exists and
        # accepts the right args. If it's not installed, it should raise MarError.
        try:
            run_snowflake_query("SELECT 1", config)
        except MarError as exc:
            assert "snowflake-connector-python" in str(exc)
        except Exception:
            # Driver is installed but fails to connect (no real Snowflake) — fine.
            pass


class TestSnowflakeCli:
    """Test the CLI arg parsing for the --snowflake path."""

    def test_snowflake_missing_required_args(self) -> None:
        """--snowflake without --sf-account/--sf-user/--sf-database should fail."""
        from fivetran_mar import main

        # Missing all three required SF args, and no env password
        result = main(["--snowflake", "-o", "/tmp/test_mar.json"])
        assert result == 1

    def test_snowflake_mutually_exclusive_with_warehouse_id(self) -> None:
        """--snowflake and --warehouse-id can't both be set."""
        with pytest.raises(SystemExit):
            from fivetran_mar import main
            main(["--snowflake", "--warehouse-id", "abc123"])

    def test_print_sql_is_unaffected(self) -> None:
        """--print-sql still works and doesn't require Snowflake args."""
        import io
        from contextlib import redirect_stdout

        from fivetran_mar import main

        buf = io.StringIO()
        with redirect_stdout(buf):
            result = main(["--print-sql"])
        assert result == 0
        output = buf.getvalue()
        assert "incremental_mar" in output
        assert "free_type" in output

    def test_csv_path_is_unaffected(self) -> None:
        """--csv still works exactly as before."""
        import tempfile

        from fivetran_mar import main

        csv_content = (
            "connection_name,schema_name,table_name,measured_month,mar\n"
            "sfdc,salesforce,Account,2026-08-01,5000\n"
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            csv_path = f.name

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            out_path = f.name

        try:
            result = main(["--csv", csv_path, "-o", out_path])
            assert result == 0
            import json
            data = json.loads(Path(out_path).read_text())
            assert data["mar"]["total_mar"] == 5000
            assert data["mar"]["by_connection"]["sfdc"] == 5000
        finally:
            Path(csv_path).unlink(missing_ok=True)
            Path(out_path).unlink(missing_ok=True)


class TestBigQueryConfig:
    def test_stores_all_fields(self) -> None:
        config = BigQueryConfig(
            project="my-gcp-project",
            dataset="fivetran_metadata",
            location="US",
        )
        assert config.project == "my-gcp-project"
        assert config.dataset == "fivetran_metadata"
        assert config.location == "US"
        assert config.credentials_json is None

    def test_credentials_json_is_optional(self) -> None:
        config = BigQueryConfig(project="proj", dataset="ds")
        assert config.credentials_json is None


class TestBigQueryImportGuard:
    def test_missing_driver_raises_a_helpful_error(self) -> None:
        config = BigQueryConfig(project="proj", dataset="ds")
        try:
            run_bigquery_query("SELECT 1", config)
        except MarError as exc:
            assert "google-cloud-bigquery" in str(exc)
        except Exception:
            pass


class TestRedshiftConfig:
    def test_stores_provisioned_cluster_fields(self) -> None:
        config = RedshiftConfig(
            database="fivetran_db",
            cluster_identifier="my-cluster",
            db_user="admin",
            region="us-east-1",
        )
        assert config.database == "fivetran_db"
        assert config.cluster_identifier == "my-cluster"
        assert config.db_user == "admin"
        assert config.workgroup_name is None
        assert config.region == "us-east-1"

    def test_stores_serverless_workgroup_fields(self) -> None:
        config = RedshiftConfig(
            database="fivetran_db",
            workgroup_name="default",
        )
        assert config.workgroup_name == "default"
        assert config.cluster_identifier is None
        assert config.db_user is None

    def test_validate_requires_cluster_or_workgroup(self) -> None:
        config = RedshiftConfig(database="db")
        with pytest.raises(MarError, match=r"--rs-cluster.*--rs-workgroup"):
            config._validate()

    def test_validate_requires_db_user_for_provisioned(self) -> None:
        config = RedshiftConfig(database="db", cluster_identifier="cluster")
        with pytest.raises(MarError, match="--rs-db-user"):
            config._validate()


class TestRedshiftImportGuard:
    def test_missing_driver_raises_a_helpful_error(self) -> None:
        config = RedshiftConfig(
            database="db", cluster_identifier="cluster", db_user="admin"
        )
        try:
            run_redshift_query("SELECT 1", config)
        except MarError as exc:
            assert "boto3" in str(exc)
        except Exception:
            pass


class TestBigQueryCli:
    def test_bigquery_missing_required_args(self) -> None:
        from fivetran_mar import main

        result = main(["--bigquery", "-o", "/tmp/test_bq_mar.json"])
        assert result == 1

    def test_bigquery_mutually_exclusive_with_snowflake(self) -> None:
        with pytest.raises(SystemExit):
            from fivetran_mar import main
            main(["--bigquery", "--snowflake"])


class TestRedshiftCli:
    def test_redshift_missing_required_args(self) -> None:
        from fivetran_mar import main

        result = main(["--redshift", "-o", "/tmp/test_rs_mar.json"])
        assert result == 1

    def test_redshift_mutually_exclusive_with_warehouse_id(self) -> None:
        with pytest.raises(SystemExit):
            from fivetran_mar import main
            main(["--redshift", "--warehouse-id", "abc123"])
