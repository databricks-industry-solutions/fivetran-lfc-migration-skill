"""Tests for ftlfc.system_tables — Databricks system table telemetry."""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / "skills/fivetran-to-lakeflow-migration/scripts"
sys.path.insert(0, str(SCRIPTS))

from ftlfc.costs import RateCard
from ftlfc.system_tables import (
    build_list_prices_query,
    build_run_duration_query,
    build_usage_query,
    enrich_rate_card,
    estimate_gateway_dbu_per_hour,
    estimate_minutes_per_run,
    extract_dbu_rates,
)


class TestBuildQueries:
    def test_list_prices_query_contains_sku_patterns(self):
        sql = build_list_prices_query()
        assert "JOBS_SERVERLESS_COMPUTE" in sql
        assert "DLT_ADVANCED_COMPUTE" in sql
        assert "price_end_time IS NULL" in sql

    def test_usage_query_contains_since_date(self):
        sql = build_usage_query(months=3)
        assert "usage_date >=" in sql
        assert "system.billing.usage" in sql
        assert "system.lakeflow.pipelines" in sql

    def test_run_duration_query_contains_pipeline_filter(self):
        sql = build_run_duration_query(months=6)
        assert "INGESTION_PIPELINE" in sql
        assert "JOBS_SERVERLESS_COMPUTE" in sql


class TestExtractDbuRates:
    def test_extracts_both_skus(self):
        prices = {
            "ENTERPRISE_JOBS_SERVERLESS_COMPUTE_US_EAST_1": 0.42,
            "ENTERPRISE_DLT_ADVANCED_COMPUTE_US_EAST_1": 0.33,
            "STANDARD_ALL_PURPOSE_COMPUTE": 0.55,
        }
        rates = extract_dbu_rates(prices)
        assert rates["serverless_dbu_usd"] == 0.42
        assert rates["gateway_dbu_usd"] == 0.33

    def test_first_match_wins(self):
        prices = {
            "ENTERPRISE_JOBS_SERVERLESS_COMPUTE_US_EAST_1": 0.42,
            "ENTERPRISE_JOBS_SERVERLESS_COMPUTE_US_WEST_2": 0.39,
        }
        rates = extract_dbu_rates(prices)
        assert rates["serverless_dbu_usd"] == 0.42

    def test_empty_prices_returns_empty(self):
        assert extract_dbu_rates({}) == {}

    def test_no_matching_sku(self):
        prices = {"STANDARD_ALL_PURPOSE_COMPUTE": 0.55}
        rates = extract_dbu_rates(prices)
        assert "serverless_dbu_usd" not in rates
        assert "gateway_dbu_usd" not in rates


class TestEstimateMinutesPerRun:
    def test_returns_none_for_empty(self):
        assert estimate_minutes_per_run([]) is None

    def test_calculates_from_avg_daily_runs(self):
        rows = [
            {"pipeline_id": "p1", "avg_daily_dbus": 2.0, "avg_daily_runs": 4.0},
        ]
        minutes = estimate_minutes_per_run(rows)
        assert minutes == 30.0  # (2.0/4.0) * 60

    def test_override_schedule_runs_per_day(self):
        rows = [
            {"pipeline_id": "p1", "avg_daily_dbus": 1.0, "avg_daily_runs": 10.0},
        ]
        minutes = estimate_minutes_per_run(rows, schedule_runs_per_day=2.0)
        assert minutes == 30.0  # (1.0/2.0) * 60

    def test_aggregates_across_pipelines(self):
        rows = [
            {"pipeline_id": "p1", "avg_daily_dbus": 1.0, "avg_daily_runs": 2.0},
            {"pipeline_id": "p2", "avg_daily_dbus": 3.0, "avg_daily_runs": 6.0},
        ]
        minutes = estimate_minutes_per_run(rows)
        # total dbus = 4.0, total runs = 8.0, avg = 0.5 * 60 = 30.0
        assert minutes == 30.0

    def test_returns_none_for_zero_runs(self):
        rows = [
            {"pipeline_id": "p1", "avg_daily_dbus": 1.0, "avg_daily_runs": 0.0},
        ]
        assert estimate_minutes_per_run(rows) is None

    def test_handles_bad_data(self):
        rows = [
            {"pipeline_id": "p1", "avg_daily_dbus": "bad", "avg_daily_runs": 4.0},
        ]
        assert estimate_minutes_per_run(rows) is None


class TestEstimateGatewayDbuPerHour:
    def test_returns_none_for_empty(self):
        assert estimate_gateway_dbu_per_hour([]) is None

    def test_returns_none_for_no_gateway_rows(self):
        rows = [
            {"pipeline_type": "INGESTION_PIPELINE", "usage_date": "2025-01-01", "dbus": 10},
        ]
        assert estimate_gateway_dbu_per_hour(rows) is None

    def test_calculates_from_daily_dbus(self):
        rows = [
            {"pipeline_type": "INGESTION_GATEWAY", "usage_date": "2025-01-01", "dbus": 48},
            {"pipeline_type": "INGESTION_GATEWAY", "usage_date": "2025-01-02", "dbus": 24},
        ]
        result = estimate_gateway_dbu_per_hour(rows)
        # avg daily = (48+24)/2 = 36, hourly = 36/24 = 1.5
        assert result == 1.5

    def test_ignores_ingestion_pipeline_rows(self):
        rows = [
            {"pipeline_type": "INGESTION_GATEWAY", "usage_date": "2025-01-01", "dbus": 24},
            {"pipeline_type": "INGESTION_PIPELINE", "usage_date": "2025-01-01", "dbus": 100},
        ]
        result = estimate_gateway_dbu_per_hour(rows)
        assert result == 1.0  # 24/24


class TestEnrichRateCard:
    def test_no_data_returns_same_card(self):
        base = RateCard()
        enriched, log = enrich_rate_card(base)
        assert enriched is base
        assert len(log["grounded_fields"]) == 0
        assert len(log["unchanged_fields"]) == 4

    def test_prices_override_defaults(self):
        base = RateCard()
        prices = {
            "ENTERPRISE_JOBS_SERVERLESS_COMPUTE_US_EAST_1": 0.42,
            "ENTERPRISE_DLT_ADVANCED_COMPUTE_US_EAST_1": 0.33,
        }
        enriched, log = enrich_rate_card(base, prices=prices)
        assert enriched.serverless_dbu_usd == 0.42
        assert enriched.gateway_dbu_usd == 0.33
        assert len(log["grounded_fields"]) == 2
        assert "system.billing.list_prices" in log["sources"]

    def test_run_duration_overrides(self):
        base = RateCard()
        durations = [
            {"pipeline_id": "p1", "avg_daily_dbus": 2.0, "avg_daily_runs": 4.0},
        ]
        enriched, log = enrich_rate_card(base, run_durations=durations)
        assert enriched.minutes_per_run == 30.0
        assert any("minutes_per_run" in f for f in log["grounded_fields"])

    def test_gateway_usage_overrides(self):
        base = RateCard()
        usage = [
            {"pipeline_type": "INGESTION_GATEWAY", "usage_date": "2025-01-01", "dbus": 48},
        ]
        enriched, log = enrich_rate_card(base, usage_rows=usage)
        assert enriched.gateway_dbu_per_hour == 2.0  # 48/24
        assert any("gateway_dbu_per_hour" in f for f in log["grounded_fields"])

    def test_all_sources_combined(self):
        base = RateCard()
        prices = {"ENTERPRISE_JOBS_SERVERLESS_COMPUTE_US_EAST_1": 0.40}
        usage = [
            {"pipeline_type": "INGESTION_GATEWAY", "usage_date": "2025-01-01", "dbus": 72},
        ]
        durations = [
            {"pipeline_id": "p1", "avg_daily_dbus": 1.5, "avg_daily_runs": 3.0},
        ]
        enriched, log = enrich_rate_card(base, prices, usage, durations)
        assert enriched.serverless_dbu_usd == 0.40
        assert enriched.minutes_per_run == 30.0
        assert enriched.gateway_dbu_per_hour == 3.0  # 72/24
        assert len(log["grounded_fields"]) == 3
        assert len(log["unchanged_fields"]) == 1  # gateway_dbu_usd still default

    def test_unchanged_fields_tracked(self):
        base = RateCard()
        prices = {"ENTERPRISE_JOBS_SERVERLESS_COMPUTE_US_EAST_1": 0.40}
        _, log = enrich_rate_card(base, prices=prices)
        unchanged_names = [f.split(":")[0] for f in log["unchanged_fields"]]
        assert "gateway_dbu_usd" in unchanged_names
        assert "minutes_per_run" in unchanged_names
        assert "gateway_dbu_per_hour" in unchanged_names

    def test_preserves_non_enriched_fields(self):
        base = RateCard(infra_uplift=1.0, fivetran_usd_per_million_mar=600)
        prices = {"ENTERPRISE_JOBS_SERVERLESS_COMPUTE_US_EAST_1": 0.40}
        enriched, _ = enrich_rate_card(base, prices=prices)
        assert enriched.infra_uplift == 1.0
        assert enriched.fivetran_usd_per_million_mar == 600
