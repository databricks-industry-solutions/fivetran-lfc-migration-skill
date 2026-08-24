"""Tests for the Fivetran vs Lakeflow Connect cost model."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "skills/fivetran-to-lakeflow-migration/scripts"
sys.path.insert(0, str(SCRIPTS))

from ftlfc.costs import (  # noqa: E402
    HOURS_PER_MONTH,
    RateCard,
    compare,
    fivetran_monthly_cost,
    measured_lakeflow_cost,
    model_lakeflow_cost,
    rate_card_from_overrides,
    runs_per_month,
)


def _plan(*items, blocked: int = 0, scd2: int = 0, gateways: int = 0) -> dict:
    item_list = list(items)
    return {
        "items": item_list,
        "summary": {
            "connections_total": len(item_list) + blocked,
            "connections_blocked": blocked,
            "tables_scd2": scd2,
            "gateways_required": gateways,
        },
    }


def _item(minutes: int = 60, gateway: str = "not_required", blockers=None) -> dict:
    return {
        "blockers": blockers or [],
        "target": {"gateway": gateway},
        "schedule": {"mode": "cron", "fivetran_minutes": minutes},
    }


class TestFivetranCost:
    def test_models_from_mar_and_connection_count(self) -> None:
        cost, basis = fivetran_monthly_cost(1_000_000, 4, RateCard())
        assert basis == "modelled"
        assert cost == pytest.approx(500.0 + 20.0)

    def test_reported_spend_beats_the_model(self) -> None:
        # The customer's invoice reflects their real contract and discounts.
        cost, basis = fivetran_monthly_cost(1_000_000, 4, RateCard(), reported_spend=1234.56)
        assert basis == "reported"
        assert cost == 1234.56

    def test_zero_mar_still_charges_the_connection_base(self) -> None:
        cost, _ = fivetran_monthly_cost(0, 3, RateCard())
        assert cost == pytest.approx(15.0)

    def test_rate_override_flows_through(self) -> None:
        rates = RateCard(fivetran_usd_per_million_mar=667.0, fivetran_base_charge_per_connection=0)
        cost, _ = fivetran_monthly_cost(2_000_000, 1, rates)
        assert cost == pytest.approx(1334.0)


class TestRunsPerMonth:
    @pytest.mark.parametrize(
        "minutes,expected",
        [(60, 730), (1440, 730 / 24), (30, 1460), (360, 730 / 6)],
    )
    def test_derives_frequency_from_the_fivetran_interval(self, minutes, expected) -> None:
        assert runs_per_month({"mode": "cron", "fivetran_minutes": minutes}) == pytest.approx(expected)

    def test_continuous_is_billed_as_the_whole_month(self) -> None:
        assert runs_per_month({"mode": "continuous"}) == HOURS_PER_MONTH


class TestModelLakeflowCost:
    def test_serverless_only_when_no_gateway_is_needed(self) -> None:
        components = model_lakeflow_cost(_plan(_item()), RateCard())
        assert [c.label for c in components] == ["Serverless ingestion"]

    def test_gateway_adds_compute_and_infrastructure_lines(self) -> None:
        components = model_lakeflow_cost(_plan(_item(gateway="required")), RateCard())
        labels = [c.label for c in components]
        assert "Ingestion gateways (continuous)" in labels
        # Classic compute carries a cloud bill Databricks never sees.
        assert "Cloud infrastructure for gateways" in labels

    def test_gateway_cost_is_continuous_and_volume_independent(self) -> None:
        rates = RateCard(gateway_dbu_per_hour=2.0, gateway_dbu_usd=0.36, infra_uplift=0.0)
        components = model_lakeflow_cost(_plan(_item(gateway="required")), rates)
        gateway = next(c for c in components if c.label.startswith("Ingestion gateways"))
        assert gateway.monthly_usd == pytest.approx(HOURS_PER_MONTH * 2.0 * 0.36)

    def test_infrastructure_uplift_applies_only_to_the_gateway(self) -> None:
        rates = RateCard(infra_uplift=0.5)
        components = model_lakeflow_cost(_plan(_item(gateway="required")), rates)
        gateway = next(c for c in components if c.label.startswith("Ingestion gateways"))
        infra = next(c for c in components if c.label.startswith("Cloud infrastructure"))
        assert infra.monthly_usd == pytest.approx(gateway.monthly_usd * 0.5)

    def test_blocked_connections_are_excluded(self) -> None:
        plan = _plan(_item(), _item(blockers=["no connector"]))
        components = model_lakeflow_cost(plan, RateCard())
        assert "1 pipelines" in components[0].detail

    def test_more_frequent_syncs_cost_more(self) -> None:
        hourly = model_lakeflow_cost(_plan(_item(minutes=60)), RateCard())[0].monthly_usd
        daily = model_lakeflow_cost(_plan(_item(minutes=1440)), RateCard())[0].monthly_usd
        # Unlike Fivetran MAR, Databricks cost scales with sync frequency.
        assert hourly > daily

    def test_continuous_pipelines_are_called_out(self) -> None:
        plan = _plan({"blockers": [], "target": {"gateway": "not_required"}, "schedule": {"mode": "continuous"}})
        assert "continuously" in model_lakeflow_cost(plan, RateCard())[0].detail


class TestMeasuredCost:
    def test_groups_real_usage_by_pipeline_type(self) -> None:
        rows = [
            {"pipeline_type": "INGESTION_PIPELINE", "list_cost_usd": 100.0},
            {"pipeline_type": "INGESTION_PIPELINE", "list_cost_usd": 50.0},
            {"pipeline_type": "INGESTION_GATEWAY", "list_cost_usd": 500.0},
        ]
        components = measured_lakeflow_cost(rows)
        assert components[0].label == "Ingestion gateways (measured)"
        assert components[0].monthly_usd == 500.0
        assert components[1].monthly_usd == 150.0

    def test_ignores_unparseable_costs(self) -> None:
        rows = [{"pipeline_type": "INGESTION_PIPELINE", "list_cost_usd": None}]
        assert measured_lakeflow_cost(rows)[0].monthly_usd == 0.0


class TestCompare:
    def test_produces_a_delta_and_percentage(self) -> None:
        result = compare(_plan(_item()), {"latest_month_mar": 10_000_000}, RateCard())
        assert result.fivetran_monthly_usd > result.lakeflow_monthly_usd
        assert result.delta_pct is not None and result.delta_pct > 0

    def test_measured_usage_replaces_the_model_and_drops_assumptions(self) -> None:
        result = compare(
            _plan(_item()),
            {"latest_month_mar": 1_000_000},
            RateCard(),
            measured_usage=[{"pipeline_type": "INGESTION_PIPELINE", "list_cost_usd": 42.0}],
        )
        assert result.basis == "measured"
        assert result.lakeflow_monthly_usd == 42.0
        assert result.assumptions == []

    def test_modelled_basis_always_carries_the_pilot_caveat(self) -> None:
        result = compare(_plan(_item()), {"latest_month_mar": 1_000_000}, RateCard())
        assert any("scenario, not a forecast" in c for c in result.caveats)

    def test_missing_mar_is_called_out(self) -> None:
        result = compare(_plan(_item()), None, RateCard())
        assert any("No MAR data" in c for c in result.caveats)

    def test_ela_risk_is_flagged_when_cost_is_modelled(self) -> None:
        result = compare(_plan(_item()), {"latest_month_mar": 1_000_000}, RateCard())
        assert any("ELA" in c for c in result.caveats)

    def test_reported_spend_suppresses_the_ela_caveat(self) -> None:
        result = compare(
            _plan(_item()), {"latest_month_mar": 1_000_000}, RateCard(), reported_spend=900.0
        )
        assert not any("ELA" in c for c in result.caveats)

    def test_blocked_connections_are_flagged_as_residual_cost(self) -> None:
        result = compare(_plan(_item(), blocked=2), {"latest_month_mar": 1}, RateCard())
        assert any("does not go away" in c for c in result.caveats)

    def test_zero_fivetran_cost_yields_no_percentage(self) -> None:
        rates = RateCard(fivetran_base_charge_per_connection=0)
        result = compare(_plan(), {"latest_month_mar": 0}, rates)
        assert result.delta_pct is None


class TestRateCardOverrides:
    def test_defaults_when_nothing_supplied(self) -> None:
        assert rate_card_from_overrides(None) == RateCard()

    def test_applies_known_overrides(self) -> None:
        assert rate_card_from_overrides({"minutes_per_run": 12}).minutes_per_run == 12

    def test_rejects_unknown_fields_by_name(self) -> None:
        with pytest.raises(ValueError, match="madeup"):
            rate_card_from_overrides({"madeup": 1})
