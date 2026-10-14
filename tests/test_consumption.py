"""Tests for the Fivetran spend record and the account-team consumption export."""

from __future__ import annotations

import copy
import csv
import datetime as dt
import json
import sys
from pathlib import Path

import pytest

SKILL = Path(__file__).resolve().parents[1] / "skills/fivetran-to-lakeflow-migration"
SCRIPTS = SKILL / "scripts"
sys.path.insert(0, str(SCRIPTS))

import fivetran_consumption
from ftlfc.consumption import (
    ConsumptionError,
    billing_rows,
    check_volume,
    consumption_rows,
    mar_by_table_rows,
    parse_metastore,
    plan_group_ids,
    record_spend,
    summary_lines,
    write_export,
)
from ftlfc.mapping import build_plan

TODAY = dt.date(2026, 10, 14)
GROUP = "schoolmaster_heedless"


def _mar(destination_id: str, name: str, table: str, month: str, mar: int, free_type="PAID"):
    return {
        "destination_id": destination_id,
        "connection_name": name,
        "schema_name": name,
        "table_name": table,
        "measured_month": month,
        "free_type": free_type,
        "mar": mar,
    }


@pytest.fixture
def plan() -> dict:
    inventory = json.loads((SKILL / "fixtures/inventory.sample.json").read_text())
    return build_plan(inventory, target_catalog="main_prod")


@pytest.fixture
def mar_doc() -> dict:
    return {
        "records": [
            _mar(GROUP, "salesforce", "account", "2026-09", 14400),
            _mar(GROUP, "salesforce", "account", "2026-09", 1000, "INITIAL_SYNC"),
            _mar(GROUP, "salesforce", "account", "2026-08", 99999),
            _mar(GROUP, "salesforce", "account", "2026-10", 77777),
            # Same connection name in another destination must not attach to erp.
            _mar("grp_other", "erp", "orders", "2026-09", 31337),
        ],
        "billing": [
            {
                "group_id": GROUP,
                "measured_month": "2026-09",
                "metric": "usage_cost_usd",
                "value": 4200.0,
            },
            {
                "group_id": GROUP,
                "measured_month": "2026-08",
                "metric": "usage_cost_usd",
                "value": 3900.5,
            },
            {
                "group_id": GROUP,
                "measured_month": "2026-10",
                "metric": "usage_cost_usd",
                "value": 50.0,
            },
            {
                "group_id": "grp_other",
                "measured_month": "2026-09",
                "metric": "usage_cost_usd",
                "value": 999.0,
            },
        ],
    }


def _by_id(rows: list[dict]) -> dict[str, dict]:
    return {r["connection_id"]: r for r in rows}


class TestRecordSpend:
    def test_records_as_given(self) -> None:
        now = dt.datetime(2026, 10, 14, tzinfo=dt.timezone.utc)
        [entry] = record_spend(
            [], month="2026-09", monthly_usd=12000, basis="invoice", monthly_mar=25, now=now
        )
        assert entry == {
            "period_month": "2026-09-01",
            "monthly_usd": 12000.0,
            "basis": "invoice",
            "monthly_mar": 25,
            "note": None,
            "recorded_at": "2026-10-14T00:00:00+00:00",
        }

    def test_same_month_replaces_the_earlier_entry(self) -> None:
        entries = record_spend([], month="2026-09", monthly_usd=1)
        entries = record_spend(entries, month="2026-08", monthly_usd=2)
        entries = record_spend(entries, month="2026-09", monthly_usd=3, basis="contract")
        assert [(e["period_month"], e["monthly_usd"]) for e in entries] == [
            ("2026-08-01", 2.0),
            ("2026-09-01", 3.0),
        ]

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"month": "2026-13", "monthly_usd": 1},
            {"month": "Sept", "monthly_usd": 1},
            {"month": "2026-09", "monthly_usd": -1},
            {"month": "2026-09", "monthly_usd": 1, "basis": "quote"},
            {"month": "2026-09", "monthly_usd": 1, "monthly_mar": -5},
        ],
    )
    def test_rejects_invalid_input(self, kwargs: dict) -> None:
        with pytest.raises(ConsumptionError):
            record_spend([], **kwargs)


class TestConsumptionRows:
    def test_one_row_per_planned_connection(self, plan: dict, mar_doc: dict) -> None:
        rows = consumption_rows(plan, mar_doc, cloud="aws", region="us-west-2", today=TODAY)
        assert len(rows) == len(plan["items"])
        assert all(r["cloud"] == "aws" and r["region"] == "us-west-2" for r in rows)

    def test_estimator_inputs_from_latest_full_month(self, plan: dict, mar_doc: dict) -> None:
        sfdc = _by_id(consumption_rows(plan, mar_doc, today=TODAY))["speak_inexpensive"]
        assert sfdc["source_type"] == "SALESFORCE"
        assert sfdc["num_flows"] == 2
        assert sfdc["sync_minutes"] == 360
        assert sfdc["runs_per_month"] == 120
        assert sfdc["measured_month"] == "2026-09-01"
        assert sfdc["paid_mar"] == 14400
        assert sfdc["total_mar"] == 15400
        assert sfdc["num_rows"] == 120

    def test_joins_on_destination_as_well_as_name(self, plan: dict, mar_doc: dict) -> None:
        erp = _by_id(consumption_rows(plan, mar_doc, today=TODAY))["steadfast_pothole"]
        assert erp["paid_mar"] is None
        assert erp["num_rows"] is None
        assert erp["measured_month"] is None

    def test_continuous_schedules_have_no_runs(self, plan: dict, mar_doc: dict) -> None:
        plan = copy.deepcopy(plan)
        item = next(
            i for i in plan["items"] if i["fivetran"]["connection_id"] == "speak_inexpensive"
        )
        item["schedule"]["mode"] = "continuous"
        sfdc = _by_id(consumption_rows(plan, mar_doc, today=TODAY))["speak_inexpensive"]
        assert sfdc["runs_per_month"] is None
        assert sfdc["num_rows"] is None
        assert sfdc["paid_mar"] == 14400

    def test_without_mar_every_consumption_column_is_empty(self, plan: dict) -> None:
        rows = consumption_rows(plan, None, today=TODAY)
        assert all(r["paid_mar"] is None and r["num_rows"] is None for r in rows)

    def test_exports_without_destination_ids_match_on_name(self, plan: dict) -> None:
        doc = {"records": [_mar("", "SalesForce", "account", "2026-09", 1200)]}
        sfdc = _by_id(consumption_rows(plan, doc, today=TODAY))["speak_inexpensive"]
        assert sfdc["paid_mar"] == 1200


class TestBillingAndTables:
    def test_billing_keeps_plan_destinations_and_customer_spend(
        self, plan: dict, mar_doc: dict
    ) -> None:
        spend = record_spend([], month="2026-09", monthly_usd=12000, basis="invoice", note="x")
        rows = billing_rows(mar_doc, spend, plan_group_ids(plan))
        assert {r["group_id"] for r in rows} == {GROUP, None}
        customer = [r for r in rows if r["source"] == "customer"]
        assert customer == [
            {
                "source": "customer",
                "group_id": None,
                "measured_month": "2026-09-01",
                "metric": "reported_invoice_usd",
                "value": 12000.0,
                "note": "x",
            }
        ]

    def test_unspecified_basis(self) -> None:
        spend = record_spend([], month="2026-09", monthly_usd=1)
        assert billing_rows(None, spend, set())[0]["metric"] == "reported_unspecified_usd"

    def test_mar_by_table_is_filtered_to_plan_destinations(self, plan: dict, mar_doc: dict) -> None:
        rows = mar_by_table_rows(plan, mar_doc)
        assert {r["destination_id"] for r in rows} == {GROUP}
        assert {r["connection_id"] for r in rows} == {"speak_inexpensive"}
        assert len(rows) == 4


class TestSummary:
    def test_reports_recorded_figures_only(self, plan: dict, mar_doc: dict) -> None:
        spend = record_spend([], month="2026-09", monthly_usd=12000, basis="invoice")
        consumption = consumption_rows(plan, mar_doc, today=TODAY)
        billing = billing_rows(mar_doc, spend, plan_group_ids(plan))
        lines = summary_lines(consumption, billing, today=TODAY)
        assert ("fivetran_spend", "2026-09-01", "$12,000.00/month (invoice)") in lines
        assert ("fivetran_consumption", "paid_mar", "14,400") in lines
        assert ("fivetran_consumption", "connections_with_mar", "1") in lines
        assert ("fivetran_consumption", "connections_without_mar", "4") in lines
        # The latest full billing month for plan destinations only; October is partial.
        assert (
            "fivetran_consumption",
            "billed usage_cost_usd 2026-09-01",
            "$4,200.00",
        ) in lines

    def test_credits_are_not_shown_as_dollars(self) -> None:
        billing = [
            {
                "source": "platform_connector",
                "group_id": GROUP,
                "measured_month": "2026-09-01",
                "metric": "credits_used",
                "value": 812.0,
                "note": None,
            }
        ]
        assert summary_lines([], billing, today=TODAY) == [
            ("fivetran_consumption", "billed credits_used 2026-09-01", "812 credits")
        ]

    def test_empty_when_nothing_was_recorded(self, plan: dict) -> None:
        assert summary_lines(consumption_rows(plan, None, today=TODAY), [], today=TODAY) == []


class TestExport:
    def test_writes_csvs_and_readme(self, tmp_path: Path, plan: dict, mar_doc: dict) -> None:
        consumption = consumption_rows(plan, mar_doc, cloud="aws", region="us-west-2", today=TODAY)
        out, counts = write_export(
            tmp_path,
            consumption,
            billing_rows(mar_doc, [], plan_group_ids(plan)),
            mar_by_table_rows(plan, mar_doc),
            today=TODAY,
        )
        assert out == tmp_path / "fivetran_consumption_2026-10-14"
        assert counts == {"connections.csv": 5, "fivetran_billing.csv": 3, "mar_by_table.csv": 4}
        assert "No credentials or row data" in (out / "README.txt").read_text()
        with (out / "connections.csv").open() as handle:
            rows = {r["connection_id"]: r for r in csv.DictReader(handle)}
        # Integers stay integers, booleans are lower-case, missing values are empty.
        assert rows["speak_inexpensive"]["paid_mar"] == "14400"
        assert rows["speak_inexpensive"]["num_rows"] == "120"
        assert rows["speak_inexpensive"]["migratable"] == "true"
        assert rows["steadfast_pothole"]["num_rows"] == ""

    def test_summary_cli_writes_nothing(
        self, tmp_path: Path, plan: dict, capsys: pytest.CaptureFixture
    ) -> None:
        plan_path = tmp_path / "plan.json"
        plan_path.write_text(json.dumps(plan))
        before = set(tmp_path.iterdir())
        assert fivetran_consumption.main(["summary", "-p", str(plan_path)]) == 0
        assert set(tmp_path.iterdir()) == before
        assert "No Fivetran consumption or spend was recorded." in capsys.readouterr().out

    def test_spend_then_export_cli(self, tmp_path: Path, plan: dict) -> None:
        plan_path = tmp_path / "plan.json"
        plan_path.write_text(json.dumps(plan))
        spend_path = tmp_path / "fivetran_spend.json"
        assert (
            fivetran_consumption.main(
                ["spend", "--month", "2026-09", "--monthly-usd", "500", "-o", str(spend_path)]
            )
            == 0
        )
        assert (
            fivetran_consumption.main(
                [
                    "export",
                    "-p",
                    str(plan_path),
                    "--spend",
                    str(spend_path),
                    "--cloud",
                    "aws",
                    "--region",
                    "us-east-1",
                    "-o",
                    str(tmp_path / "exp"),
                ]
            )
            == 0
        )
        [folder] = (tmp_path / "exp").iterdir()
        billing = (folder / "fivetran_billing.csv").read_text()
        assert "customer,,2026-09-01,reported_unspecified_usd,500.0," in billing

    def test_rejects_a_bad_volume_before_writing(self, tmp_path: Path, plan: dict) -> None:
        plan_path = tmp_path / "plan.json"
        plan_path.write_text(json.dumps(plan))
        assert (
            fivetran_consumption.main(
                [
                    "export",
                    "-p",
                    str(plan_path),
                    "-o",
                    str(tmp_path / "exp"),
                    "--volume",
                    "main.schema; rm -rf",
                ]
            )
            == 1
        )
        assert not (tmp_path / "exp").exists()


class TestHelpers:
    def test_parse_metastore(self) -> None:
        assert parse_metastore("aws:us-west-2:00000000-0000-0000-0000-000000000000") == (
            "aws",
            "us-west-2",
        )
        assert parse_metastore("") == (None, None)

    def test_check_volume(self) -> None:
        assert check_volume("main.fivetran_migration.exports") == (
            "main",
            "fivetran_migration",
            "exports",
        )
        with pytest.raises(ConsumptionError):
            check_volume("main.exports")
