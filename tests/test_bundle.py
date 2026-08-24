"""Tests for Databricks Asset Bundle generation."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

SCRIPTS = Path(__file__).resolve().parents[1] / "skills/fivetran-to-lakeflow-migration/scripts"
sys.path.insert(0, str(SCRIPTS))

from ftlfc.bundle import build_bundle


def _item(
    *,
    key: str = "sales_abc",
    connection_type: str = "SALESFORCE",
    gateway: str = "not_required",
    scriptable: str = "yes",
    objects=None,
    schedule=None,
    blockers=None,
) -> dict:
    return {
        "fivetran": {"connection_id": "abc", "service": "salesforce"},
        "target": {
            "connection_name": f"conn_{key}",
            "connection_type": connection_type,
            "category": "saas_managed",
            "availability": "ga",
            "gateway": gateway,
            "scriptable": scriptable,
            "effort": "low",
            "pipeline_name": key,
            "destination_catalog": "main",
            "destination_schema": "sales",
            "alternative": "",
        },
        "objects": objects
        if objects is not None
        else [
            {
                "type": "table",
                "source_schema": "objects",
                "source_table": "Account",
                "destination_catalog": "main",
                "destination_schema": "sales",
                "destination_table": "account",
                "table_configuration": {"scd_type": "SCD_TYPE_1", "primary_keys": ["Id"]},
                "primary_keys_known": True,
            }
        ],
        "schedule": schedule
        or {
            "mode": "cron",
            "fivetran_minutes": 60,
            "quartz_cron_expression": "0 0 * * * ?",
            "timezone_id": "UTC",
            "max_concurrent_runs": 1,
        },
        "prerequisites": {"steps": [], "automatable": True, "auth_note": ""},
        "blockers": blockers or [],
        "warnings": [],
    }


def _plan(*items, catalog: str = "main_prod") -> dict:
    item_list = list(items)
    migratable = [i for i in item_list if not i["blockers"]]
    return {
        "target_catalog": catalog,
        "items": item_list,
        "summary": {
            "connections_total": len(item_list),
            "connections_blocked": len(item_list) - len(migratable),
            "tables_total": sum(len(i["objects"]) for i in migratable),
            "gateways_required": sum(1 for i in migratable if i["target"]["gateway"] == "required"),
        },
    }


def _load(files: dict[str, str], path: str) -> dict:
    return yaml.safe_load(files[path])


class TestRoot:
    def test_emits_dev_and_prod_targets(self) -> None:
        root = _load(build_bundle(_plan(_item()), "acme"), "databricks.yml")
        assert root["bundle"]["name"] == "acme"
        assert root["targets"]["dev"]["mode"] == "development"
        assert root["targets"]["prod"]["mode"] == "production"

    def test_dev_target_is_the_default(self) -> None:
        root = _load(build_bundle(_plan(_item()), "acme"), "databricks.yml")
        assert root["targets"]["dev"]["default"] is True
        assert "default" not in root["targets"]["prod"]

    def test_dev_writes_to_a_separate_catalog(self) -> None:
        root = _load(build_bundle(_plan(_item()), "acme"), "databricks.yml")
        assert root["targets"]["dev"]["variables"]["dest_catalog"] == "main_prod_dev"
        assert root["targets"]["prod"]["variables"]["dest_catalog"] == "main_prod"

    def test_workspace_is_omitted_when_no_host_is_given(self) -> None:
        root = _load(build_bundle(_plan(_item()), "acme"), "databricks.yml")
        assert "workspace" not in root["targets"]["dev"]

    def test_host_is_applied_to_both_targets(self) -> None:
        files = build_bundle(_plan(_item()), "acme", host="https://x.databricks.com")
        root = _load(files, "databricks.yml")
        assert root["targets"]["dev"]["workspace"]["host"] == "https://x.databricks.com"
        assert root["targets"]["prod"]["workspace"]["host"] == "https://x.databricks.com"

    def test_no_yaml_anchors_leak_into_the_output(self) -> None:
        # Sharing one dict across targets makes PyYAML emit an alias, which is
        # valid but confusing in a file humans review.
        files = build_bundle(_plan(_item()), "acme", host="https://x.databricks.com")
        assert "&id" not in files["databricks.yml"]
        assert "*id" not in files["databricks.yml"]


class TestSaasPipeline:
    @pytest.fixture
    def pipeline(self) -> dict:
        files = build_bundle(_plan(_item()), "acme")
        return _load(files, "resources/sales_abc.pipeline.yml")["resources"]["pipelines"][
            "sales_abc"
        ]

    def test_runs_serverless(self, pipeline: dict) -> None:
        assert pipeline["serverless"] is True

    def test_references_the_connection_directly(self, pipeline: dict) -> None:
        assert pipeline["ingestion_definition"]["connection_name"] == "conn_sales_abc"
        assert "ingestion_gateway_id" not in pipeline["ingestion_definition"]

    def test_omits_output_only_source_type(self, pipeline: dict) -> None:
        # source_type is ignored on input; emitting it is noise.
        assert "source_type" not in pipeline["ingestion_definition"]

    def test_carries_table_configuration_through(self, pipeline: dict) -> None:
        table = pipeline["ingestion_definition"]["objects"][0]["table"]
        assert table["table_configuration"]["scd_type"] == "SCD_TYPE_1"
        assert table["table_configuration"]["primary_keys"] == ["Id"]

    def test_salesforce_formula_fields_use_the_configuration_flag(self, pipeline: dict) -> None:
        # The table-level salesforce_include_formula_fields field is private preview.
        assert (
            pipeline["configuration"]["pipelines.enableSalesforceFormulaFieldsMVComputation"]
            == "true"
        )

    def test_no_gateway_file_is_emitted(self) -> None:
        files = build_bundle(_plan(_item()), "acme")
        assert not any("gateway" in name for name in files)


class TestGatewayPipeline:
    @pytest.fixture
    def files(self) -> dict[str, str]:
        return build_bundle(_plan(_item(connection_type="SQLSERVER", gateway="required")), "acme")

    def test_gateway_lives_in_its_own_file(self, files: dict[str, str]) -> None:
        # The CLI recommends one pipeline per .pipeline.yml file.
        assert "resources/sales_abc_gateway.pipeline.yml" in files
        ingestion = _load(files, "resources/sales_abc.pipeline.yml")
        assert list(ingestion["resources"]["pipelines"]) == ["sales_abc"]

    def test_gateway_runs_continuously_on_classic_compute(self, files: dict[str, str]) -> None:
        gateway = _load(files, "resources/sales_abc_gateway.pipeline.yml")["resources"][
            "pipelines"
        ]["sales_abc_gateway"]
        assert gateway["continuous"] is True
        assert gateway["serverless"] is False

    def test_gateway_declares_its_staging_storage(self, files: dict[str, str]) -> None:
        gateway = _load(files, "resources/sales_abc_gateway.pipeline.yml")["resources"][
            "pipelines"
        ]["sales_abc_gateway"]
        definition = gateway["gateway_definition"]
        assert definition["connection_name"] == "conn_sales_abc"
        assert definition["gateway_storage_catalog"] == "${var.staging_catalog}"
        assert definition["gateway_storage_name"] == "sales_abc_staging"

    def test_ingestion_references_the_gateway_by_pipeline_id(self, files: dict[str, str]) -> None:
        definition = _load(files, "resources/sales_abc.pipeline.yml")["resources"]["pipelines"][
            "sales_abc"
        ]["ingestion_definition"]
        assert definition["ingestion_gateway_id"] == "${resources.pipelines.sales_abc_gateway.id}"
        # Exactly one of gateway id / connection name may be set.
        assert "connection_name" not in definition


class TestJob:
    def test_every_pipeline_gets_a_companion_job(self) -> None:
        # There is no supported pipeline-level schedule.
        files = build_bundle(_plan(_item()), "acme")
        assert "resources/sales_abc.job.yml" in files

    def test_cron_schedule_is_emitted_with_a_timezone(self) -> None:
        files = build_bundle(_plan(_item()), "acme")
        job = _load(files, "resources/sales_abc.job.yml")["resources"]["jobs"]["sales_abc_schedule"]
        assert job["schedule"]["quartz_cron_expression"] == "0 0 * * * ?"
        assert job["schedule"]["timezone_id"] == "UTC"

    def test_concurrency_is_capped_to_match_fivetran(self) -> None:
        files = build_bundle(_plan(_item()), "acme")
        job = _load(files, "resources/sales_abc.job.yml")["resources"]["jobs"]["sales_abc_schedule"]
        assert job["max_concurrent_runs"] == 1

    def test_job_triggers_the_pipeline_by_reference(self) -> None:
        files = build_bundle(_plan(_item()), "acme")
        job = _load(files, "resources/sales_abc.job.yml")["resources"]["jobs"]["sales_abc_schedule"]
        task = job["tasks"][0]["pipeline_task"]
        assert task["pipeline_id"] == "${resources.pipelines.sales_abc.id}"
        assert task["full_refresh"] is False

    def test_continuous_schedule_becomes_a_continuous_job(self) -> None:
        item = _item(schedule={"mode": "continuous", "max_concurrent_runs": 1})
        files = build_bundle(_plan(item), "acme")
        job = _load(files, "resources/sales_abc.job.yml")["resources"]["jobs"]["sales_abc_schedule"]
        assert job["continuous"] == {"pause_status": "UNPAUSED"}
        assert "schedule" not in job

    def test_notifications_are_wired_when_an_email_is_given(self) -> None:
        files = build_bundle(_plan(_item()), "acme", notification_email="a@b.com")
        job = _load(files, "resources/sales_abc.job.yml")["resources"]["jobs"]["sales_abc_schedule"]
        assert job["email_notifications"]["on_failure"] == ["a@b.com"]


class TestConnectionScript:
    def test_emits_a_create_call_per_connection(self) -> None:
        files = build_bundle(_plan(_item()), "acme")
        script = files["scripts/create_connections.sh"]
        assert "databricks connections create" in script
        assert "conn_sales_abc" in script

    def test_marks_connections_read_only(self) -> None:
        script = build_bundle(_plan(_item()), "acme")["scripts/create_connections.sh"]
        assert '"read_only": true' in script

    def test_blocked_connectors_are_skipped_with_an_explanation(self) -> None:
        item = _item(connection_type="HUBSPOT", blockers=["browser OAuth only"])
        script = build_bundle(_plan(item), "acme")["scripts/create_connections.sh"]
        assert "SKIPPED" in script
        assert "browser OAuth only" in script

    def test_conditional_connectors_carry_their_auth_caveat(self) -> None:
        item = _item(scriptable="conditional")
        item["prerequisites"]["auth_note"] = "Requires mTLS Beta."
        script = build_bundle(_plan(item), "acme")["scripts/create_connections.sh"]
        assert "Requires mTLS Beta." in script

    def test_secret_values_are_placeholders_not_guesses(self) -> None:
        script = build_bundle(_plan(_item()), "acme")["scripts/create_connections.sh"]
        assert "REPLACE_ME" in script


class TestBlockedConnections:
    def test_blocked_items_produce_no_pipeline_files(self) -> None:
        files = build_bundle(_plan(_item(blockers=["no connector"])), "acme")
        assert not any(name.endswith(".pipeline.yml") for name in files)

    def test_readme_names_what_was_left_out(self) -> None:
        files = build_bundle(_plan(_item(blockers=["No managed connector for s3."])), "acme")
        assert "Not included" in files["README.md"]
        assert "No managed connector for s3." in files["README.md"]

    def test_mixed_plan_emits_only_the_migratable_pipelines(self) -> None:
        plan = _plan(_item(key="good"), _item(key="bad", blockers=["nope"]))
        files = build_bundle(plan, "acme")
        assert "resources/good.pipeline.yml" in files
        assert "resources/bad.pipeline.yml" not in files
