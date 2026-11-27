"""Tests for Databricks Asset Bundle generation."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
import yaml

SCRIPTS = Path(__file__).resolve().parents[1] / "skills/fivetran-to-lakeflow-migration/scripts"
sys.path.insert(0, str(SCRIPTS))

import generate_bundle
from ftlfc.bundle import build_bundle
from ftlfc.mapping import build_plan
from ftlfc.workspace import WorkspaceError

SAMPLE_INVENTORY = SCRIPTS.parent / "fixtures/inventory.sample.json"


def _item(
    *,
    key: str = "sales_abc",
    connection_type: str = "SALESFORCE",
    gateway: str = "not_required",
    scriptable: str = "yes",
    preferred_auth: str | None = None,
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
            "preferred_auth": preferred_auth,
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
            "connections_not_migratable": len(item_list) - len(migratable),
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

    def test_dev_catalog_can_be_overridden(self) -> None:
        files = build_bundle(_plan(_item()), "acme", dev_catalog="sandbox")
        root = _load(files, "databricks.yml")
        assert root["targets"]["dev"]["variables"]["dest_catalog"] == "sandbox"
        assert root["targets"]["prod"]["variables"]["dest_catalog"] == "main_prod"

    def test_prod_target_sets_a_root_path(self) -> None:
        # mode: production does not validate without an explicit root_path.
        for host in (None, "https://x.databricks.com"):
            root = _load(build_bundle(_plan(_item()), "acme", host=host), "databricks.yml")
            assert root["targets"]["prod"]["workspace"]["root_path"] == (
                "/Workspace/Users/${workspace.current_user.userName}/.bundle/"
                "${bundle.name}/${bundle.target}"
            )
            assert "root_path" not in root["targets"]["dev"].get("workspace", {})

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


def _integrated_cdc_item(gateway: str = "not_required", **overrides) -> dict:
    item = _item(
        key="erp_abc",
        connection_type="SQLSERVER",
        gateway=gateway,
        objects=[
            {
                "type": "table",
                "source_catalog": "ERPProd",
                "source_schema": "dbo",
                "source_table": "Orders",
                "destination_catalog": "main",
                "destination_schema": "erp",
                "destination_table": "orders",
                "table_configuration": {"scd_type": "SCD_TYPE_1"},
                "primary_keys_known": False,
            }
        ],
        **overrides,
    )
    item["target"]["category"] = "database_cdc"
    item["target"]["architecture"] = "integrated_cdc"
    item["target"]["connector_type"] = "CDC"
    return item


class TestIntegratedCdcPipeline:
    @pytest.fixture
    def files(self) -> dict[str, str]:
        return build_bundle(_plan(_integrated_cdc_item()), "acme")

    @pytest.fixture
    def pipeline(self, files: dict[str, str]) -> dict:
        return _load(files, "resources/erp_abc.pipeline.yml")["resources"]["pipelines"]["erp_abc"]

    def test_no_gateway_file_is_emitted(self, files: dict[str, str]) -> None:
        assert not any("gateway" in name for name in files)

    def test_runs_a_single_serverless_pipeline(self, pipeline: dict) -> None:
        assert pipeline["serverless"] is True

    def test_references_the_connection_directly_with_connector_type_cdc(
        self, pipeline: dict
    ) -> None:
        definition = pipeline["ingestion_definition"]
        assert definition["connection_name"] == "conn_erp_abc"
        assert definition["connector_type"] == "CDC"
        assert "ingestion_gateway_id" not in definition

    def test_declares_a_staging_volume(self, pipeline: dict) -> None:
        staging = pipeline["ingestion_definition"]["data_staging_options"]
        assert staging["catalog_name"] == "${var.staging_catalog}"
        assert staging["schema_name"] == "${var.staging_schema}"

    def test_table_object_carries_the_source_database_as_source_catalog(
        self, pipeline: dict
    ) -> None:
        table = pipeline["ingestion_definition"]["objects"][0]["table"]
        assert table["source_catalog"] == "ERPProd"
        assert table["source_schema"] == "dbo"
        assert table["source_table"] == "Orders"

    def test_gets_a_companion_job(self, files: dict[str, str]) -> None:
        assert "resources/erp_abc.job.yml" in files

    def test_gateway_architecture_omits_connector_type_and_staging(self) -> None:
        # The same source generated as the gateway pair must not leak integrated fields.
        item = _integrated_cdc_item(gateway="required")
        item["target"]["architecture"] = "gateway"
        item["target"]["connector_type"] = None
        files = build_bundle(_plan(item), "acme")
        assert "resources/erp_abc_gateway.pipeline.yml" in files
        definition = _load(files, "resources/erp_abc.pipeline.yml")["resources"]["pipelines"][
            "erp_abc"
        ]["ingestion_definition"]
        assert "connector_type" not in definition
        assert "data_staging_options" not in definition
        assert "ingestion_gateway_id" in definition


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
        assert job["continuous"] == {}
        assert "schedule" not in job

    def test_jobs_leave_pause_status_to_the_deployment_mode(self) -> None:
        for schedule in (None, {"mode": "continuous", "max_concurrent_runs": 1}):
            item = _item(schedule=schedule) if schedule else _item()
            files = build_bundle(_plan(item), "acme")
            assert "pause_status" not in files["resources/sales_abc.job.yml"]
            root = _load(files, "databricks.yml")
            assert "presets" not in root["targets"]["dev"]
            assert "presets" not in root["targets"]["prod"]

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
        item = _item(connection_type="HUBSPOT", blockers=["no connector"])
        script = build_bundle(_plan(item), "acme")["scripts/create_connections.sh"]
        assert "SKIPPED" in script
        assert "no connector" in script

    def test_browser_oauth_connections_get_a_manual_step_not_a_create_call(self) -> None:
        item = _item(key="hub", connection_type="HUBSPOT", scriptable="no")
        script = build_bundle(_plan(item), "acme")["scripts/create_connections.sh"]
        assert "MANUAL" in script
        assert "Connection name: conn_hub" in script
        assert "Connection type: HUBSPOT" in script
        assert "databricks connections create" not in script

    def test_salesforce_mtls_emits_certificate_and_key_options(self) -> None:
        item = _item(scriptable="conditional", preferred_auth="OAUTH_MTLS")
        script = build_bundle(_plan(item), "acme")["scripts/create_connections.sh"]
        assert '"client_private_key": "REPLACE_ME"' in script
        assert '"client_certificate": "REPLACE_ME"' in script
        assert "OAUTH_MTLS auth path" in script
        assert "verify them before running" in script

    def test_unknown_preferred_auth_falls_back_to_default_options(self) -> None:
        item = _item(connection_type="SERVICENOW", preferred_auth="OAUTH_RESOURCE_OWNER_PASSWORD")
        script = build_bundle(_plan(item), "acme")["scripts/create_connections.sh"]
        assert '"oauth_scope": "useraccount"' in script

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

    def test_readme_does_not_list_manual_connections_when_there_are_none(self) -> None:
        assert "Manual connections" not in build_bundle(_plan(_item()), "acme")["README.md"]

    def test_readme_validates_prod_only_right_before_the_prod_deploy(self) -> None:
        lines = build_bundle(_plan(_item()), "acme")["README.md"].splitlines()
        prod_validate = lines.index("databricks bundle validate --strict -t prod")
        assert lines[prod_validate + 1] == "databricks bundle deploy -t prod"
        assert prod_validate > lines.index("databricks bundle deploy -t dev")

    def test_mixed_plan_emits_only_the_migratable_pipelines(self) -> None:
        plan = _plan(_item(key="good"), _item(key="bad", blockers=["nope"]))
        files = build_bundle(plan, "acme")
        assert "resources/good.pipeline.yml" in files
        assert "resources/bad.pipeline.yml" not in files


class TestBrowserOAuthConnections:
    def test_pipeline_and_job_are_generated(self) -> None:
        item = _item(key="conf", connection_type="CONFLUENCE", scriptable="no")
        files = build_bundle(_plan(item), "acme")
        assert "resources/conf.pipeline.yml" in files
        assert "resources/conf.job.yml" in files
        pipeline = _load(files, "resources/conf.pipeline.yml")["resources"]["pipelines"]["conf"]
        assert pipeline["ingestion_definition"]["connection_name"] == "conn_conf"

    def test_readme_lists_the_manual_connection(self) -> None:
        item = _item(key="conf", connection_type="CONFLUENCE", scriptable="no")
        readme = build_bundle(_plan(item), "acme")["README.md"]
        assert "## Manual connections" in readme
        assert "| `conn_conf` | CONFLUENCE |" in readme
        assert "Not included" not in readme


def _pagerduty_plan() -> dict:
    def table(name: str, enabled: bool = True) -> dict:
        return {
            "source_schema": "pagerduty",
            "source_table": name,
            "destination_table": name,
            "enabled": enabled,
            "primary_keys": ["id"],
            "primary_keys_known": True,
        }

    connection = {
        "id": "pd1",
        "service": "pagerduty",
        "destination_schema": "pagerduty",
        "sync_frequency_minutes": 60,
        "objects": [table("incident"), table("users"), table("team", enabled=False)],
    }
    return build_plan({"connections": [connection]}, "main")


class TestSupportedTableObjects:
    """Connectors with a supported-table list get table objects named as Lakeflow
    Connect names them, never a schema object."""

    def test_pagerduty_pipeline_lists_lakeflow_tables(self) -> None:
        plan = _pagerduty_plan()
        files = build_bundle(plan, "acme")
        key = plan["items"][0]["target"]["pipeline_name"]
        pipeline = _load(files, f"resources/{key}.pipeline.yml")["resources"]["pipelines"][key]
        objects = pipeline["ingestion_definition"]["objects"]

        assert all(set(o) == {"table"} for o in objects)
        by_name = {o["table"]["source_table"]: o["table"] for o in objects}
        assert "incidents" in by_name and "users" in by_name
        # Turned off in Fivetran, so not ingested.
        assert "teams" not in by_name
        assert {t["source_schema"] for t in by_name.values()} == {"default"}
        assert by_name["incidents"]["destination_table"] == "incidents"

    def test_readme_lists_renamed_tables(self) -> None:
        readme = build_bundle(_pagerduty_plan(), "acme")["README.md"]
        assert "## Renamed tables" in readme
        assert "| `incident` | `pagerduty.incidents` |" in readme

    def test_readme_lists_lakeflow_only_tables(self) -> None:
        plan = _pagerduty_plan()
        key = plan["items"][0]["target"]["pipeline_name"]
        readme = build_bundle(plan, "acme")["README.md"]
        assert "## Lakeflow Connect-only tables" in readme
        assert f"| `pagerduty.priorities` | `{key}` |" in readme
        # Paired tables are not listed as Lakeflow Connect-only.
        section = readme.split("## Lakeflow Connect-only tables")[1]
        assert "pagerduty.incidents" not in section

    def test_readme_lists_lakeflow_only_tables_not_included(self) -> None:
        plan = build_plan(json.loads(SAMPLE_INVENTORY.read_text()), "main_prod")
        readme = build_bundle(plan, "acme")["README.md"]
        section = readme.split("## Lakeflow Connect tables not included")[1].split("\n## ")[0]
        assert "rerun plan_migration.py with --include-lakeflow-only all" in section
        assert "`deals_pipelines`" in section
        assert "`deals`," not in section
        assert "## Lakeflow Connect-only tables" not in readme

    def test_readme_omits_not_included_section_with_all(self) -> None:
        plan = build_plan(
            json.loads(SAMPLE_INVENTORY.read_text()), "main_prod", include_lakeflow_only="all"
        )
        readme = build_bundle(plan, "acme")["README.md"]
        assert "## Lakeflow Connect tables not included" not in readme
        assert "## Lakeflow Connect-only tables" in readme

    def test_readme_omits_lakeflow_only_section_without_such_tables(self) -> None:
        assert "Lakeflow Connect-only" not in build_bundle(_plan(_item()), "acme")["README.md"]

    def test_saas_without_supported_tables_emits_explicit_objects(self) -> None:
        files = build_bundle(_plan(_item()), "acme")
        pipeline = _load(files, "resources/sales_abc.pipeline.yml")["resources"]["pipelines"][
            "sales_abc"
        ]
        objects = pipeline["ingestion_definition"]["objects"]
        assert "table" in objects[0]
        assert "schema" not in objects[0]


def _reused(scriptable: str = "yes", verified: bool = False) -> dict:
    item = _item(key="pd", connection_type="PAGERDUTY", scriptable=scriptable)
    item["target"]["connection_name"] = "pd_prod"
    item["target"]["connection_source"] = "existing"
    item["target"]["connection_verified"] = verified
    return item


class TestReusedConnections:
    def test_script_neither_creates_nor_modifies_a_reused_connection(self) -> None:
        script = build_bundle(_plan(_reused()), "acme")["scripts/create_connections.sh"]
        assert "EXISTING. Reusing 'pd_prod'" in script
        assert "databricks connections create" not in script

    def test_pipeline_references_the_reused_connection_by_name(self) -> None:
        files = build_bundle(_plan(_reused()), "acme")
        pipeline = _load(files, "resources/pd.pipeline.yml")["resources"]["pipelines"]["pd"]
        assert pipeline["ingestion_definition"]["connection_name"] == "pd_prod"

    def test_reused_browser_oauth_connection_is_not_a_manual_step(self) -> None:
        files = build_bundle(_plan(_reused(scriptable="no")), "acme")
        assert "MANUAL" not in files["scripts/create_connections.sh"]
        assert "## Manual connections" not in files["README.md"]

    @pytest.mark.parametrize("verified,label", [(True, "yes"), (False, "not checked")])
    def test_readme_lists_reused_connections(self, verified: bool, label: str) -> None:
        readme = build_bundle(_plan(_reused(verified=verified)), "acme")["README.md"]
        assert "## Existing connections" in readme
        assert f"| `pd_prod` | PAGERDUTY | {label} |" in readme

    def test_readme_skips_replace_me_step_when_nothing_is_created(self) -> None:
        files = build_bundle(_plan(_reused()), "acme")
        assert "REPLACE_ME" not in files["scripts/create_connections.sh"].split("set -euo")[1]
        assert "Fill in every `REPLACE_ME`" not in files["README.md"]


class TestReadmeSetup:
    def test_replace_me_step_appears_when_the_script_needs_values(self) -> None:
        readme = build_bundle(_plan(_item()), "acme")["README.md"]
        assert "Fill in every `REPLACE_ME`" in readme

    def test_readme_documents_the_prod_root_path_choice(self) -> None:
        readme = build_bundle(_plan(_item()), "acme")["README.md"]
        assert "`workspace.root_path`" in readme
        assert "run_as.service_principal_name" in readme

    @pytest.mark.parametrize(
        "exists,label",
        [
            (True, "confirmed in the workspace"),
            (False, "not found in the workspace"),
            (None, "not checked"),
        ],
    )
    def test_readme_reports_each_catalog(self, exists: bool | None, label: str) -> None:
        readme = build_bundle(
            _plan(_item()), "acme", catalog_exists={"main_prod_dev": exists, "main_prod": True}
        )["README.md"]
        dev_line = next(line for line in readme.splitlines() if "`main_prod_dev` (dev)" in line)
        assert label in dev_line
        assert "`main_prod` (prod): confirmed in the workspace" in readme

    def test_unchecked_catalogs_say_how_to_check(self) -> None:
        readme = build_bundle(_plan(_item()), "acme", dev_catalog="sandbox")["README.md"]
        assert "`sandbox` (dev): not checked" in readme
        assert "databricks catalogs get sandbox" in readme


class TestCatalogCheck:
    def test_missing_dev_catalog_falls_back_to_the_target(self, capsys) -> None:
        found = {"main_dev": False, "main": True}
        assert generate_bundle.fall_back_dev_catalog("main_dev", "main", found) == "main"
        err = capsys.readouterr().err
        assert "dev catalog 'main_dev' not found" in err and "_dev schemas" in err

    @pytest.mark.parametrize("exists", [True, None])
    def test_existing_or_unchecked_dev_catalog_is_kept(self, exists, capsys) -> None:
        found = {"sandbox": exists}
        assert generate_bundle.fall_back_dev_catalog("sandbox", "main", found) == "sandbox"
        assert capsys.readouterr().err == ""

    def test_missing_catalog_warns_and_does_not_fail(self, capsys) -> None:
        found = generate_bundle.check_catalogs(
            ["main_dev", "main"], lambda name: {"name": name} if name == "main" else None
        )
        assert found == {"main_dev": False, "main": True}
        err = capsys.readouterr().err
        assert "catalog 'main_dev' does not exist" in err and "--dev-catalog" in err
        assert "'main' does not exist" not in err

    def test_lookup_error_is_reported_as_unchecked(self, capsys) -> None:
        def lookup(name: str):
            raise WorkspaceError("refresh token is invalid")

        assert generate_bundle.check_catalogs(["main"], lookup) == {"main": None}
        assert "could not check catalog 'main'" in capsys.readouterr().err

    def test_same_catalog_is_checked_once(self) -> None:
        seen: list[str] = []
        generate_bundle.check_catalogs(["main", "main"], lambda n: seen.append(n) or {})
        assert seen == ["main"]

    def test_cli_flags_set_the_dev_catalog_and_check_both(self, tmp_path, monkeypatch) -> None:
        checked: list[str] = []
        monkeypatch.setattr(
            generate_bundle,
            "cli_catalog_lookup",
            lambda profile: lambda name: checked.append((profile, name)) or {"name": name},
        )
        monkeypatch.setattr(generate_bundle, "cli_pipeline_search", lambda profile: lambda f: [])
        monkeypatch.setattr(generate_bundle, "cli_pipeline_lookup", lambda profile: lambda i: None)
        monkeypatch.setattr(generate_bundle, "cli_current_user", lambda profile: "me@example.com")
        monkeypatch.setattr(
            generate_bundle, "cli_workspace_path_lookup", lambda profile: lambda p: None
        )
        monkeypatch.setattr(generate_bundle, "cli_table_list", lambda profile: lambda c, s: [])
        plan_path = tmp_path / "plan.json"
        plan_path.write_text(json.dumps(_plan(_item())))
        code = generate_bundle.main(
            [
                "-p",
                str(plan_path),
                "-o",
                str(tmp_path / "bundle"),
                "--dev-catalog",
                "sandbox",
                "--databricks-profile",
                "staging",
            ]
        )
        assert code == 0
        assert checked == [("staging", "sandbox"), ("staging", "main_prod")]
        root = yaml.safe_load((tmp_path / "bundle/databricks.yml").read_text())
        assert root["targets"]["dev"]["variables"]["dest_catalog"] == "sandbox"
        readme = (tmp_path / "bundle/README.md").read_text()
        assert "`sandbox` (dev): confirmed in the workspace" in readme
