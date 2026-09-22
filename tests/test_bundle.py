"""Tests for Databricks Asset Bundle generation."""

from __future__ import annotations

import json
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


def _manifest(files: dict) -> list[dict]:
    return json.loads(files["connections/connections.json"])["connections"]


class TestConnectionsBundle:
    def test_scriptable_connection_gets_a_connections_bundle(self) -> None:
        files = build_bundle(_plan(_item()), "acme")
        for path in (
            "connections/databricks.yml",
            "connections/resources/credentials.secret_scope.yml",
            "connections/resources/bootstrap_connections.job.yml",
            "connections/connections.json",
            "connections/src/bootstrap_connections.py",
            "connections/scripts/put_secrets.sh",
        ):
            assert path in files, path

    def test_no_legacy_script_and_no_placeholders_anywhere(self) -> None:
        files = build_bundle(_plan(_item()), "acme")
        assert "scripts/create_connections.sh" not in files
        assert not any("REPLACE_ME" in contents for contents in files.values())

    def test_secret_scope_is_named_after_the_bundle(self) -> None:
        files = build_bundle(_plan(_item()), "acme")
        scope = _load(files, "connections/resources/credentials.secret_scope.yml")
        assert scope["resources"]["secret_scopes"]["credentials"]["name"] == "acme-credentials"

    def test_bootstrap_job_is_serverless_and_reads_the_bundle_scope(self) -> None:
        files = build_bundle(_plan(_item()), "acme")
        job = _load(files, "connections/resources/bootstrap_connections.job.yml")["resources"][
            "jobs"
        ]["bootstrap_connections"]
        task = job["tasks"][0]
        assert "new_cluster" not in task and "existing_cluster_id" not in task
        assert task["environment_key"] == job["environments"][0]["environment_key"]
        assert job["environments"][0]["spec"]["environment_version"] == "5"
        params = task["spark_python_task"]["parameters"]
        assert params[params.index("--scope") + 1] == "${resources.secret_scopes.credentials.name}"
        assert task["spark_python_task"]["python_file"] == "../src/bootstrap_connections.py"

    def test_manifest_splits_fixed_options_from_customer_secrets(self) -> None:
        entry = _manifest(build_bundle(_plan(_item()), "acme"))[0]
        assert entry["name"] == "conn_sales_abc"
        assert entry["options"] == {"is_sandbox": "false"}
        assert set(entry["from_secrets"]) == {"instance_url", "client_id", "client_secret"}
        assert entry["options_json_secret"] is False

    def test_salesforce_mtls_asks_for_key_and_certificate_from_files(self) -> None:
        item = _item(scriptable="conditional", preferred_auth="OAUTH_MTLS")
        files = build_bundle(_plan(item), "acme")
        entry = _manifest(files)[0]
        assert {"client_private_key", "client_certificate"} <= set(entry["from_secrets"])
        assert entry["unverified_secret_keys"] is True
        script = files["connections/scripts/put_secrets.sh"]
        assert "put_file conn_sales_abc.client_private_key" in script
        assert "put_file conn_sales_abc.client_certificate" in script
        assert "put conn_sales_abc.client_id" in script

    def test_uncataloged_connection_type_takes_a_json_options_secret(self) -> None:
        item = _item(connection_type="PAGERDUTY")
        files = build_bundle(_plan(item), "acme")
        entry = _manifest(files)[0]
        assert entry["options_json_secret"] is True
        assert entry["from_secrets"] == []
        assert "put_file conn_sales_abc.options_json" in files["connections/scripts/put_secrets.sh"]

    def test_put_secrets_never_takes_values_on_the_command_line(self) -> None:
        script = build_bundle(_plan(_item()), "acme")["connections/scripts/put_secrets.sh"]
        assert "--string-value" not in script
        assert 'SCOPE="${2:-acme-credentials}"' in script

    def test_browser_oauth_connections_are_not_in_the_manifest(self) -> None:
        plan = _plan(
            _item(key="sf"),
            _item(key="hub", connection_type="HUBSPOT", scriptable="no"),
        )
        names = [e["name"] for e in _manifest(build_bundle(plan, "acme"))]
        assert names == ["conn_sf"]

    def test_blocked_connections_are_not_in_the_manifest(self) -> None:
        plan = _plan(_item(key="sf"), _item(key="s3", blockers=["no connector"]))
        names = [e["name"] for e in _manifest(build_bundle(plan, "acme"))]
        assert names == ["conn_sf"]

    def test_no_connections_bundle_when_nothing_is_scriptable(self) -> None:
        files = build_bundle(_plan(_item(connection_type="HUBSPOT", scriptable="no")), "acme")
        assert not any(path.startswith("connections/") for path in files)
        assert "put_secrets" not in files["README.md"]

    def test_ingestion_bundle_does_not_sync_the_connections_bundle(self) -> None:
        root = _load(build_bundle(_plan(_item()), "acme"), "databricks.yml")
        assert root["sync"]["exclude"] == ["connections/**"]

    def test_readme_orders_connections_before_the_ingestion_deploy(self) -> None:
        readme = build_bundle(_plan(_item()), "acme")["README.md"]
        assert readme.index("bundle run bootstrap_connections") < readme.index(
            "bundle deploy -t dev"
        )
        assert "`conn_sales_abc.client_secret`" in readme


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


class TestAutoDiscoverForManagedSaas:
    """When a managed SaaS connector has a cataloged supported-table list,
    the bundle should emit a schema-level ingestion spec (auto-discover)
    rather than an explicit per-table objects list."""

    def _item_with_supported_tables(self, **overrides) -> dict:
        item = _item(**overrides)
        item["target"]["has_supported_tables"] = True
        return item

    def test_saas_with_supported_tables_emits_schema_level_object(self) -> None:
        item = self._item_with_supported_tables(connection_type="PAGERDUTY")
        files = build_bundle(_plan(item), "acme")
        pipeline = _load(files, "resources/sales_abc.pipeline.yml")["resources"]["pipelines"][
            "sales_abc"
        ]
        objects = pipeline["ingestion_definition"]["objects"]
        assert len(objects) == 1
        assert "schema" in objects[0]
        assert "table" not in objects[0]
        assert objects[0]["schema"]["destination_catalog"] == "${var.dest_catalog}"

    def test_saas_without_supported_tables_emits_explicit_objects(self) -> None:
        item = _item()
        files = build_bundle(_plan(item), "acme")
        pipeline = _load(files, "resources/sales_abc.pipeline.yml")["resources"]["pipelines"][
            "sales_abc"
        ]
        objects = pipeline["ingestion_definition"]["objects"]
        assert "table" in objects[0]
        assert "schema" not in objects[0]

    def test_gateway_source_keeps_explicit_objects_even_with_supported_tables(self) -> None:
        item = self._item_with_supported_tables(
            connection_type="SQLSERVER", gateway="required"
        )
        item["target"]["category"] = "database_cdc"
        files = build_bundle(_plan(item), "acme")
        pipeline = _load(files, "resources/sales_abc.pipeline.yml")["resources"]["pipelines"][
            "sales_abc"
        ]
        objects = pipeline["ingestion_definition"]["objects"]
        assert "table" in objects[0]

    def test_auto_discover_uses_source_schema_from_objects(self) -> None:
        item = self._item_with_supported_tables(connection_type="PAGERDUTY")
        item["objects"][0]["source_schema"] = "default"
        files = build_bundle(_plan(item), "acme")
        pipeline = _load(files, "resources/sales_abc.pipeline.yml")["resources"]["pipelines"][
            "sales_abc"
        ]
        schema_spec = pipeline["ingestion_definition"]["objects"][0]["schema"]
        assert schema_spec["source_schema"] == "default"
