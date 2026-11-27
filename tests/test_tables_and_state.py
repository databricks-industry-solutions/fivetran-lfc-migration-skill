"""Tests for dev schemas, destination table ownership, and spend left by an earlier session."""

from __future__ import annotations

import datetime as dt
import json
import subprocess
import sys
from pathlib import Path

import yaml

SCRIPTS = Path(__file__).resolve().parents[1] / "skills/fivetran-to-lakeflow-migration/scripts"
sys.path.insert(0, str(SCRIPTS))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import fivetran_consumption
import generate_bundle
from ftlfc.bundle import build_bundle
from ftlfc.consumption import load_spend, record_spend, save_spend, spend_recorded_before
from ftlfc.workspace import WorkspaceError, cli_table_list
from test_bundle import _item, _plan

OTHER = "/Workspace/Users/me@example.com/.bundle/fivetran_migration_priyal/dev/state/metadata.json"
OWN = "/Workspace/Users/me@example.com/.bundle/acme/dev/state/metadata.json"


def _workspace(tables: dict[tuple[str, str], list[dict]], pipelines: dict[str, dict]):
    def list_tables(catalog: str, schema: str) -> list[dict]:
        return tables.get((catalog, schema), [])

    def get(pipeline_id: str) -> dict | None:
        return pipelines.get(pipeline_id)

    return list_tables, get


def _pipeline(name: str, path: str | None) -> dict:
    spec = {"deployment": {"kind": "BUNDLE", "metadata_file_path": path}} if path else {}
    return {"name": name, "spec": spec}


class TestDevSchema:
    def test_dev_target_sets_a_schema_suffix(self) -> None:
        root = yaml.safe_load(build_bundle(_plan(_item()), "acme")["databricks.yml"])
        assert root["variables"]["schema_suffix"]["default"] == ""
        assert root["targets"]["dev"]["variables"]["schema_suffix"] == "_dev"
        assert "schema_suffix" not in root["targets"]["prod"]["variables"]

    def test_pipeline_and_objects_use_the_suffix(self) -> None:
        files = build_bundle(_plan(_item()), "acme")
        pipeline = yaml.safe_load(files["resources/sales_abc.pipeline.yml"])["resources"][
            "pipelines"
        ]["sales_abc"]
        assert pipeline["schema"] == "sales${var.schema_suffix}"
        table = pipeline["ingestion_definition"]["objects"][0]["table"]
        assert table["destination_schema"] == "sales${var.schema_suffix}"

    def test_readme_says_dev_has_its_own_schemas(self) -> None:
        assert "`<schema>_dev`" in build_bundle(_plan(_item()), "acme")["README.md"]


class TestTableConflicts:
    def test_table_owned_by_another_bundle_is_a_conflict(self) -> None:
        list_tables, get = _workspace(
            {("main_prod", "sales"): [{"name": "account", "pipeline_id": "p1"}]},
            {"p1": _pipeline("[dev me] lfc-sales-dev", OTHER)},
        )
        conflicts = generate_bundle.find_table_conflicts(
            _plan(_item()), "main_prod_dev", "acme", list_tables, get
        )
        assert conflicts == [
            {
                "target": "prod",
                "table": "main_prod.sales.account",
                "owner": (
                    "pipeline `p1` ([dev me] lfc-sales-dev), bundle `fivetran_migration_priyal`"
                ),
            }
        ]

    def test_dev_is_checked_in_its_own_schema(self) -> None:
        list_tables, get = _workspace(
            {("main_prod_dev", "sales_dev"): [{"name": "account", "pipeline_id": "p1"}]},
            {"p1": _pipeline("lfc-x-dev", None)},
        )
        conflicts = generate_bundle.find_table_conflicts(
            _plan(_item()), "main_prod_dev", "acme", list_tables, get
        )
        assert [(c["target"], c["table"]) for c in conflicts] == [
            ("dev", "main_prod_dev.sales_dev.account")
        ]
        assert "not from a bundle" in conflicts[0]["owner"]

    def test_table_from_this_bundle_is_not_a_conflict(self) -> None:
        list_tables, get = _workspace(
            {("main_prod_dev", "sales_dev"): [{"name": "account", "pipeline_id": "p1"}]},
            {"p1": _pipeline("[dev me] lfc-sales_abc-dev", OWN)},
        )
        assert (
            generate_bundle.find_table_conflicts(
                _plan(_item()), "main_prod_dev", "acme", list_tables, get
            )
            == []
        )

    def test_plain_table_is_a_conflict(self) -> None:
        list_tables, get = _workspace(
            {("main_prod", "sales"): [{"name": "account", "table_type": "MANAGED"}]}, {}
        )
        conflicts = generate_bundle.find_table_conflicts(
            _plan(_item()), "main_prod_dev", "acme", list_tables, get
        )
        assert conflicts[0]["owner"] == "MANAGED not owned by a pipeline"

    def test_each_schema_is_listed_once(self) -> None:
        calls: list[tuple[str, str]] = []
        item = _item()
        item["objects"].append({**item["objects"][0], "destination_table": "contact"})
        generate_bundle.find_table_conflicts(
            _plan(item), "dev_cat", "acme", lambda c, s: calls.append((c, s)) or [], lambda i: None
        )
        assert calls == [("dev_cat", "sales_dev"), ("main_prod", "sales")]

    def test_readme_lists_conflicts(self) -> None:
        readme = build_bundle(
            _plan(_item()),
            "acme",
            table_conflicts=[{"target": "dev", "table": "c.s.t", "owner": "pipeline `p1`"}],
        )["README.md"]
        assert "## Table conflicts" in readme and "| dev | `c.s.t` | pipeline `p1` |" in readme

    def test_lookup_failure_warns_and_reports_nothing(self, monkeypatch, capsys) -> None:
        def broken(profile):
            def list_tables(catalog, schema):
                raise WorkspaceError("refresh token is invalid")

            return list_tables

        monkeypatch.setattr(generate_bundle, "cli_table_list", broken)
        monkeypatch.setattr(generate_bundle, "cli_pipeline_lookup", lambda p: lambda i: None)
        assert generate_bundle.check_tables(_plan(_item()), "dev_cat", "acme", "staging") == []
        assert "could not check destination tables" in capsys.readouterr().err

    def test_cli_lists_tables_and_treats_a_missing_schema_as_empty(self) -> None:
        calls: list[list[str]] = []

        def run(command, **kwargs):
            calls.append(command)
            if command[4] == "missing":
                return subprocess.CompletedProcess(
                    command, 1, "", "Error: Schema 'main.missing' does not exist."
                )
            return subprocess.CompletedProcess(command, 0, '[{"name": "users"}]', "")

        list_tables = cli_table_list("staging", runner=run)
        assert list_tables("main", "pd") == [{"name": "users"}]
        assert list_tables("main", "missing") == []
        assert calls[0][:6] == ["databricks", "tables", "list", "main", "pd", "--omit-columns"]


class TestEarlierSpend:
    START = dt.datetime(2026, 11, 25, 14, 0, tzinfo=dt.timezone.utc)

    def _entries(self) -> list[dict]:
        old = record_spend(
            [],
            month="2026-10",
            monthly_usd=6850,
            basis="invoice",
            now=dt.datetime(2026, 11, 1, tzinfo=dt.timezone.utc),
        )
        return record_spend(
            old,
            month="2026-11",
            monthly_usd=7000,
            basis="estimate",
            now=dt.datetime(2026, 11, 25, 15, 0, tzinfo=dt.timezone.utc),
        )

    def test_only_entries_before_the_session_are_earlier(self) -> None:
        earlier = spend_recorded_before(self._entries(), self.START)
        assert [e["period_month"] for e in earlier] == ["2026-10-01"]

    def test_without_a_session_start_every_entry_is_listed(self) -> None:
        assert len(spend_recorded_before(self._entries(), None)) == 2

    def test_unreadable_recorded_at_counts_as_earlier(self) -> None:
        assert spend_recorded_before([{"period_month": "2026-01-01"}], self.START) == [
            {"period_month": "2026-01-01"}
        ]

    def test_review_lists_without_changing_the_file(self, tmp_path, capsys) -> None:
        path = tmp_path / "fivetran_spend.json"
        save_spend(path, self._entries())
        before = path.read_text()
        code = fivetran_consumption.main(
            ["review-spend", "--spend", str(path), "--since", self.START.isoformat()]
        )
        assert code == 0
        out = capsys.readouterr().out
        assert "2026-10-01: 6850.0 USD/month" in out and "recorded 2026-11-01" in out
        assert "2026-11-01: 7000" not in out
        assert "keep or clear" in out
        assert path.read_text() == before

    def test_clear_removes_only_earlier_entries(self, tmp_path) -> None:
        path = tmp_path / "fivetran_spend.json"
        save_spend(path, self._entries())
        code = fivetran_consumption.main(
            ["review-spend", "--spend", str(path), "--since", self.START.isoformat(), "--clear"]
        )
        assert code == 0
        assert [e["period_month"] for e in load_spend(path)] == ["2026-11-01"]

    def test_failed_clear_says_nothing_was_cleared(self, tmp_path, monkeypatch, capsys) -> None:
        path = tmp_path / "fivetran_spend.json"
        save_spend(path, self._entries())
        before = path.read_text()

        def blocked(*args, **kwargs):
            raise PermissionError("read-only file system")

        monkeypatch.setattr(fivetran_consumption, "save_spend", blocked)
        code = fivetran_consumption.main(
            ["review-spend", "--spend", str(path), "--since", self.START.isoformat(), "--clear"]
        )
        assert code == 1
        err = capsys.readouterr().err
        assert "nothing was cleared" in err and "1 earlier entry remain" in err
        assert "ask how to proceed" in err
        assert path.read_text() == before

    def test_no_earlier_spend(self, tmp_path, capsys) -> None:
        code = fivetran_consumption.main(["review-spend", "--spend", str(tmp_path / "none.json")])
        assert code == 0
        assert "No Fivetran spend from an earlier session" in capsys.readouterr().out

    def test_end_to_end_conflict_lands_in_the_readme(self, tmp_path, monkeypatch) -> None:
        list_tables, get = _workspace(
            {("main_prod_dev", "sales_dev"): [{"name": "account", "pipeline_id": "p1"}]},
            {"p1": _pipeline("[dev me] lfc-sales-dev", OTHER)},
        )
        monkeypatch.setattr(generate_bundle, "cli_catalog_lookup", lambda p: lambda n: {})
        monkeypatch.setattr(generate_bundle, "cli_pipeline_search", lambda p: lambda f: [])
        monkeypatch.setattr(generate_bundle, "cli_pipeline_lookup", lambda p: get)
        monkeypatch.setattr(generate_bundle, "cli_table_list", lambda p: list_tables)
        monkeypatch.setattr(generate_bundle, "cli_current_user", lambda p: "me@example.com")
        monkeypatch.setattr(generate_bundle, "cli_workspace_path_lookup", lambda p: lambda x: None)
        plan_path = tmp_path / "plan.json"
        plan_path.write_text(json.dumps(_plan(_item())))
        code = generate_bundle.main(
            [
                "-p",
                str(plan_path),
                "-o",
                str(tmp_path / "b"),
                "--name",
                "acme",
                "--databricks-profile",
                "staging",
            ]
        )
        assert code == 0
        readme = (tmp_path / "b/README.md").read_text()
        assert "`main_prod_dev.sales_dev.account`" in readme
        assert "fivetran_migration_priyal" in readme


def test_missing_dev_catalog_writes_dev_to_the_target_catalog(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        generate_bundle,
        "cli_catalog_lookup",
        lambda p: lambda n: {"name": n} if n == "main_prod" else None,
    )
    monkeypatch.setattr(generate_bundle, "cli_pipeline_search", lambda p: lambda f: [])
    monkeypatch.setattr(generate_bundle, "cli_pipeline_lookup", lambda p: lambda i: None)
    monkeypatch.setattr(generate_bundle, "cli_table_list", lambda p: lambda c, s: [])
    monkeypatch.setattr(generate_bundle, "cli_current_user", lambda p: "me@example.com")
    monkeypatch.setattr(generate_bundle, "cli_workspace_path_lookup", lambda p: lambda x: None)
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(_plan(_item())))
    args = ["-p", str(plan_path), "-o", str(tmp_path / "b"), "--name", "acme"]
    assert generate_bundle.main([*args, "--databricks-profile", "staging"]) == 0
    root = yaml.safe_load((tmp_path / "b/databricks.yml").read_text())
    dev = root["targets"]["dev"]["variables"]
    assert dev["dest_catalog"] == "main_prod"
    assert dev["schema_suffix"] == "_dev"
