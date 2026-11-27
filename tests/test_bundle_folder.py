"""Tests for writing a bundle into an existing folder and for pipeline name clashes."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

SCRIPTS = Path(__file__).resolve().parents[1] / "skills/fivetran-to-lakeflow-migration/scripts"
sys.path.insert(0, str(SCRIPTS))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import generate_bundle
from ftlfc.bundle import build_bundle, is_pipeline_name_for, rename_pipeline_keys
from ftlfc.workspace import WorkspaceError, cli_pipeline_search, cli_workspace_path_lookup
from test_bundle import _item, _plan

OTHER_BUNDLE = (
    "/Workspace/Users/me@example.com/.bundle/fivetran_migration_test/dev/state/metadata.json"
)
OWN_BUNDLE = "/Workspace/Users/me@example.com/.bundle/acme/dev/state/metadata.json"


def _pipelines(**deployments: str | None):
    """A fake workspace: pipeline name -> metadata_file_path (None = not from a bundle)."""
    by_id = {f"id-{i}": (name, path) for i, (name, path) in enumerate(deployments.items())}

    def search(fragment: str) -> list[dict]:
        return [
            {"pipeline_id": pid, "name": name}
            for pid, (name, _) in by_id.items()
            if fragment in name
        ]

    def get(pipeline_id: str) -> dict:
        _, path = by_id[pipeline_id]
        spec = {"deployment": {"kind": "BUNDLE", "metadata_file_path": path}} if path else {}
        return {"pipeline_id": pipeline_id, "spec": spec}

    return search, get


def _write_plan(tmp_path: Path, plan: dict) -> Path:
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan))
    return path


def _seed_leftover_bundle(folder: Path) -> None:
    (folder / "resources").mkdir(parents=True)
    (folder / "databricks.yml").write_text("bundle: {name: fivetran_migration}\n")
    (folder / "resources/salesforce_1.pipeline.yml").write_text("resources: {}\n")
    (folder / "resources/erp_2.pipeline.yml").write_text("resources: {}\n")


class TestPipelineNameMatch:
    @pytest.mark.parametrize(
        "name",
        [
            "lfc-pd_main-dev",
            "lfc-pd_main-prod",
            "[dev jane_doe] lfc-pd_main-dev",
            "lfc-pd_main-gateway-prod",
        ],
    )
    def test_matches_names_this_bundle_would_deploy(self, name: str) -> None:
        assert is_pipeline_name_for(name, "pd_main")

    @pytest.mark.parametrize(
        "name", ["lfc-pd_main_2-dev", "lfc-pdXmain-dev", "my lfc-pd_main-dev", "lfc-pd_main"]
    )
    def test_ignores_other_names(self, name: str) -> None:
        assert not is_pipeline_name_for(name, "pd_main")


class TestFindNameClashes:
    def test_pipeline_from_another_bundle_is_a_clash(self) -> None:
        search, get = _pipelines(**{"[dev me] lfc-pd-dev": OTHER_BUNDLE})
        assert generate_bundle.find_name_clashes(["pd"], "acme", search, get) == {"pd": "pd_2"}

    def test_pipeline_created_by_hand_is_a_clash(self) -> None:
        search, get = _pipelines(**{"lfc-pd-prod": None})
        assert generate_bundle.find_name_clashes(["pd"], "acme", search, get) == {"pd": "pd_2"}

    def test_pipeline_from_this_bundle_is_not_a_clash(self) -> None:
        search, get = _pipelines(**{"[dev me] lfc-pd-dev": OWN_BUNDLE})
        assert generate_bundle.find_name_clashes(["pd"], "acme", search, get) == {}

    def test_suffix_skips_names_that_are_also_taken(self) -> None:
        search, get = _pipelines(**{"lfc-pd-dev": OTHER_BUNDLE, "lfc-pd_2-dev": OTHER_BUNDLE})
        assert generate_bundle.find_name_clashes(["pd"], "acme", search, get) == {"pd": "pd_3"}

    def test_suffix_does_not_collide_with_another_key_in_the_plan(self) -> None:
        search, get = _pipelines(**{"lfc-pd-dev": OTHER_BUNDLE})
        renames = generate_bundle.find_name_clashes(["pd", "pd_2"], "acme", search, get)
        assert renames == {"pd": "pd_3"}

    def test_rename_applies_to_pipeline_job_and_gateway(self) -> None:
        plan = rename_pipeline_keys(
            _plan(_item(key="pd", connection_type="SQLSERVER", gateway="required")), {"pd": "pd_2"}
        )
        files = build_bundle(plan, "acme", renamed_keys={"pd": "pd_2"})
        assert {
            "resources/pd_2.pipeline.yml",
            "resources/pd_2.job.yml",
            "resources/pd_2_gateway.pipeline.yml",
        } <= set(files)
        pipeline = yaml.safe_load(files["resources/pd_2.pipeline.yml"])["resources"]["pipelines"]
        assert pipeline["pd_2"]["name"] == "lfc-pd_2-${bundle.target}"
        assert "`pd` -> `pd_2`" in files["README.md"]
        assert "Do not bind" in files["README.md"]


class TestExistingFolder:
    def test_non_empty_folder_lists_the_bundle_it_holds(self, tmp_path, capsys) -> None:
        _seed_leftover_bundle(tmp_path / "bundle")
        code = generate_bundle.main(
            ["-p", str(_write_plan(tmp_path, _plan(_item()))), "-o", str(tmp_path / "bundle")]
        )
        assert code == 1
        err = capsys.readouterr().err
        assert "resources/salesforce_1.pipeline.yml" in err and "new -o folder" in err

    def test_force_refuses_leftover_resources_and_writes_nothing(self, tmp_path, capsys) -> None:
        folder = tmp_path / "bundle"
        _seed_leftover_bundle(folder)
        code = generate_bundle.main(
            ["-p", str(_write_plan(tmp_path, _plan(_item()))), "-o", str(folder), "--force"]
        )
        assert code == 1
        err = capsys.readouterr().err
        assert "resources/erp_2.pipeline.yml" in err and "--keep-existing-resources" in err
        assert not (folder / "resources/sales_abc.pipeline.yml").exists()
        assert "fivetran_migration" in (folder / "databricks.yml").read_text()

    def test_keep_existing_resources_lists_them_in_the_readme(self, tmp_path) -> None:
        folder = tmp_path / "bundle"
        _seed_leftover_bundle(folder)
        code = generate_bundle.main(
            [
                "-p",
                str(_write_plan(tmp_path, _plan(_item()))),
                "-o",
                str(folder),
                "--name",
                "acme",
                "--force",
                "--keep-existing-resources",
            ]
        )
        assert code == 0
        readme = (folder / "README.md").read_text()
        assert "`resources/salesforce_1.pipeline.yml`" in readme
        assert "deploy with this bundle" in readme

    def test_force_regenerating_the_same_plan_needs_no_flag(self, tmp_path) -> None:
        folder = tmp_path / "bundle"
        plan_path = _write_plan(tmp_path, _plan(_item()))
        assert generate_bundle.main(["-p", str(plan_path), "-o", str(folder)]) == 0
        assert generate_bundle.main(["-p", str(plan_path), "-o", str(folder), "--force"]) == 0

    def test_clash_is_renamed_end_to_end(self, tmp_path, monkeypatch, capsys) -> None:
        search, get = _pipelines(**{"[dev me] lfc-sales_abc-dev": OTHER_BUNDLE})
        monkeypatch.setattr(generate_bundle, "cli_catalog_lookup", lambda p: lambda n: {})
        monkeypatch.setattr(generate_bundle, "cli_pipeline_search", lambda p: search)
        monkeypatch.setattr(generate_bundle, "cli_pipeline_lookup", lambda p: get)
        monkeypatch.setattr(generate_bundle, "cli_current_user", lambda p: "me@example.com")
        monkeypatch.setattr(generate_bundle, "cli_workspace_path_lookup", lambda p: lambda x: None)
        monkeypatch.setattr(generate_bundle, "cli_table_list", lambda p: lambda c, s: [])
        folder = tmp_path / "bundle"
        code = generate_bundle.main(
            [
                "-p",
                str(_write_plan(tmp_path, _plan(_item()))),
                "-o",
                str(folder),
                "--name",
                "acme",
                "--databricks-profile",
                "staging",
            ]
        )
        assert code == 0
        assert (folder / "resources/sales_abc_2.pipeline.yml").exists()
        assert not (folder / "resources/sales_abc.pipeline.yml").exists()
        assert "using key 'sales_abc_2'" in capsys.readouterr().err

    def test_existing_bundle_state_warns(self, monkeypatch, capsys) -> None:
        monkeypatch.setattr(generate_bundle, "cli_current_user", lambda p: "me@example.com")
        monkeypatch.setattr(
            generate_bundle, "cli_workspace_path_lookup", lambda p: lambda x: {"path": x}
        )
        generate_bundle.check_bundle_state("acme", "staging")
        err = capsys.readouterr().err
        assert "/Workspace/Users/me@example.com/.bundle/acme exists" in err
        assert "different --name" in err

    def test_name_check_failure_warns_and_renames_nothing(self, monkeypatch, capsys) -> None:
        def broken(profile):
            def search(fragment):
                raise WorkspaceError("refresh token is invalid")

            return search

        monkeypatch.setattr(generate_bundle, "cli_pipeline_search", broken)
        monkeypatch.setattr(generate_bundle, "cli_pipeline_lookup", lambda p: lambda i: None)
        assert generate_bundle.check_pipeline_names(_plan(_item()), "acme", "staging") == {}
        assert "could not check pipeline names" in capsys.readouterr().err


class TestCliCommands:
    def test_pipeline_search_uses_a_name_filter(self) -> None:
        calls: list[list[str]] = []

        def run(command, **kwargs):
            calls.append(command)
            listing = '[{"pipeline_id": "p", "name": "x"}]'
            return subprocess.CompletedProcess(command, 0, listing, "")

        assert cli_pipeline_search("staging", runner=run)("lfc-pd-") == [
            {"pipeline_id": "p", "name": "x"}
        ]
        assert calls[0][:5] == [
            "databricks",
            "pipelines",
            "list-pipelines",
            "--filter",
            "name LIKE '%lfc-pd-%'",
        ]

    def test_missing_workspace_path_is_none(self) -> None:
        def run(command, **kwargs):
            return subprocess.CompletedProcess(
                command, 1, "", "Error: Path (/Workspace/Users/x/.bundle/acme) doesn't exist."
            )

        assert cli_workspace_path_lookup("staging", runner=run)("/Workspace/x") is None
