"""Validates generated bundles against the real Databricks Asset Bundle schema.

The unit tests in test_bundle.py assert on the fields we deliberately emit. They
cannot catch a field we spelled wrong, nested one level too deep, or that the
Databricks CLI has since renamed, because they only ever look where we expect.
Checking the merged bundle against the schema the CLI itself publishes is the
only oracle for that class of mistake, and the bundle emitter is the component
where such a mistake reaches a customer's workspace.

Requires the `databricks` CLI on PATH to produce the schema, and jsonschema to
check against it. Skips when either is missing so the suite stays runnable
offline.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

SCRIPTS = Path(__file__).resolve().parents[1] / "skills/fivetran-to-lakeflow-migration/scripts"
sys.path.insert(0, str(SCRIPTS))

from ftlfc.bundle import build_bundle
from ftlfc.catalog import CATALOG
from ftlfc.mapping import build_plan

jsonschema = pytest.importorskip("jsonschema", reason="jsonschema not installed")

FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "skills/fivetran-to-lakeflow-migration/fixtures/inventory.sample.json"
)


@pytest.fixture(scope="module")
def dab_schema() -> dict:
    """The bundle JSON schema, straight from the installed CLI."""
    try:
        proc = subprocess.run(
            ["databricks", "bundle", "schema"],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        pytest.skip(f"databricks CLI unavailable: {exc}")
    if proc.returncode != 0:
        pytest.skip(f"databricks bundle schema failed: {proc.stderr[:200]}")
    return json.loads(proc.stdout)


def _merged_bundle(files: dict[str, str], root_dir: str = "") -> dict:
    """Recombine the emitted files the way the CLI would when it loads a bundle."""
    root = yaml.safe_load(files[f"{root_dir}databricks.yml"])
    resources: dict[str, dict] = {}
    for name, text in files.items():
        if not name.startswith(f"{root_dir}resources/"):
            continue
        for kind, entries in (yaml.safe_load(text).get("resources") or {}).items():
            resources.setdefault(kind, {}).update(entries)
    root["resources"] = resources
    return root


def _plan_from_fixture() -> dict:
    return build_plan(json.loads(FIXTURE.read_text()), target_catalog="main_prod")


def _assert_valid(dab_schema: dict, merged: dict) -> None:
    errors = sorted(
        jsonschema.Draft7Validator(dab_schema).iter_errors(merged),
        key=lambda e: list(e.path),
    )
    assert not errors, "\n".join(
        "/" + "/".join(map(str, e.path)) + " -> " + e.message for e in errors[:20]
    )


def test_generated_bundle_matches_dab_schema(dab_schema: dict) -> None:
    files = build_bundle(_plan_from_fixture(), bundle_name="acme-lfc", host=None)
    _assert_valid(dab_schema, _merged_bundle(files))


def test_connections_bundle_matches_dab_schema(dab_schema: dict) -> None:
    files = build_bundle(
        _plan_from_fixture(), bundle_name="acme-lfc", host="https://example.cloud.databricks.com"
    )
    merged = _merged_bundle(files, root_dir="connections/")
    assert set(merged["resources"]) == {"secret_scopes", "jobs"}
    _assert_valid(dab_schema, merged)


def _enum_containing(schema: dict, member: str) -> set[str]:
    """Find the enum in the schema that contains a known member."""
    found: set[str] = set()

    def walk(node: object) -> None:
        if isinstance(node, dict):
            values = node.get("enum")
            if isinstance(values, list) and member in values:
                found.update(v for v in values if isinstance(v, str))
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(schema)
    return found


def test_catalog_connection_types_exist_in_sdk_enum(dab_schema: dict) -> None:
    """Every connector we claim to target must be a type Databricks recognises.

    A plausible-looking but wrong type (MONDAY for MONDAY_COM, or SALESFORCE for
    Salesforce Marketing Cloud) survives every other test here and only fails
    later, against the customer's workspace, when the connection is created.
    """
    source_types = _enum_containing(dab_schema, "SALESFORCE")
    assert source_types, "could not locate IngestionSourceType enum in the bundle schema"

    unknown = sorted(
        {
            target.connection_type
            for target in CATALOG.values()
            if target.connection_type and target.connection_type not in source_types
        }
    )
    assert not unknown, f"connection types absent from IngestionSourceType: {unknown}"


def test_fixture_exercises_both_pipeline_shapes() -> None:
    """Guards the test above: a fixture with no gateway would validate vacuously."""
    files = build_bundle(_plan_from_fixture(), bundle_name="acme-lfc", host=None)
    pipelines = [n for n in files if n.endswith(".pipeline.yml")]
    assert any("gateway" in n for n in pipelines), "fixture no longer covers CDC gateways"
    assert any("gateway" not in n for n in pipelines), "fixture no longer covers serverless SaaS"
