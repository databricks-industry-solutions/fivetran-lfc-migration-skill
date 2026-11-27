"""Tests for limiting Fivetran discovery to the connections the customer named."""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / "skills/fivetran-to-lakeflow-migration/scripts"
sys.path.insert(0, str(SCRIPTS))

import fivetran_discover
from ftlfc.inventory import build_inventory


class FakeClient:
    def __init__(self) -> None:
        self.warnings: list[str] = []

    def get_account_info(self) -> dict:
        return {"account_id": "acc", "system_key_id": "k"}

    def list_groups(self) -> list[dict]:
        return [{"id": "g1", "name": "Group"}]

    def get_destination(self, group_id: str) -> dict:
        return {"id": group_id, "service": "databricks"}

    def list_connections(self, group_id: str) -> list[dict]:
        return [
            {"id": "c_pd", "service": "pagerduty", "schema": "it_data_pagerduty_bronze"},
            {"id": "c_sf", "service": "salesforce", "schema": "it_sfdc_bronze"},
        ]

    def list_connector_types(self) -> list[dict]:
        return []

    def list_transformations(self) -> list[dict]:
        return []


def _ids(inventory: dict) -> list[str]:
    return [c["id"] for c in inventory["connections"]]


def test_all_connections_by_default() -> None:
    assert _ids(build_inventory(FakeClient(), include_schemas=False)) == ["c_pd", "c_sf"]


def test_limit_by_schema_name_or_id() -> None:
    by_schema = build_inventory(
        FakeClient(), include_schemas=False, connection_names=["it_data_pagerduty_bronze"]
    )
    assert _ids(by_schema) == ["c_pd"]
    by_id = build_inventory(FakeClient(), include_schemas=False, connection_names=["c_sf"])
    assert _ids(by_id) == ["c_sf"]


def test_no_match_is_an_error_and_writes_nothing(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(fivetran_discover, "build_client", lambda args: FakeClient())
    out = tmp_path / "inventory.json"
    code = fivetran_discover.main(["-o", str(out), "--no-schemas", "--connection", "nope"])
    assert code == 1
    assert "no connection matched nope; nothing was written" in capsys.readouterr().err
    assert not out.exists()
