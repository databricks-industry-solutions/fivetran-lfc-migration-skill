"""Tests for the Fivetran client and inventory normalisation."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "skills/fivetran-to-lakeflow-migration/scripts"
sys.path.insert(0, str(SCRIPTS))

from ftlfc.fivetran import ACCEPT_V1, ACCEPT_V2, _accept_for, redact  # noqa: E402
from ftlfc.inventory import count_objects, flatten_schemas, summarise  # noqa: E402


class TestAcceptHeader:
    """Fivetran returns 406 rather than falling back, so this must be exact."""

    @pytest.mark.parametrize(
        "path",
        [
            "/v1/connections",
            "/v1/connections/",
            "/v1/connectors",
            "/v1/destinations/schoolmaster_heedless",
            "/v1/groups/abc/connections",
        ],
    )
    def test_v2_paths(self, path: str) -> None:
        assert _accept_for(path) == ACCEPT_V2

    @pytest.mark.parametrize(
        "path",
        [
            "/v1/groups",
            "/v1/account/info",
            "/v1/metadata/connector-types",
            "/v1/connections/speak_inexpensive/schemas",
            "/v1/connections/speak_inexpensive/schemas/public/tables/customers/columns",
            "/v1/transformations",
        ],
    )
    def test_v1_paths(self, path: str) -> None:
        assert _accept_for(path) == ACCEPT_V1

    def test_nested_connection_subpath_is_not_v2(self) -> None:
        # Only the bare collection is v2; sub-resources reject the v2 header.
        assert _accept_for("/v1/connections/abc/schemas") == ACCEPT_V1


class TestRedact:
    def test_masks_secret_like_keys(self) -> None:
        config = {"password": "hunter2", "api_secret": "s3cr3t", "host": "db.example.com"}
        assert redact(config) == {
            "password": "***REDACTED***",
            "api_secret": "***REDACTED***",
            "host": "db.example.com",
        }

    def test_preserves_structural_keys_that_look_secret(self) -> None:
        config = {"auth_type": "OAUTH2", "public_key": "abc", "primary_keys": ["id"]}
        assert redact(config) == config

    def test_recurses_into_nested_structures(self) -> None:
        config = {"tunnel": {"ssh_password": "x"}, "hosts": [{"token": "y"}]}
        result = redact(config)
        assert result["tunnel"]["ssh_password"] == "***REDACTED***"
        assert result["hosts"][0]["token"] == "***REDACTED***"

    def test_leaves_none_as_none(self) -> None:
        assert redact({"password": None}) == {"password": None}


SCHEMAS_RESPONSE = {
    "enable_new_by_default": True,
    "schema_change_handling": "ALLOW_COLUMNS",
    "schemas": {
        "public": {
            "name_in_destination": "public_prod",
            "enabled": True,
            "tables": {
                "customers": {
                    "name_in_destination": "customers",
                    "enabled": True,
                    "sync_mode": "HISTORY",
                    "supports_columns_config": True,
                    "supports_history_mode": True,
                    "columns": {
                        "id": {"enabled": True, "hashed": False, "is_primary_key": True},
                        "email": {"enabled": True, "hashed": True, "is_primary_key": False},
                        "ssn": {"enabled": False, "hashed": False, "is_primary_key": False},
                    },
                },
                # No column overrides: every column syncs and the PK is unknown.
                "orders": {
                    "enabled": True,
                    "sync_mode": "SOFT_DELETE",
                    "supports_columns_config": True,
                    "columns": {},
                },
                "audit_log": {
                    "enabled": False,
                    "supports_columns_config": False,
                    "enabled_patch_settings": {"allowed": False, "reason_code": "SYSTEM_TABLE"},
                    "columns": {},
                },
            },
        },
        # A disabled schema disables its tables regardless of their own flag.
        "staging": {
            "enabled": False,
            "tables": {"tmp": {"enabled": True, "columns": {}}},
        },
    },
}


class TestFlattenSchemas:
    @pytest.fixture
    def tables(self) -> dict[str, dict]:
        return {t["source_table"]: t for t in flatten_schemas(SCHEMAS_RESPONSE)}

    def test_flattens_every_table(self, tables: dict[str, dict]) -> None:
        assert set(tables) == {"customers", "orders", "audit_log", "tmp"}

    def test_disabled_schema_disables_its_tables(self, tables: dict[str, dict]) -> None:
        assert tables["tmp"]["enabled"] is False
        assert tables["tmp"]["schema_enabled"] is False

    def test_history_sync_mode_maps_to_scd2(self, tables: dict[str, dict]) -> None:
        assert tables["customers"]["retains_history"] is True
        assert tables["orders"]["retains_history"] is False

    def test_absent_sync_mode_is_not_history(self, tables: dict[str, dict]) -> None:
        assert tables["audit_log"]["sync_mode"] is None
        assert tables["audit_log"]["retains_history"] is False

    def test_destination_names_fall_back_to_source(self, tables: dict[str, dict]) -> None:
        assert tables["customers"]["destination_schema"] == "public_prod"
        assert tables["orders"]["destination_schema"] == "public_prod"
        assert tables["orders"]["destination_table"] == "orders"

    def test_excluded_and_hashed_columns_are_extracted(self, tables: dict[str, dict]) -> None:
        assert tables["customers"]["excluded_columns"] == ["ssn"]
        assert tables["customers"]["hashed_columns"] == ["email"]

    def test_primary_keys_known_when_override_present(self, tables: dict[str, dict]) -> None:
        assert tables["customers"]["primary_keys"] == ["id"]
        assert tables["customers"]["primary_keys_known"] is True

    def test_primary_keys_unknown_when_no_overrides(self, tables: dict[str, dict]) -> None:
        # The critical case: an empty columns map means "nobody customised this",
        # not "this table has no primary key". Emitting SCD config on the latter
        # assumption would silently produce a wrong pipeline.
        assert tables["orders"]["primary_keys"] == []
        assert tables["orders"]["primary_keys_known"] is False
        assert tables["orders"]["columns_complete"] is False


class TestCounts:
    def test_counts_only_enabled_tables(self) -> None:
        counts = count_objects(flatten_schemas(SCHEMAS_RESPONSE))
        assert counts["tables_total"] == 4
        assert counts["tables_enabled"] == 2
        assert counts["tables_history_mode"] == 1
        assert counts["columns_hashed"] == 1
        assert counts["columns_excluded"] == 1
        assert counts["tables_primary_keys_unknown"] == 1


class TestSummarise:
    def test_aggregates_across_connections(self) -> None:
        connections = [
            {
                "service": "salesforce",
                "paused": False,
                "networking_method": "Directly",
                "status": {"setup_state": "connected"},
                "counts": {"tables_enabled": 10, "tables_history_mode": 2},
            },
            {
                "service": "salesforce",
                "paused": True,
                "networking_method": "PrivateLink",
                "status": {"setup_state": "connected"},
                "counts": {"tables_enabled": 5, "tables_history_mode": 0},
            },
            {
                "service": "postgres",
                "paused": False,
                "networking_method": "SshTunnel",
                "status": {"setup_state": "broken"},
                "counts": {"tables_enabled": 3, "tables_history_mode": 1},
            },
        ]
        summary = summarise(connections)
        assert summary["connections_total"] == 3
        assert summary["connections_active"] == 2
        assert summary["connections_paused"] == 1
        assert summary["connections_broken"] == 1
        assert summary["distinct_services"] == 2
        assert summary["connections_by_service"] == {"salesforce": 2, "postgres": 1}
        assert summary["tables_enabled"] == 18
        assert summary["tables_history_mode"] == 3
        # Only active connections count toward networking work.
        assert summary["non_direct_networking"] == 1
