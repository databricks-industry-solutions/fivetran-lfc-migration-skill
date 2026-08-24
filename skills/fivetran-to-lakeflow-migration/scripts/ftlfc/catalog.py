"""Loader for the connector capability catalog.

All Lakeflow Connect facts (connector availability, API enum values, gateway
requirements, source-side prerequisites, pricing) live in
``scripts/data/connector_catalog.yaml`` rather than in code, because that data
changes on Databricks' release cadence and must be reviewable in a diff.
"""

from __future__ import annotations

import functools
from pathlib import Path
from typing import Any

import yaml

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
CATALOG_PATH = DATA_DIR / "connector_catalog.yaml"
PRICING_PATH = DATA_DIR / "pricing.yaml"

#: Verdicts, ordered from most to least migratable. The order is meaningful:
#: report sections and exit codes rely on it.
VERDICTS = ("supported", "supported_with_changes", "preview", "unsupported", "unknown")


class CatalogError(RuntimeError):
    """The catalog file is missing or structurally invalid."""


@functools.lru_cache(maxsize=None)
def load_catalog(path: str | None = None) -> dict[str, Any]:
    """Load and validate the connector catalog."""
    catalog_path = Path(path) if path else CATALOG_PATH
    data = _load_yaml(catalog_path)

    targets = data.get("targets")
    mappings = data.get("mappings")
    if not isinstance(targets, dict) or not targets:
        raise CatalogError(f"{catalog_path}: 'targets' must be a non-empty mapping")
    if not isinstance(mappings, dict) or not mappings:
        raise CatalogError(f"{catalog_path}: 'mappings' must be a non-empty mapping")

    for name, target in targets.items():
        missing = {"status", "source_type"} - set(target or {})
        if missing:
            raise CatalogError(
                f"{catalog_path}: target '{name}' is missing {sorted(missing)}"
            )

    for service, mapping in mappings.items():
        target_name = (mapping or {}).get("target")
        if target_name is not None and target_name not in targets:
            raise CatalogError(
                f"{catalog_path}: mapping '{service}' points at unknown target '{target_name}'"
            )
    return data


@functools.lru_cache(maxsize=None)
def load_pricing(path: str | None = None) -> dict[str, Any]:
    """Load the pricing assumptions used by the billing comparison."""
    return _load_yaml(Path(path) if path else PRICING_PATH)


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise CatalogError(f"missing data file: {path}")
    with path.open() as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise CatalogError(f"{path}: expected a YAML mapping at the top level")
    return data


def resolve(service: str, catalog: dict[str, Any] | None = None) -> dict[str, Any]:
    """Resolve a Fivetran ``service`` identifier to its Lakeflow Connect target.

    Returns a dict with ``verdict``, ``target`` (may be ``None``), ``notes``, and
    the merged target definition. Unknown services resolve to the ``unknown``
    verdict rather than raising, so a single unrecognised connector never aborts
    a migration plan.
    """
    catalog = catalog or load_catalog()
    mappings = catalog["mappings"]
    targets = catalog["targets"]

    mapping = mappings.get(service)
    if mapping is None:
        mapping = _resolve_by_alias(service, mappings)

    if mapping is None:
        return {
            "fivetran_service": service,
            "verdict": "unknown",
            "target": None,
            "target_name": None,
            "notes": [
                f"'{service}' is not in the connector catalog. Check the current "
                "Lakeflow Connect connector list and add an entry to "
                "data/connector_catalog.yaml."
            ],
            "source_prerequisites": [],
            "alternatives": [],
        }

    target_name = mapping.get("target")
    target = dict(targets[target_name]) if target_name else None
    verdict = mapping.get("verdict") or _default_verdict(target)

    return {
        "fivetran_service": service,
        "verdict": verdict,
        "target": target,
        "target_name": target_name,
        "notes": list(mapping.get("notes") or []),
        "source_prerequisites": list(
            mapping.get("source_prerequisites") or (target or {}).get("source_prerequisites") or []
        ),
        "alternatives": list(mapping.get("alternatives") or []),
    }


def _resolve_by_alias(service: str, mappings: dict[str, Any]) -> dict[str, Any] | None:
    for mapping in mappings.values():
        if service in (mapping or {}).get("fivetran_aliases", []):
            return mapping
    return None


def _default_verdict(target: dict[str, Any] | None) -> str:
    if target is None:
        return "unsupported"
    status = (target.get("status") or "").lower()
    if status in ("public_preview", "private_preview", "beta"):
        return "preview"
    if status == "ga":
        return "supported"
    return "unknown"


def verdict_rank(verdict: str) -> int:
    try:
        return VERDICTS.index(verdict)
    except ValueError:
        return len(VERDICTS)
