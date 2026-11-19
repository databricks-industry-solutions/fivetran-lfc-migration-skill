"""Turn a Fivetran inventory into a Lakeflow Connect migration plan.

The plan is the decision artifact: for every Fivetran connection it records the
target Lakeflow Connect shape, everything that has to happen first, and every
reason the migration might not be faithful. It is deliberately conservative --
where the inventory is uncertain, the plan says so rather than guessing, because
a silently wrong pipeline is worse than a flagged gap.
"""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from . import PLAN_SCHEMA_VERSION
from .catalog import Availability, Category, Effort, Gateway, Scriptable, Target, lookup

STALENESS_DAYS = 90

#: SQL Server (and other integrated-CDC-capable sources) can be generated as a
#: single integrated CDC pipeline or as the older gateway-based pair. Integrated
#: is the default because it removes the continuously-billed gateway.
SQLSERVER_ARCH_INTEGRATED = "integrated"
SQLSERVER_ARCH_GATEWAY = "gateway"
SQLSERVER_ARCH_CHOICES = (SQLSERVER_ARCH_INTEGRATED, SQLSERVER_ARCH_GATEWAY)

# Fivetran attaches sync frequency to the connector. Databricks has no supported
# pipeline-level schedule, so each pipeline needs a companion job. Quartz format
# is: seconds minutes hours day-of-month month day-of-week.
CRON_BY_MINUTES: dict[int, str] = {
    15: "0 0/15 * * * ?",
    30: "0 0/30 * * * ?",
    60: "0 0 * * * ?",
    120: "0 0 0/2 * * ?",
    180: "0 0 0/3 * * ?",
    360: "0 0 0/6 * * ?",
    480: "0 0 0/8 * * ?",
    720: "0 0 0/12 * * ?",
    1440: "0 0 0 * * ?",
}

# Below an hour a periodic trigger cannot express the cadence at all, and at
# 1-5 minutes a cron job thrashes: every fire is a pipeline update with real
# startup cost. A continuous job is the honest equivalent.
CONTINUOUS_THRESHOLD_MINUTES = 5

DEFAULT_SYNC_MINUTES = 360

#: Table statuses the generated pipeline ingests. ``lakeflow_only`` is ingested
#: only when ``keep_lakeflow_only`` allows it; ``disabled_in_fivetran`` and
#: ``not_available`` are reported but never ingested.
PAIRED_STATUSES = frozenset({"same_name", "renamed"})

#: ``--include-lakeflow-only`` choices. ``auto`` ingests Lakeflow-only tables only
#: when the connector offers at most LAKEFLOW_ONLY_AUTO_LIMIT tables, so a short
#: list like PagerDuty's comes over whole while HubSpot's does not add dozens of
#: tables nobody asked for.
LAKEFLOW_ONLY_AUTO = "auto"
LAKEFLOW_ONLY_CHOICES = (LAKEFLOW_ONLY_AUTO, "all", "none")
LAKEFLOW_ONLY_AUTO_LIMIT = 10

# Long table lists are cut to this many names, then "and N more".
_LIST_LIMIT = 10

_INVALID_NAME = re.compile(r"[^a-z0-9_]+")


def to_identifier(*parts: str) -> str:
    """Build a Unity Catalog / pipeline-safe identifier from arbitrary text."""
    joined = "_".join(p for p in parts if p)
    cleaned = _INVALID_NAME.sub("_", joined.lower()).strip("_")
    return re.sub(r"_{2,}", "_", cleaned) or "unnamed"


def name_keys(table: str) -> frozenset[str]:
    """Normalise a table name so Fivetran's and Lakeflow Connect's spellings meet.

    Case and underscores are ignored and a trailing ``ies`` becomes ``y``. Two
    keys follow, and two names pair when any key is shared: one drops a trailing
    ``s``, the other first drops ``es`` after ``ss``, ``us``, ``x``, ``z``, ``ch``
    or ``sh``. ``-es`` is ambiguous, so neither key alone is enough: ``branches``
    needs the second to meet ``branch``, while ``tranches`` needs the first to
    meet ``tranche``. So ``incident`` meets ``incidents``, ``escalation_policy``
    meets ``escalation_policies``, ``custom_status`` meets ``custom_statuses``,
    ``cause`` meets ``causes``, and ``on_call`` meets ``oncalls``.
    """
    key = re.sub(r"ies$", "y", table.lower().replace("_", ""))
    return frozenset(
        {
            re.sub(r"s$", "", key),
            re.sub(r"s$", "", re.sub(r"(ss|us|x|z|ch|sh)es$", r"\1", key)),
        }
    )


def pair_tables(
    tables: list[dict[str, Any]], supported: frozenset[str] | None
) -> list[dict[str, Any]]:
    """Plan one connection's tables: one row per table, each with a ``status``.

    Without a supported-table list, every enabled Fivetran table is ``same_name``.
    With one, the plan starts from the Lakeflow Connect list, so a Fivetran name
    that differs can only add a table, never drop it. Each Lakeflow Connect table
    pairs with a Fivetran table sharing one of its ``name_keys``: ``same_name`` or
    ``renamed`` when that table is enabled, ``disabled_in_fivetran`` when the
    customer turned it off, ``lakeflow_only`` when nothing pairs. Enabled Fivetran
    tables left unpaired are ``not_available``.
    """
    if supported is None:
        return [
            {"lakeflow_table": t.get("source_table"), "fivetran": t, "status": "same_name"}
            for t in tables
            if t.get("enabled")
        ]

    by_key: dict[str, list[dict[str, Any]]] = {}
    for table in tables:
        for key in name_keys(table.get("source_table") or ""):
            by_key.setdefault(key, []).append(table)

    rows: list[dict[str, Any]] = []
    chosen: set[int] = set()
    for lakeflow_table in sorted(t.lower() for t in supported):
        candidates = {
            id(t): t for key in name_keys(lakeflow_table) for t in by_key.get(key, [])
        }
        match = _best_match(list(candidates.values()), lakeflow_table)
        if match is None:
            status = "lakeflow_only"
        else:
            chosen.add(id(match))
            if not match.get("enabled"):
                status = "disabled_in_fivetran"
            elif (match.get("source_table") or "").lower() == lakeflow_table:
                status = "same_name"
            else:
                status = "renamed"
        rows.append({"lakeflow_table": lakeflow_table, "fivetran": match, "status": status})

    rows.extend(
        {"lakeflow_table": None, "fivetran": t, "status": "not_available"}
        for t in tables
        if t.get("enabled") and id(t) not in chosen
    )
    return rows


def _best_match(candidates: list[dict[str, Any]], lakeflow_table: str) -> dict[str, Any] | None:
    """Enabled tables first, then an exact name match, then by name."""
    return min(
        candidates,
        key=lambda t: (
            not t.get("enabled"),
            (t.get("source_table") or "").lower() != lakeflow_table,
            t.get("source_table") or "",
        ),
        default=None,
    )


def keep_lakeflow_only(target: Target, mode: str = LAKEFLOW_ONLY_AUTO) -> bool:
    """Whether ``lakeflow_only`` tables of ``target`` are ingested under ``mode``."""
    if mode == "all":
        return True
    if mode == "none" or target.supported_tables is None:
        return False
    return len(target.supported_tables) <= LAKEFLOW_ONLY_AUTO_LIMIT


def _is_kept(row: dict[str, Any], keep_lakeflow: bool) -> bool:
    return row["status"] in PAIRED_STATUSES or (row["status"] == "lakeflow_only" and keep_lakeflow)


def _sample(names: list[str]) -> str:
    shown = ", ".join(names[:_LIST_LIMIT])
    more = len(names) - _LIST_LIMIT
    return f"{shown} and {more} more" if more > 0 else shown


#: Looks up a UC connection by name; returns its info, or None if it does not exist.
ConnectionLookup = Callable[[str], dict[str, Any] | None]


def build_plan(
    inventory: dict[str, Any],
    target_catalog: str,
    mar: dict[str, Any] | None = None,
    include_paused: bool = False,
    existing_connections: dict[str, str] | None = None,
    lookup_connection: ConnectionLookup | None = None,
    sqlserver_arch: str = SQLSERVER_ARCH_INTEGRATED,
    include_lakeflow_only: str = LAKEFLOW_ONLY_AUTO,
) -> dict[str, Any]:
    """Produce a migration plan from a discovery inventory.

    ``existing_connections`` maps a Fivetran connection id or service name to a
    UC connection that already exists and should be reused rather than created.
    ``lookup_connection``, when given, confirms each one exists with the right type.
    ``sqlserver_arch`` selects the SQL Server architecture: ``integrated`` (the
    default single-pipeline integrated CDC connector) or ``gateway`` (the older
    gateway-based pair). ``include_lakeflow_only`` decides whether Lakeflow Connect
    tables no Fivetran table pairs with are ingested: ``auto``, ``all``, or ``none``.
    """
    if include_lakeflow_only not in LAKEFLOW_ONLY_CHOICES:
        raise ValueError(
            f"include_lakeflow_only must be one of {', '.join(LAKEFLOW_ONLY_CHOICES)}, "
            f"got '{include_lakeflow_only}'"
        )
    if sqlserver_arch not in SQLSERVER_ARCH_CHOICES:
        raise ValueError(
            f"sqlserver_arch must be one of {', '.join(SQLSERVER_ARCH_CHOICES)}, "
            f"got '{sqlserver_arch}'"
        )
    connections = [
        c for c in inventory.get("connections", []) if include_paused or not c.get("paused")
    ]
    existing = existing_connections or {}
    unmatched = sorted(
        set(existing) - {c.get("id") for c in connections} - {c.get("service") for c in connections}
    )
    if unmatched:
        raise ValueError(
            f"--use-connection key(s) match no Fivetran connection id or service: "
            f"{', '.join(unmatched)}"
        )

    reuse = _ReuseContext(existing, lookup_connection)
    items = [
        _plan_connection(c, target_catalog, mar, reuse, sqlserver_arch, include_lakeflow_only)
        for c in connections
    ]

    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "target_catalog": target_catalog,
        "include_lakeflow_only": include_lakeflow_only,
        "source_account": inventory.get("account", {}),
        "items": items,
        "summary": summarise_plan(items),
        "blockers": _collect(items, "blockers"),
        "warnings": _collect(items, "warnings"),
    }


class _ReuseContext:
    """Which existing UC connections to reuse, and what the workspace says about them."""

    def __init__(self, existing: dict[str, str], lookup_connection: ConnectionLookup | None):
        self._existing = existing
        self._lookup = lookup_connection
        self._cache: dict[str, dict[str, Any] | None] = {}

    def name_for(self, connection: dict[str, Any]) -> str | None:
        """A connection id takes precedence over a service-wide mapping."""
        return self._existing.get(connection.get("id") or "") or self._existing.get(
            connection.get("service") or ""
        )

    @property
    def can_check(self) -> bool:
        return self._lookup is not None

    def check(self, name: str, target: Target) -> tuple[bool, str | None]:
        """Return (verified, blocker) for reusing ``name`` as ``target``'s connection."""
        if self._lookup is None:
            return False, None
        if name not in self._cache:
            self._cache[name] = self._lookup(name)
        info = self._cache[name]
        if info is None:
            return False, (
                f"UC connection '{name}' was given to reuse but does not exist in the "
                "workspace. Fix the name or drop --use-connection to generate a new one."
            )
        actual = _effective_connection_type(info)
        if actual != _normalise_type(target.connection_type):
            return False, (
                f"UC connection '{name}' is type {_describe_type(info)}, but this Fivetran "
                f"connection needs a {target.connection_type} connection."
            )
        return True, None


def _normalise_type(value: str | None) -> str:
    return (value or "").replace("_", "").upper()


def _effective_connection_type(info: dict[str, Any]) -> str:
    # Newer managed connectors (PagerDuty, for one) are stored as API_SOURCE and name the
    # connector in options.source_name rather than in connection_type.
    if info.get("connection_type") == "API_SOURCE":
        return _normalise_type((info.get("options") or {}).get("source_name"))
    return _normalise_type(info.get("connection_type"))


def _describe_type(info: dict[str, Any]) -> str:
    source_name = (info.get("options") or {}).get("source_name")
    if info.get("connection_type") == "API_SOURCE" and source_name:
        return f"API_SOURCE (source_name={source_name})"
    return str(info.get("connection_type"))


def _plan_connection(
    connection: dict[str, Any],
    target_catalog: str,
    mar: dict[str, Any] | None,
    reuse: _ReuseContext,
    sqlserver_arch: str = SQLSERVER_ARCH_INTEGRATED,
    include_lakeflow_only: str = LAKEFLOW_ONLY_AUTO,
) -> dict[str, Any]:
    service = connection.get("service") or ""
    target = lookup(service)
    name = to_identifier(connection.get("destination_schema") or service, connection.get("id"))

    use_integrated = (
        target.supports_integrated_cdc and sqlserver_arch == SQLSERVER_ARCH_INTEGRATED
    )
    # The effective gateway requirement, once the architecture choice is applied.
    effective_gateway = Gateway.NOT_REQUIRED if use_integrated else target.gateway

    reused_name = reuse.name_for(connection) if target.has_managed_connector else None
    verified, reuse_blocker = reuse.check(reused_name, target) if reused_name else (False, None)

    schedule = _plan_schedule(connection)
    blockers, warnings = _assess(
        connection,
        target,
        schedule,
        reused_name=reused_name,
        verified=verified,
        checked=reuse.can_check,
        use_integrated=use_integrated,
    )
    tables = _plan_objects(
        connection,
        target,
        target_catalog,
        keep_lakeflow_only(target, include_lakeflow_only),
        blocked=bool(blockers or reuse_blocker),
    )
    if tables.blocker:
        blockers.append(tables.blocker)
    if reuse_blocker:
        blockers.append(reuse_blocker)
    if blockers:
        tables.withdraw()

    return {
        "fivetran": {
            "connection_id": connection.get("id"),
            "group_id": connection.get("group_id"),
            "service": service,
            "destination_schema": connection.get("destination_schema"),
            "paused": connection.get("paused"),
            "setup_state": connection.get("status", {}).get("setup_state"),
            "sync_frequency_minutes": connection.get("sync_frequency_minutes"),
            "networking_method": connection.get("networking_method"),
            "monthly_mar": _mar_for(connection, mar),
        },
        "target": {
            "connection_name": reused_name or to_identifier(service, "conn", connection.get("id")),
            "connection_source": "existing" if reused_name else "new",
            "connection_verified": verified,
            "connection_type": target.connection_type,
            "category": target.category.value,
            "availability": target.availability.value,
            "gateway": effective_gateway.value,
            "architecture": _architecture(target, effective_gateway, use_integrated),
            "connector_type": "CDC" if use_integrated else None,
            "scriptable": target.scriptable.value,
            "preferred_auth": target.preferred_auth,
            "effort": _effort(target, reused=bool(reused_name)).value,
            "pipeline_name": name,
            "destination_catalog": target_catalog,
            "destination_schema": to_identifier(connection.get("destination_schema") or service),
            "alternative": target.alternative,
            "has_supported_tables": target.supported_tables is not None,
        },
        "objects": tables.objects,
        "table_counts": tables.counts,
        "table_plan": tables.plan,
        "schedule": schedule,
        "prerequisites": _prerequisites(target, effective_gateway, use_integrated),
        "blockers": blockers,
        "warnings": warnings + tables.warnings,
    }


def _architecture(target: Target, effective_gateway: Gateway, use_integrated: bool) -> str | None:
    """Name the ingestion architecture for a CDC database source.

    ``None`` for non-database sources, where the concept does not apply.
    """
    if target.category is not Category.DATABASE_CDC:
        return None
    if use_integrated:
        return "integrated_cdc"
    return "gateway" if effective_gateway is Gateway.REQUIRED else "managed_cdc"


def _effort(target: Target, reused: bool) -> Effort:
    """Discount the connection work a reused connection has already absorbed.

    For SaaS that is all of it: the sign-in and the credential setup behind it.
    A database connection does not imply CDC is enabled at the source, so only
    a browser sign-in (never the case for databases today) is discounted.
    """
    effort = target.effort
    if not reused or effort is Effort.NO_MANAGED_CONNECTOR:
        return effort
    if target.category is Category.SAAS:
        return Effort.LOW
    if effort is Effort.HIGH:
        return Effort.LOW if target.prerequisites_automatable else Effort.MEDIUM
    return effort


@dataclass
class _TablePlan:
    objects: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    blocker: str | None = None
    counts: dict[str, int] = field(default_factory=dict)
    #: Every row with its status, for connectors with a supported-table list.
    plan: list[dict[str, Any]] | None = None

    def withdraw(self) -> None:
        """A blocked item ingests nothing. Statuses stay, since they explain the blocker."""
        self.objects = []
        for key in ("tables", "lakeflow_only", "lakeflow_skipped"):
            if key in self.counts:
                self.counts[key] = 0
        for row in self.plan or []:
            row["kept"] = False


def _plan_objects(
    connection: dict[str, Any],
    target: Target,
    target_catalog: str,
    keep_lakeflow: bool,
    blocked: bool = False,
) -> _TablePlan:
    """Translate the connection's tables into Lakeflow object specs.

    Tables are paired with the connector's supported-table list first (see
    ``pair_tables``), so ``source_table`` is always the Lakeflow Connect name.
    A connection where no table pairs is blocked: there is nothing to migrate.
    ``blocked`` means another blocker will withdraw every table, so no warning
    says Lakeflow-only tables are ingested.
    """
    result = _TablePlan()
    if not target.has_managed_connector:
        return result

    destination_schema = to_identifier(
        connection.get("destination_schema") or connection.get("service")
    )
    # The source database is required as source_catalog for integrated CDC, and
    # is a valid (optional) field for the gateway-based ingestion pipeline too.
    source_catalog = (connection.get("config") or {}).get("database") or None
    tables = connection.get("objects", [])
    rows = pair_tables(tables, target.supported_tables)
    # A Lakeflow-only table has no Fivetran schema of its own.
    default_schema = min(
        (t["source_schema"] for t in tables if t.get("source_schema")), default=None
    )
    warnings = result.warnings
    specs = result.objects
    keyless: list[str] = []

    for row in rows:
        if not _is_kept(row, keep_lakeflow):
            continue
        table = row["fivetran"] or {}
        source_table = row["lakeflow_table"]
        fivetran_schema = table.get("source_schema") or default_schema

        config: dict[str, Any] = {}

        # Fivetran HISTORY mode is SCD type 2. Anything else merges in place.
        scd_type = "SCD_TYPE_2" if table.get("retains_history") else "SCD_TYPE_1"
        config["scd_type"] = scd_type

        # Managed connectors read keys from the source themselves, so a key
        # Fivetran did not report is not a gap: only pass keys Fivetran knows.
        if table.get("primary_keys"):
            config["primary_keys"] = table["primary_keys"]
        elif table.get("primary_keys_known"):
            keyless.append(f"{table.get('source_schema', '?')}.{source_table}")

        if table.get("excluded_columns"):
            config["exclude_columns"] = table["excluded_columns"]

        if table.get("hashed_columns"):
            warnings.append(
                f"{table['source_schema']}.{table['source_table']} hashes "
                f"{len(table['hashed_columns'])} column(s) in Fivetran "
                f"({', '.join(table['hashed_columns'])}). Lakeflow Connect has no equivalent "
                "at ingest; reproduce with a downstream masking policy or transformation."
            )

        # Fivetran's destination name only carries over when the source names
        # match; a renamed table lands under its Lakeflow Connect name.
        destination_table = table.get("destination_table") if row["status"] == "same_name" else None
        is_database = target.category is Category.DATABASE_CDC
        specs.append(
            {
                "type": "report" if target.connection_type == "WORKDAY_RAAS" else "table",
                "source_catalog": source_catalog if is_database else None,
                "source_schema": target.fixed_source_schema or fivetran_schema,
                "fivetran_source_schema": fivetran_schema,
                "source_table": source_table,
                "fivetran_table": table.get("source_table"),
                "status": row["status"],
                "destination_catalog": target_catalog,
                "destination_schema": destination_schema,
                "destination_table": to_identifier(destination_table or source_table),
                "table_configuration": config,
                "primary_keys_known": table.get("primary_keys_known", False),
            }
        )

    # SaaS connectors define their own keys, so Fivetran's view is irrelevant.
    # A database table genuinely without one changes source setup, e.g. SQL
    # Server needs CDC rather than change tracking for it.
    if keyless and target.category is Category.DATABASE_CDC:
        warnings.append(
            f"Fivetran reports {len(keyless)} table(s) with no primary key at the source "
            f"({_sample(sorted(keyless))}). Keyless tables need the connector's keyless-table "
            "source setup (for SQL Server, CDC instead of change tracking)."
        )

    warnings.extend(_table_warnings(rows, tables, target, keep_lakeflow, blocked))
    result.blocker = _table_blocker(rows, target)
    result.counts = _table_counts(rows, keep_lakeflow)
    if target.supported_tables is not None:
        result.plan = [
            {
                "fivetran_table": (r["fivetran"] or {}).get("source_table"),
                "lakeflow_table": r["lakeflow_table"],
                "status": r["status"],
                "kept": _is_kept(r, keep_lakeflow),
            }
            for r in rows
        ]

    if target.supported_tables is not None and target.last_verified:
        try:
            verified = dt.date.fromisoformat(target.last_verified)
            age = (dt.date.today() - verified).days
            if age > STALENESS_DAYS:
                warnings.append(
                    f"The {target.connection_type} supported-table list was last verified "
                    f"on {target.last_verified} ({age} days ago). Check "
                    f"{target.docs_url or 'the connector reference'} for newly added tables."
                )
        except ValueError:
            pass

    rewritten = {
        s["fivetran_source_schema"]
        for s in specs
        if s["fivetran_source_schema"] and s["fivetran_source_schema"] != s["source_schema"]
    }
    if rewritten:
        warnings.append(
            f"Source schema rewritten from {', '.join(sorted(rewritten))} to "
            f"'{target.fixed_source_schema}': the {target.connection_type} connector "
            "addresses objects under a schema it defines, not the one Fivetran used."
        )

    return result


def _table_counts(rows: list[dict[str, Any]], keep_lakeflow: bool) -> dict[str, int]:
    def count(*statuses: str) -> int:
        return sum(1 for r in rows if r["status"] in statuses)

    lakeflow_only = count("lakeflow_only")
    return {
        "tables": sum(1 for r in rows if _is_kept(r, keep_lakeflow)),
        "paired": count(*PAIRED_STATUSES),
        "renamed": count("renamed"),
        "lakeflow_only": lakeflow_only if keep_lakeflow else 0,
        "lakeflow_skipped": 0 if keep_lakeflow else lakeflow_only,
        "not_available": count("not_available"),
        "disabled_in_fivetran": count("disabled_in_fivetran"),
    }


def _table_blocker(rows: list[dict[str, Any]], target: Target) -> str | None:
    if any(r["status"] in PAIRED_STATUSES for r in rows):
        return None
    if target.supported_tables is None:
        return (
            "No enabled tables to ingest. Enable tables in Fivetran and re-run discovery "
            "(without --no-schemas)."
        )
    return (
        f"None of the tables Fivetran syncs has an equivalent in {target.connection_type}, "
        f"which offers {_sample(sorted(target.supported_tables))}. Enable one in Fivetran "
        "and re-run discovery, check Lakeflow Community Connectors for one that covers "
        "these tables, or keep this connection on Fivetran."
        f"{_see(target, 'Supported tables:')}"
    )


def _table_warnings(
    rows: list[dict[str, Any]],
    tables: list[dict[str, Any]],
    target: Target,
    keep_lakeflow: bool,
    blocked: bool = False,
) -> list[str]:
    connection_type = target.connection_type
    warnings: list[str] = []

    renamed = sorted(
        f"{r['fivetran']['source_table']} as {r['lakeflow_table']}"
        for r in rows
        if r["status"] == "renamed"
    )
    if renamed:
        warnings.append(
            f"{len(renamed)} table(s) have a different name in {connection_type}, so "
            "downstream references change (Fivetran as Lakeflow Connect): "
            f"{_sample(renamed)}."
        )

    paired = any(r["status"] in PAIRED_STATUSES for r in rows)
    lakeflow_only = sorted(r["lakeflow_table"] for r in rows if r["status"] == "lakeflow_only")
    if paired and lakeflow_only and keep_lakeflow:
        if not blocked:
            warnings.append(
                f"{len(lakeflow_only)} {connection_type} table(s) match no table Fivetran "
                f"lists and are ingested too: {_sample(lakeflow_only)}. Remove any you do "
                "not need from the bundle."
            )
    elif paired and lakeflow_only and not blocked:
        warnings.append(
            f"{len(lakeflow_only)} {connection_type} table(s) match no table Fivetran lists "
            f"and are not included: {_sample(lakeflow_only)}. To ingest them, add them to "
            "this pipeline in the bundle, or re-run plan_migration.py with "
            f"--include-lakeflow-only all, which applies to every connection.{_see(target)}"
        )

    not_available = sorted(
        f"{r['fivetran'].get('source_schema', '?')}.{r['fivetran'].get('source_table')}"
        for r in rows
        if r["status"] == "not_available"
    )
    if not_available:
        warnings.append(
            f"{len(not_available)} Fivetran table(s) have no {connection_type} equivalent "
            f"and are not migrated: {_sample(not_available)}.{_see(target)}"
        )

    disabled = sorted(
        f"{t.get('source_schema', '?')}.{t.get('source_table')}"
        for t in tables
        if not t.get("enabled")
    )
    if disabled:
        warnings.append(
            f"{len(disabled)} table(s) disabled in Fivetran are not migrated "
            f"({_sample(disabled)}); enable them in Fivetran and re-run discovery to "
            "include them."
        )
    return warnings


def _plan_schedule(connection: dict[str, Any]) -> dict[str, Any]:
    """Map a Fivetran sync frequency onto a Lakeflow Job trigger.

    There is no supported pipeline-level schedule, so every ingestion pipeline
    needs a companion job. max_concurrent_runs is pinned to 1 because Fivetran
    serialises syncs and an unbounded cron job would not.
    """
    minutes = connection.get("sync_frequency_minutes") or DEFAULT_SYNC_MINUTES

    if minutes <= CONTINUOUS_THRESHOLD_MINUTES:
        return {
            "mode": "continuous",
            "fivetran_minutes": minutes,
            "max_concurrent_runs": 1,
            "note": (
                f"Fivetran syncs every {minutes} min. A cron job at that cadence would "
                "thrash, since each fire is a full pipeline update. A continuous job is "
                "the closer equivalent, but there is no documented minimum interval for "
                "managed ingestion and source API limits may bind. Confirm before cutover."
            ),
        }

    cron = CRON_BY_MINUTES.get(minutes)
    if not cron:
        nearest = min(CRON_BY_MINUTES, key=lambda m: abs(m - minutes))
        return {
            "mode": "cron",
            "fivetran_minutes": minutes,
            "quartz_cron_expression": CRON_BY_MINUTES[nearest],
            "timezone_id": "UTC",
            "max_concurrent_runs": 1,
            "note": (
                f"No exact cron for {minutes} min; using the nearest supported "
                f"cadence ({nearest} min)."
            ),
        }

    return {
        "mode": "cron",
        "fivetran_minutes": minutes,
        "quartz_cron_expression": cron,
        "timezone_id": "UTC",
        "max_concurrent_runs": 1,
        "note": "",
    }


def _prerequisites(
    target: Target,
    effective_gateway: Gateway = Gateway.NOT_REQUIRED,
    use_integrated: bool = False,
) -> dict[str, Any]:
    steps: list[str] = []
    if target.source_prerequisites:
        steps.append(target.source_prerequisites)
    if use_integrated:
        steps.append(
            "Enable the integrated CDC connector on the workspace (through the Databricks "
            "account team) before deploying. The single pipeline stages through a Unity "
            "Catalog volume it creates in the destination schema unless data_staging_options "
            "names another, so the ingestion identity needs CREATE VOLUME on that schema."
        )
    elif effective_gateway is Gateway.REQUIRED:
        steps.append(
            "Create an ingestion gateway pipeline first. It runs continuously on classic "
            "compute and is billed even while the ingestion pipeline is idle. The ingestion "
            "pipeline references it by the gateway's pipeline_id."
        )
    return {
        "steps": steps,
        "automatable": target.prerequisites_automatable,
        "auth_note": target.auth_note,
    }


def _assess(
    connection: dict[str, Any],
    target: Target,
    schedule: dict[str, Any],
    reused_name: str | None = None,
    verified: bool = False,
    checked: bool = False,
    use_integrated: bool = False,
) -> tuple[list[str], list[str]]:
    """Separate what stops the migration from what merely needs attention."""
    blockers: list[str] = []
    warnings: list[str] = []
    service = connection.get("service")

    if not target.has_managed_connector:
        blockers.append(
            f"No managed Lakeflow Connect connector for '{service}'. {target.alternative}"
            f"{_see(target)}"
        )
        return blockers, warnings

    if use_integrated:
        warnings.append(
            "Generated as an integrated CDC pipeline (connector_type: CDC, no gateway). "
            "The integrated CDC connector must be enabled on the target workspace by the "
            "Databricks account team; confirm before deploying. The pipeline runs in "
            "triggered mode on its companion job (continuous mode is Beta), and does not "
            "support read replicas or standby instances. To fall back to the gateway-based "
            "architecture, re-run with --sqlserver-arch gateway."
        )

    # An existing connection already carries its auth, so neither the sign-in
    # step nor the source-side setup behind a SaaS credential applies.
    if not reused_name:
        warnings.extend(_connection_auth_warnings(target, service))

    if not target.prerequisites_automatable and not (
        reused_name and target.category is Category.SAAS
    ):
        warnings.append(
            f"Source-side setup for '{service}' cannot be fully scripted; generate a "
            "runbook for the source administrator."
        )

    availability = _availability_warning(target, reused_name if not checked else None, verified)
    if availability:
        warnings.append(availability)
    elif target.availability is Availability.UNVERIFIED:
        warnings.append(
            f"Release state for {target.connection_type} was not verified. Confirm against "
            "the connector docs before committing to a date."
            f"{_see(target)}"
        )

    if connection.get("networking_method") not in (None, "Directly"):
        warnings.append(
            f"Source reachable over {connection['networking_method']}. Private networking "
            "must be designed on the Databricks side before ingestion will connect."
        )

    if connection.get("status", {}).get("setup_state") == "broken":
        warnings.append("Connection is broken in Fivetran; its configuration may be stale.")

    if schedule.get("note"):
        warnings.append(schedule["note"])

    return blockers, warnings


def _connection_auth_warnings(target: Target, service: str | None) -> list[str]:
    if target.scriptable is Scriptable.NO:
        # A browser sign-in costs one manual step for the connection only; the
        # pipeline and job are still generated and reference it by name.
        return [
            (
                f"'{service}' uses browser-based OAuth only, so its Unity Catalog connection "
                "cannot be created programmatically. A human must create it once in Catalog "
                "Explorer, using the connection name in the bundle, before deploying; the "
                "pipeline and job are generated and deploy normally afterwards."
            )
        ]
    if target.scriptable is Scriptable.CONDITIONAL:
        auth = f" (generated as {target.preferred_auth})" if target.preferred_auth else ""
        return [f"Conditionally scriptable{auth}: {target.auth_note}"]
    return []


def _availability_warning(
    target: Target, unchecked_reuse: str | None, verified: bool
) -> str | None:
    """Caveat for a connector not yet GA. The catalog cannot see any workspace, so
    this is a prompt to confirm, never a claim about the customer's workspace.

    ``unchecked_reuse`` names a reused connection that nobody has looked up yet.
    """
    if target.availability is Availability.BETA:
        # A connection of this type visible in the target workspace means the
        # connector is already usable there.
        if verified:
            return None
        message = (
            f"The {target.connection_type} connector is in Beta; confirm it is enabled in "
            "the workspace you deploy to."
        )
        if unchecked_reuse:
            message += (
                f" Reusing connection '{unchecked_reuse}' suggests it already is; pass "
                "--databricks-profile to confirm and drop this note."
            )
        return message + _see(target)
    if target.availability is Availability.PUBLIC_PREVIEW:
        if verified:
            return None
        return (
            f"The {target.connection_type} connector is in Public Preview. Some Public Preview "
            "connectors need enrollment through the Databricks account team; confirm access "
            "in the workspace you deploy to."
            f"{_see(target)}"
        )
    return None


def _see(target: Target, label: str = "See") -> str:
    return f" {label} {target.docs_url}" if target.docs_url else ""


def _mar_for(connection: dict[str, Any], mar: dict[str, Any] | None) -> int | None:
    """Attach MAR to a connection.

    Fivetran's Platform Connector keys MAR by connection *name*, which is the
    destination schema, not the connection id the REST API returns.
    """
    if not mar:
        return None
    by_connection = mar.get("by_connection") or {}
    return by_connection.get(connection.get("destination_schema"))


def _collect(items: list[dict[str, Any]], key: str) -> list[dict[str, str]]:
    out = []
    for item in items:
        for message in item.get(key, []):
            out.append({"connection": item["fivetran"]["connection_id"], "message": message})
    return out


def summarise_plan(items: list[dict[str, Any]]) -> dict[str, Any]:
    by_effort: dict[str, int] = {}
    by_category: dict[str, int] = {}
    for item in items:
        effort = item["target"]["effort"]
        category = item["target"]["category"]
        by_effort[effort] = by_effort.get(effort, 0) + 1
        by_category[category] = by_category.get(category, 0) + 1

    migratable = [i for i in items if not i["blockers"]]
    needs_gateway = [i for i in items if i["target"]["gateway"] == Gateway.REQUIRED.value]
    integrated_cdc = [
        i for i in migratable if i["target"].get("architecture") == "integrated_cdc"
    ]
    reused = [i for i in migratable if i["target"].get("connection_source") == "existing"]
    manual_connections = [
        i
        for i in migratable
        if i["target"]["scriptable"] == Scriptable.NO.value and i not in reused
    ]

    def total(key: str, among: list[dict[str, Any]]) -> int:
        return sum(i.get("table_counts", {}).get(key, 0) for i in among)

    return {
        "connections_total": len(items),
        "connections_migratable": len(migratable),
        "connections_not_migratable": len(items) - len(migratable),
        "connections_manual_sign_in": len(manual_connections),
        "connections_reused": len(reused),
        # Table counts cover what the bundle ingests, so only migratable connections,
        # except tables_not_available, which reports every gap.
        "tables_total": sum(len(i["objects"]) for i in migratable),
        "tables_paired": total("paired", migratable),
        "tables_renamed": total("renamed", migratable),
        "tables_lakeflow_only": total("lakeflow_only", migratable),
        "tables_lakeflow_skipped": total("lakeflow_skipped", migratable),
        "tables_not_available": total("not_available", items),
        "tables_scd2": sum(
            1
            for i in migratable
            for o in i["objects"]
            if o["table_configuration"].get("scd_type") == "SCD_TYPE_2"
        ),
        "gateways_required": len(needs_gateway),
        "integrated_cdc_pipelines": len(integrated_cdc),
        "jobs_required": len(migratable),
        "by_effort": by_effort,
        "by_category": by_category,
    }
