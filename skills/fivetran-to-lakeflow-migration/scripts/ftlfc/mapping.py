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
from typing import Any

from . import PLAN_SCHEMA_VERSION
from .catalog import Availability, Category, Effort, Gateway, Scriptable, Target, lookup

STALENESS_DAYS = 90

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

_INVALID_NAME = re.compile(r"[^a-z0-9_]+")


def to_identifier(*parts: str) -> str:
    """Build a Unity Catalog / pipeline-safe identifier from arbitrary text."""
    joined = "_".join(p for p in parts if p)
    cleaned = _INVALID_NAME.sub("_", joined.lower()).strip("_")
    return re.sub(r"_{2,}", "_", cleaned) or "unnamed"


#: Looks up a UC connection by name; returns its info, or None if it does not exist.
ConnectionLookup = Callable[[str], dict[str, Any] | None]


def build_plan(
    inventory: dict[str, Any],
    target_catalog: str,
    mar: dict[str, Any] | None = None,
    include_paused: bool = False,
    existing_connections: dict[str, str] | None = None,
    lookup_connection: ConnectionLookup | None = None,
) -> dict[str, Any]:
    """Produce a migration plan from a discovery inventory.

    ``existing_connections`` maps a Fivetran connection id or service name to a
    UC connection that already exists and should be reused rather than created.
    ``lookup_connection``, when given, confirms each one exists with the right type.
    """
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
    items = [_plan_connection(c, target_catalog, mar, reuse) for c in connections]

    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "target_catalog": target_catalog,
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
) -> dict[str, Any]:
    service = connection.get("service") or ""
    target = lookup(service)
    name = to_identifier(connection.get("destination_schema") or service, connection.get("id"))

    reused_name = reuse.name_for(connection) if target.has_managed_connector else None
    verified, reuse_blocker = reuse.check(reused_name, target) if reused_name else (False, None)

    objects, obj_warnings = _plan_objects(connection, target, target_catalog)
    schedule = _plan_schedule(connection)
    blockers, warnings = _assess(
        connection,
        target,
        objects,
        schedule,
        reused_name=reused_name,
        verified=verified,
        checked=reuse.can_check,
    )
    if reuse_blocker:
        blockers.append(reuse_blocker)

    return {
        "fivetran": {
            "connection_id": connection.get("id"),
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
            "gateway": target.gateway.value,
            "scriptable": target.scriptable.value,
            "preferred_auth": target.preferred_auth,
            "effort": _effort(target, reused=bool(reused_name)).value,
            "pipeline_name": name,
            "destination_catalog": target_catalog,
            "destination_schema": to_identifier(connection.get("destination_schema") or service),
            "alternative": target.alternative,
            "has_supported_tables": target.supported_tables is not None,
        },
        "objects": objects,
        "schedule": schedule,
        "prerequisites": _prerequisites(target),
        "blockers": blockers,
        "warnings": warnings + obj_warnings,
    }


def _effort(target: Target, reused: bool) -> Effort:
    """Discount the connection work a reused connection has already absorbed.

    For SaaS that is all of it: the sign-in and the credential setup behind it.
    A database connection does not imply CDC is enabled at the source, so only
    a browser sign-in (never the case for databases today) is discounted.
    """
    effort = target.effort
    if not reused or effort is Effort.BLOCKED:
        return effort
    if target.category is Category.SAAS:
        return Effort.LOW
    if effort is Effort.HIGH:
        return Effort.LOW if target.prerequisites_automatable else Effort.MEDIUM
    return effort


def _plan_objects(
    connection: dict[str, Any], target: Target, target_catalog: str
) -> tuple[list[dict[str, Any]], list[str]]:
    """Translate enabled Fivetran tables into Lakeflow object specs.

    When the target has a ``supported_tables`` set, only tables in that set are
    emitted.  Fivetran tables absent from the set are excluded and a warning is
    generated so the plan clearly reports the coverage gap.
    """
    warnings: list[str] = []
    if not target.has_managed_connector:
        return [], warnings

    destination_schema = to_identifier(
        connection.get("destination_schema") or connection.get("service")
    )
    specs: list[dict[str, Any]] = []
    dropped: list[str] = []
    keyless: list[str] = []

    for table in connection.get("objects", []):
        if not table.get("enabled"):
            continue

        source_table = table.get("source_table") or ""

        if (
            target.supported_tables is not None
            and source_table.lower() not in {t.lower() for t in target.supported_tables}
        ):
            dropped.append(
                f"{table.get('source_schema', '?')}.{source_table}"
            )
            continue

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

        specs.append(
            {
                "type": "report" if target.connection_type == "WORKDAY_RAAS" else "table",
                "source_schema": target.fixed_source_schema or table.get("source_schema"),
                "fivetran_source_schema": table.get("source_schema"),
                "source_table": table.get("source_table"),
                "destination_catalog": target_catalog,
                "destination_schema": destination_schema,
                "destination_table": to_identifier(
                    table.get("destination_table") or table.get("source_table")
                ),
                "table_configuration": config,
                "primary_keys_known": table.get("primary_keys_known", False),
            }
        )

    # SaaS connectors define their own keys, so Fivetran's view is irrelevant.
    # A database table genuinely without one changes source setup, e.g. SQL
    # Server needs CDC rather than change tracking for it.
    if keyless and target.category is Category.DATABASE_CDC:
        shown = ", ".join(sorted(keyless)[:10])
        more = f" and {len(keyless) - 10} more" if len(keyless) > 10 else ""
        warnings.append(
            f"Fivetran reports {len(keyless)} table(s) with no primary key at the source "
            f"({shown}{more}). Keyless tables need the connector's keyless-table source "
            "setup (for SQL Server, CDC instead of change tracking)."
        )

    if dropped:
        docs_hint = f" See {target.docs_url}" if target.docs_url else ""
        warnings.append(
            f"{len(dropped)} Fivetran table(s) have no Lakeflow Connect equivalent "
            f"in the {target.connection_type} connector and were excluded: "
            f"{', '.join(sorted(dropped))}.{docs_hint}"
        )

    if target.supported_tables is not None and target.last_verified:
        try:
            verified = dt.date.fromisoformat(target.last_verified)
            age = (dt.date.today() - verified).days
            if age > STALENESS_DAYS:
                docs_suffix = f" {target.docs_url}" if target.docs_url else ""
                warnings.append(
                    f"The {target.connection_type} supported-table list was last verified "
                    f"on {target.last_verified} ({age} days ago). Check the connector "
                    f"reference for newly added tables.{docs_suffix}"
                )
        except ValueError:
            pass

    rewritten = {
        s["fivetran_source_schema"]
        for s in specs
        if s["fivetran_source_schema"] != s["source_schema"]
    }
    if rewritten:
        warnings.append(
            f"Source schema rewritten from {', '.join(sorted(rewritten))} to "
            f"'{target.fixed_source_schema}': the {target.connection_type} connector "
            "addresses objects under a schema it defines, not the one Fivetran used."
        )

    return specs, warnings


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


def _prerequisites(target: Target) -> dict[str, Any]:
    steps: list[str] = []
    if target.source_prerequisites:
        steps.append(target.source_prerequisites)
    if target.gateway is Gateway.REQUIRED:
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
    objects: list[dict[str, Any]],
    schedule: dict[str, Any],
    reused_name: str | None = None,
    verified: bool = False,
    checked: bool = False,
) -> tuple[list[str], list[str]]:
    """Separate what stops the migration from what merely needs attention."""
    blockers: list[str] = []
    warnings: list[str] = []
    service = connection.get("service")

    if not target.has_managed_connector:
        blockers.append(
            f"No managed Lakeflow Connect connector for '{service}'. {target.alternative}"
        )
        return blockers, warnings

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
            "the current Lakeflow Connect connector list before committing to a date."
        )

    if not objects:
        warnings.append(
            "No enabled tables found. Either nothing is selected in Fivetran, or discovery "
            "ran with --no-schemas."
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
            f"The {target.connection_type} connector is in Beta. A workspace admin turns Beta "
            "connectors on from the workspace Previews page; confirm it is on in the "
            "workspace you deploy to."
        )
        if unchecked_reuse:
            message += (
                f" Reusing connection '{unchecked_reuse}' suggests it already is; pass "
                "--databricks-profile to confirm and drop this note."
            )
        return message
    if target.availability is Availability.PUBLIC_PREVIEW:
        if verified:
            return None
        return (
            f"The {target.connection_type} connector is in Public Preview. Some Public Preview "
            "connectors need enrollment through the Databricks account team; confirm access "
            "in the workspace you deploy to."
        )
    return None


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
    reused = [i for i in migratable if i["target"].get("connection_source") == "existing"]
    manual_connections = [
        i
        for i in migratable
        if i["target"]["scriptable"] == Scriptable.NO.value and i not in reused
    ]

    tables_dropped = sum(
        1
        for i in items
        for w in i.get("warnings", [])
        if "have no Lakeflow Connect equivalent" in w
    )

    return {
        "connections_total": len(items),
        "connections_migratable": len(migratable),
        "connections_blocked": len(items) - len(migratable),
        "connections_manual_sign_in": len(manual_connections),
        "connections_reused": len(reused),
        "tables_total": sum(len(i["objects"]) for i in items),
        "tables_dropped": tables_dropped,
        "tables_scd2": sum(
            1
            for i in items
            for o in i["objects"]
            if o["table_configuration"].get("scd_type") == "SCD_TYPE_2"
        ),
        "gateways_required": len(needs_gateway),
        "jobs_required": len(migratable),
        "by_effort": by_effort,
        "by_category": by_category,
    }
