"""Fivetran consumption and spend, recorded for the Databricks account team.

The skill records what the customer consumes in Fivetran so their account team
can model cost with their own tools. It never prices Lakeflow Connect, compares
costs, or estimates a saving.

Three inputs, all optional except the plan:

- ``plan.json`` (stage 3): one row per Fivetran connection.
- ``mar.json`` (stage 2): Platform Connector MAR per table and month, plus the
  ``usage_cost`` / ``credits_used`` rows Fivetran billed.
- ``fivetran_spend.json``: the spend the customer shared, recorded as given.

``consumption_rows`` has one row per connection in the input shape of the
Lakeflow Connect cost estimator: ``source_type``, ``num_rows`` per run,
``num_flows`` (tables), ``cloud``, ``region``.
"""

from __future__ import annotations

import csv
import datetime as dt
import json
import re
from pathlib import Path
from typing import Any

from .mar import PAID, latest_full_month

SPEND_BASES = ("invoice", "contract", "estimate")

# Runs per month assume a 30-day month: 30 * 24 * 60 minutes.
MINUTES_PER_MONTH = 43200

CONSUMPTION_COLUMNS = (
    "connection_id",
    "group_id",
    "fivetran_service",
    "fivetran_connection_name",
    "source_type",
    "category",
    "migratable",
    "effort",
    "num_flows",
    "sync_minutes",
    "schedule_mode",
    "runs_per_month",
    "measured_month",
    "paid_mar",
    "total_mar",
    "num_rows",
    "cloud",
    "region",
)
BILLING_COLUMNS = ("source", "group_id", "measured_month", "metric", "value", "note")
MAR_BY_TABLE_COLUMNS = (
    "connection_id",
    "destination_id",
    "connection_name",
    "schema_name",
    "table_name",
    "measured_month",
    "free_type",
    "mar",
)

_MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
_VOLUME = re.compile(r"^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$")

README_TEXT = """\
Fivetran consumption export from the fivetran-to-lakeflow-migration skill, for the Databricks
account team.

connections.csv: one row per Fivetran connection in the plan. source_type, num_rows, num_flows,
  cloud, and region match the Lakeflow Connect cost estimator inputs. num_rows = latest full
  month's paid MAR / runs_per_month, a floor (MAR counts each key once a month); empty for
  continuous schedules or without MAR. runs_per_month assumes a 30-day month.
fivetran_billing.csv: Fivetran usage_cost / credits_used per destination and month from the
  Platform Connector, plus any spend the customer reported (source = customer).
mar_by_table.csv: monthly MAR per table and free_type from the Platform Connector, for the
  destinations in the plan.

These are recorded figures, not estimates. No credentials or row data are included.
"""


class ConsumptionError(ValueError):
    """Consumption inputs were invalid."""


# -- Spend -------------------------------------------------------------------


def record_spend(
    entries: list[dict[str, Any]],
    *,
    month: str,
    monthly_usd: float,
    basis: str | None = None,
    monthly_mar: int | None = None,
    note: str | None = None,
    now: dt.datetime | None = None,
) -> list[dict[str, Any]]:
    """Return ``entries`` with the spend for ``month`` added, replacing any earlier one.

    Records only what the customer said; nothing is looked up or derived.
    """
    if not _MONTH.match(month or ""):
        raise ConsumptionError(f"month must be YYYY-MM, got {month!r}")
    if monthly_usd < 0:
        raise ConsumptionError("monthly_usd cannot be negative")
    if monthly_mar is not None and monthly_mar < 0:
        raise ConsumptionError("monthly_mar cannot be negative")
    if basis is not None and basis not in SPEND_BASES:
        raise ConsumptionError(f"basis must be one of {', '.join(SPEND_BASES)}")

    period_month = f"{month}-01"
    entry = {
        "period_month": period_month,
        "monthly_usd": float(monthly_usd),
        "basis": basis,
        "monthly_mar": monthly_mar,
        "note": note or None,
        "recorded_at": (now or dt.datetime.now(dt.timezone.utc)).isoformat(timespec="seconds"),
    }
    kept = [e for e in entries if e.get("period_month") != period_month]
    return sorted([*kept, entry], key=lambda e: e["period_month"])


def load_spend(path: Path | None) -> list[dict[str, Any]]:
    if not path or not path.exists():
        return []
    data = json.loads(path.read_text())
    entries = data.get("fivetran_spend") if isinstance(data, dict) else data
    if not isinstance(entries, list):
        raise ConsumptionError(f"{path} does not hold a fivetran_spend list")
    return entries


def save_spend(path: Path, entries: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"fivetran_spend": entries}, indent=2) + "\n")


# -- Export rows --------------------------------------------------------------


def parse_metastore(metastore_id: str) -> tuple[str | None, str | None]:
    """Split ``current_metastore()`` (``<cloud>:<region>:<uuid>``) into cloud and region."""
    parts = (metastore_id or "").split(":")
    if len(parts) < 3:
        return None, None
    return parts[0] or None, parts[1] or None


def consumption_rows(
    plan: dict[str, Any],
    mar_doc: dict[str, Any] | None,
    *,
    cloud: str | None = None,
    region: str | None = None,
    today: dt.date | None = None,
) -> list[dict[str, Any]]:
    records = _records(mar_doc)
    month = latest_full_month({r["measured_month"] for r in records}, today)
    by_key: dict[tuple[str, str], dict[str, int]] = {}
    for r in records:
        if r["measured_month"] != month:
            continue
        totals = by_key.setdefault(_mar_key(r["destination_id"], r["connection_name"]), {})
        totals["paid"] = totals.get("paid", 0) + (r["mar"] if r["free_type"] == PAID else 0)
        totals["total"] = totals.get("total", 0) + r["mar"]

    rows = []
    for item in plan.get("items", []):
        fivetran, target, schedule = item["fivetran"], item["target"], item.get("schedule", {})
        minutes = schedule.get("fivetran_minutes") or fivetran.get("sync_frequency_minutes")
        mode = schedule.get("mode")
        runs = None if mode == "continuous" or not minutes else round(MINUTES_PER_MONTH / minutes)
        mar = _match_mar(by_key, fivetran.get("group_id"), fivetran.get("destination_schema"))
        paid = mar.get("paid") if mar else None
        rows.append(
            {
                "connection_id": fivetran.get("connection_id"),
                "group_id": fivetran.get("group_id"),
                "fivetran_service": fivetran.get("service"),
                "fivetran_connection_name": fivetran.get("destination_schema"),
                "source_type": target.get("connection_type"),
                "category": target.get("category"),
                "migratable": not item.get("blockers"),
                "effort": target.get("effort"),
                "num_flows": len(item.get("objects", [])),
                "sync_minutes": minutes,
                "schedule_mode": mode,
                "runs_per_month": runs,
                "measured_month": _month_date(month) if mar else None,
                "paid_mar": paid,
                "total_mar": mar.get("total") if mar else None,
                "num_rows": round(paid / runs) if paid is not None and runs else None,
                "cloud": cloud,
                "region": region,
            }
        )
    return sorted(rows, key=lambda r: str(r["connection_id"]))


def billing_rows(
    mar_doc: dict[str, Any] | None,
    spend: list[dict[str, Any]],
    group_ids: set[str],
) -> list[dict[str, Any]]:
    rows = [
        {
            "source": "platform_connector",
            "group_id": b.get("group_id"),
            "measured_month": _month_date(b.get("measured_month")),
            "metric": b.get("metric"),
            "value": b.get("value"),
            "note": None,
        }
        for b in (mar_doc or {}).get("billing") or []
        if b.get("group_id") in group_ids
    ]
    rows += [
        {
            "source": "customer",
            "group_id": None,
            "measured_month": e.get("period_month"),
            "metric": f"reported_{e.get('basis') or 'unspecified'}_usd",
            "value": e.get("monthly_usd"),
            "note": e.get("note"),
        }
        for e in spend
        if e.get("monthly_usd") is not None
    ]
    return sorted(
        rows,
        key=lambda r: (r["source"], r["measured_month"] or "", r["group_id"] or "", r["metric"]),
    )


def mar_by_table_rows(plan: dict[str, Any], mar_doc: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Per-table MAR for the destinations in the plan only."""
    connection_ids: dict[tuple[str, str], str] = {}
    names: set[str] = set()
    group_ids: set[str] = set()
    for item in plan.get("items", []):
        f = item["fivetran"]
        name = (f.get("destination_schema") or "").lower()
        connection_ids[(f.get("group_id") or "", name)] = f.get("connection_id")
        names.add(name)
        if f.get("group_id"):
            group_ids.add(f["group_id"])

    rows = []
    for r in _records(mar_doc):
        name = r["connection_name"].lower()
        if r["destination_id"]:
            if r["destination_id"] not in group_ids:
                continue
            connection_id = connection_ids.get((r["destination_id"], name))
        else:
            if name not in names:
                continue
            connection_id = next((cid for (_, n), cid in connection_ids.items() if n == name), None)
        rows.append(
            {
                "connection_id": connection_id,
                **{k: r[k] for k in MAR_BY_TABLE_COLUMNS[1:]},
                "measured_month": _month_date(r["measured_month"]),
            }
        )
    return sorted(
        rows,
        key=lambda r: (
            r["measured_month"] or "",
            r["connection_name"],
            r["schema_name"],
            r["table_name"],
            r["free_type"],
        ),
    )


def plan_group_ids(plan: dict[str, Any]) -> set[str]:
    return {
        item["fivetran"]["group_id"]
        for item in plan.get("items", [])
        if item["fivetran"].get("group_id")
    }


# -- Gate 1 summary -----------------------------------------------------------


def summary_lines(
    consumption: list[dict[str, Any]],
    billing: list[dict[str, Any]],
    today: dt.date | None = None,
) -> list[tuple[str, str, str]]:
    """The fivetran_spend and fivetran_consumption sections of the Gate 1 report.

    Returns ``(section, item, detail)`` triples. Every value is a recorded
    figure; nothing is priced.
    """
    lines: list[tuple[str, str, str]] = []
    for row in billing:
        if row["source"] != "customer":
            continue
        basis = row["metric"].removeprefix("reported_").removesuffix("_usd")
        detail = f"${row['value']:,.2f}/month ({basis})"
        if row.get("note"):
            detail += f"; {row['note']}"
        lines.append(("fivetran_spend", str(row["measured_month"]), detail))

    with_mar = [r for r in consumption if r["paid_mar"] is not None]
    if with_mar:
        lines += [
            ("fivetran_consumption", "measured_month", str(with_mar[0]["measured_month"])),
            ("fivetran_consumption", "paid_mar", f"{sum(r['paid_mar'] for r in with_mar):,}"),
            ("fivetran_consumption", "connections_with_mar", str(len(with_mar))),
            (
                "fivetran_consumption",
                "connections_without_mar",
                str(len(consumption) - len(with_mar)),
            ),
        ]

    current = _month_date((today or dt.date.today()).strftime("%Y-%m"))
    billed = [
        r
        for r in billing
        if r["source"] == "platform_connector" and (r["measured_month"] or "") < current
    ]
    if billed:
        month = max(r["measured_month"] for r in billed)
        totals: dict[str, float] = {}
        for r in billed:
            if r["measured_month"] == month:
                totals[r["metric"]] = totals.get(r["metric"], 0.0) + float(r["value"])
        for metric, total in sorted(totals.items()):
            detail = f"{total:,.0f} credits" if metric == "credits_used" else f"${total:,.2f}"
            lines.append(("fivetran_consumption", f"billed {metric} {month}", detail))
    return lines


# -- Writing ------------------------------------------------------------------


def check_volume(volume: str) -> tuple[str, str, str]:
    if not _VOLUME.match(volume or ""):
        raise ConsumptionError(f"volume must be <catalog>.<schema>.<volume>, got {volume!r}")
    catalog, schema, name = volume.split(".")
    return catalog, schema, name


def write_export(
    parent: Path,
    consumption: list[dict[str, Any]],
    billing: list[dict[str, Any]],
    mar_by_table: list[dict[str, Any]],
    today: dt.date | None = None,
) -> tuple[Path, dict[str, int]]:
    """Write the export folder under ``parent``; return its path and row counts."""
    out = parent / f"fivetran_consumption_{(today or dt.date.today()).isoformat()}"
    out.mkdir(parents=True, exist_ok=True)
    files = {
        "connections.csv": (CONSUMPTION_COLUMNS, consumption),
        "fivetran_billing.csv": (BILLING_COLUMNS, billing),
        "mar_by_table.csv": (MAR_BY_TABLE_COLUMNS, mar_by_table),
    }
    counts = {}
    for file_name, (columns, rows) in files.items():
        with (out / file_name).open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(_csv_row(row) for row in rows)
        counts[file_name] = len(rows)
    (out / "README.txt").write_text(README_TEXT)
    return out, counts


def _csv_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        k: ("" if v is None else str(v).lower() if isinstance(v, bool) else v)
        for k, v in row.items()
    }


# -- Helpers ------------------------------------------------------------------


def _records(mar_doc: dict[str, Any] | None) -> list[dict[str, Any]]:
    out = []
    for r in (mar_doc or {}).get("records") or []:
        out.append(
            {
                "destination_id": str(r.get("destination_id") or ""),
                "connection_name": str(r.get("connection_name") or ""),
                "schema_name": str(r.get("schema_name") or ""),
                "table_name": str(r.get("table_name") or ""),
                "measured_month": str(r.get("measured_month") or "")[:7],
                "free_type": str(r.get("free_type") or PAID).upper(),
                "mar": int(r.get("mar") or 0),
            }
        )
    return out


def _mar_key(destination_id: str, connection_name: str) -> tuple[str, str]:
    return destination_id or "", (connection_name or "").lower()


def _match_mar(
    by_key: dict[tuple[str, str], dict[str, int]], group_id: str | None, name: str | None
) -> dict[str, int] | None:
    """MAR for a connection: by destination and name, or by name for exports without ids."""
    exact = by_key.get(_mar_key(group_id or "", name or ""))
    if exact is not None:
        return exact
    return by_key.get(_mar_key("", name or ""))


def _month_date(month: str | None) -> str | None:
    if not month:
        return None
    return f"{month[:7]}-01"
