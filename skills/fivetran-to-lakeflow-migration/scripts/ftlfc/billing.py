"""Cost comparison between Fivetran and Lakeflow Connect.

Design constraint: this model must never produce a single confident number for a
customer. Fivetran bills on Monthly Active Rows against a negotiated contract,
and Lakeflow Connect bills serverless DBUs whose consumption depends on source
volume, change rate, and sync frequency. Neither side is knowable from a
connector inventory alone.

So the model:

* prefers measured inputs (the customer's Fivetran invoice, and their actual
  Lakeflow Connect spend from ``system.billing.usage``) over estimates,
* labels every estimate with the assumption that produced it,
* emits low / base / high scenarios instead of a point estimate,
* refuses to compare when the inputs are too weak, rather than guessing.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from .catalog import load_pricing

HOURS_PER_MONTH = 730

#: Confidence in the comparison, driven purely by which inputs were supplied.
CONFIDENCE = {
    "measured": (
        "Both sides are measured: a known Fivetran cost and observed Lakeflow "
        "Connect spend. Safe to show a customer."
    ),
    "partial": (
        "One side is measured and one is modelled. Present as a directional "
        "range, not a quote."
    ),
    "modelled": (
        "Both sides are modelled from assumptions in data/pricing.yaml. Use "
        "internally to decide whether a deeper analysis is worth it. Do not "
        "present these numbers to a customer as a quote."
    ),
}


def compare(
    plan: dict[str, Any],
    fivetran_annual_cost: float | None = None,
    fivetran_monthly_mar: float | None = None,
    lfc_actual_monthly_cost: float | None = None,
    pricing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a cost comparison for a migration plan."""
    pricing = pricing or load_pricing()

    fivetran = _fivetran_side(fivetran_annual_cost, fivetran_monthly_mar, pricing)
    lakeflow = _lakeflow_side(plan, lfc_actual_monthly_cost, pricing)

    measured_sides = int(fivetran["basis"] == "measured") + int(lakeflow["basis"] == "measured")
    confidence = ("measured", "partial", "modelled")[2 - measured_sides]

    comparison = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "pricing_last_reviewed": pricing.get("last_reviewed"),
        "confidence": confidence,
        "confidence_note": CONFIDENCE[confidence],
        "fivetran": fivetran,
        "lakeflow_connect": lakeflow,
        "scenarios": _scenarios(fivetran, lakeflow),
        "caveats": _caveats(fivetran, lakeflow, plan, pricing),
    }
    return comparison


def _fivetran_side(
    annual_cost: float | None, monthly_mar: float | None, pricing: dict[str, Any]
) -> dict[str, Any]:
    if annual_cost is not None:
        monthly = annual_cost / 12
        return {
            "basis": "measured",
            "source": "customer-supplied annual Fivetran cost",
            "monthly_cost_low": monthly,
            "monthly_cost_base": monthly,
            "monthly_cost_high": monthly,
            "monthly_mar": monthly_mar,
            "detail": f"${annual_cost:,.0f}/year contract value divided evenly across 12 months.",
        }

    if monthly_mar is not None:
        tiers = pricing.get("fivetran", {}).get("mar_tiers") or []
        if not tiers:
            return _unknown_side(
                "Fivetran",
                "No MAR pricing tiers are configured in data/pricing.yaml and no "
                "contract value was supplied.",
            )
        low, base, high = _mar_cost_range(monthly_mar, tiers, pricing)
        return {
            "basis": "modelled",
            "source": "MAR volume priced against published list tiers",
            "monthly_cost_low": low,
            "monthly_cost_base": base,
            "monthly_cost_high": high,
            "monthly_mar": monthly_mar,
            "detail": (
                f"{monthly_mar:,.0f} MAR priced against the list tiers in "
                "data/pricing.yaml. Real invoices are almost always discounted "
                "below list, so treat the low end as more likely."
            ),
        }

    return _unknown_side(
        "Fivetran",
        "Supply --fivetran-annual-cost (best) or --fivetran-monthly-mar. MAR is "
        "available from the Fivetran Platform Connector's incremental_mar table, "
        "or from the Usage page in the Fivetran dashboard.",
    )


def _mar_cost_range(
    monthly_mar: float, tiers: list[dict[str, Any]], pricing: dict[str, Any]
) -> tuple[float, float, float]:
    """Price MAR through declining marginal rate tiers."""
    remaining = monthly_mar
    cost = 0.0
    previous_ceiling = 0.0
    for tier in sorted(tiers, key=lambda t: t.get("up_to_mar") or float("inf")):
        ceiling = tier.get("up_to_mar") or float("inf")
        span = min(remaining, ceiling - previous_ceiling)
        if span <= 0:
            break
        cost += (span / 1000.0) * float(tier["usd_per_1k_mar"])
        remaining -= span
        previous_ceiling = ceiling
        if remaining <= 0:
            break

    discount = pricing.get("fivetran", {}).get("typical_discount_range") or [0.0, 0.0]
    low = cost * (1.0 - float(max(discount)))
    high = cost * (1.0 - float(min(discount)))
    base = (low + high) / 2
    return low, base, high


def _lakeflow_side(
    plan: dict[str, Any], actual_monthly_cost: float | None, pricing: dict[str, Any]
) -> dict[str, Any]:
    if actual_monthly_cost is not None:
        return {
            "basis": "measured",
            "source": "system.billing.usage for existing Lakeflow Connect pipelines",
            "monthly_cost_low": actual_monthly_cost,
            "monthly_cost_base": actual_monthly_cost,
            "monthly_cost_high": actual_monthly_cost,
            "detail": (
                "Observed Lakeflow Connect spend. This is the strongest available "
                "input; scale it by the ratio of migrated to existing tables if the "
                "existing footprint is only part of the plan."
            ),
            "components": [],
        }

    lfc = pricing.get("lakeflow_connect") or {}
    dbu_price = lfc.get("usd_per_dbu")
    if not dbu_price:
        return _unknown_side(
            "Lakeflow Connect",
            "No usd_per_dbu configured in data/pricing.yaml.",
        )

    components: list[dict[str, Any]] = []
    total_low = total_base = total_high = 0.0

    for connection in plan.get("connections", []):
        if connection.get("blockers") or not connection.get("objects"):
            continue
        estimate = _estimate_connection_dbus(connection, lfc)
        for key, value in (
            ("low", "dbus_low"),
            ("base", "dbus_base"),
            ("high", "dbus_high"),
        ):
            cost = estimate[value] * float(dbu_price)
            estimate[f"cost_{key}"] = cost
        total_low += estimate["cost_low"]
        total_base += estimate["cost_base"]
        total_high += estimate["cost_high"]
        components.append(estimate)

    return {
        "basis": "modelled",
        "source": "planned pipelines priced against assumptions in data/pricing.yaml",
        "monthly_cost_low": total_low,
        "monthly_cost_base": total_base,
        "monthly_cost_high": total_high,
        "detail": (
            "Modelled from pipeline count, table count, schedule, and gateway "
            "requirements. DBU consumption scales with data volume and change "
            "rate, neither of which the Fivetran inventory exposes, so the spread "
            "between low and high is wide by design."
        ),
        "components": components,
    }


def _estimate_connection_dbus(connection: dict[str, Any], lfc: dict[str, Any]) -> dict[str, Any]:
    schedule = connection["schedule"]
    table_count = len(connection["objects"])

    dbu_per_run = float(lfc.get("dbu_per_pipeline_run", 0.0))
    dbu_per_table_run = float(lfc.get("dbu_per_table_per_run", 0.0))
    gateway_dbu_per_hour = float(lfc.get("gateway_dbu_per_hour", 0.0))
    spread = float(lfc.get("estimate_spread", 0.5))

    if schedule["mode"] == "continuous":
        runs_per_month = float(lfc.get("continuous_equivalent_runs_per_month", HOURS_PER_MONTH * 4))
    else:
        minutes = schedule.get("fivetran_sync_frequency_minutes") or 1440
        runs_per_month = (HOURS_PER_MONTH * 60) / max(minutes, 5)

    ingest_dbus = runs_per_month * (dbu_per_run + dbu_per_table_run * table_count)

    gateway_dbus = 0.0
    if connection["target"].get("requires_gateway"):
        gateway_dbus = gateway_dbu_per_hour * HOURS_PER_MONTH

    base = ingest_dbus + gateway_dbus
    return {
        "fivetran_service": connection["fivetran_service"],
        "pipeline_name": connection["pipeline_name"],
        "tables": table_count,
        "schedule_mode": schedule["mode"],
        "runs_per_month": round(runs_per_month, 1),
        "requires_gateway": bool(connection["target"].get("requires_gateway")),
        "dbus_low": base * (1 - spread),
        "dbus_base": base,
        "dbus_high": base * (1 + spread),
    }


def _unknown_side(name: str, reason: str) -> dict[str, Any]:
    return {
        "basis": "unknown",
        "source": None,
        "monthly_cost_low": None,
        "monthly_cost_base": None,
        "monthly_cost_high": None,
        "detail": f"{name} cost could not be established. {reason}",
        "components": [],
    }


def _scenarios(fivetran: dict[str, Any], lakeflow: dict[str, Any]) -> list[dict[str, Any]]:
    """Cross the two ranges into best / expected / worst outcomes for migrating."""
    if fivetran["basis"] == "unknown" or lakeflow["basis"] == "unknown":
        return []

    pairs = (
        ("best_case", fivetran["monthly_cost_high"], lakeflow["monthly_cost_low"]),
        ("expected", fivetran["monthly_cost_base"], lakeflow["monthly_cost_base"]),
        ("worst_case", fivetran["monthly_cost_low"], lakeflow["monthly_cost_high"]),
    )
    scenarios = []
    for label, fivetran_cost, lakeflow_cost in pairs:
        savings = fivetran_cost - lakeflow_cost
        scenarios.append(
            {
                "scenario": label,
                "fivetran_monthly": fivetran_cost,
                "lakeflow_monthly": lakeflow_cost,
                "monthly_savings": savings,
                "annual_savings": savings * 12,
                "savings_pct": (savings / fivetran_cost * 100) if fivetran_cost else None,
            }
        )
    return scenarios


def _caveats(
    fivetran: dict[str, Any],
    lakeflow: dict[str, Any],
    plan: dict[str, Any],
    pricing: dict[str, Any],
) -> list[str]:
    caveats = [
        "Fivetran bills Monthly Active Rows against a negotiated contract; list "
        "pricing overstates what most customers actually pay.",
        "Lakeflow Connect bills serverless DBUs driven by data volume and change "
        "rate. A connector inventory does not expose either, so any estimate "
        "made without a trial pipeline is directional only.",
        "The comparison covers ingestion only. It excludes the destination "
        "warehouse or DLT compute that both options feed, which is unchanged by "
        "the migration.",
    ]

    if lakeflow["basis"] == "modelled":
        caveats.append(
            "To replace the Lakeflow Connect estimate with a measurement, migrate "
            "one representative connection, let it run for a week, then query "
            "system.billing.usage and re-run this comparison with "
            "--lfc-actual-monthly-cost."
        )
    if fivetran["basis"] == "modelled":
        caveats.append(
            "Ask the customer for their Fivetran invoice or contract value and "
            "re-run with --fivetran-annual-cost. That single input removes most "
            "of the uncertainty."
        )
    if plan["summary"].get("connections_blocked"):
        caveats.append(
            f"{plan['summary']['connections_blocked']} connection(s) cannot move to "
            "Lakeflow Connect today, so the customer keeps a Fivetran contract. "
            "Fivetran's platform fee and tier minimums may not scale down "
            "proportionally with the removed MAR."
        )
    if plan["summary"].get("gateways_required"):
        caveats.append(
            f"{plan['summary']['gateways_required']} source(s) need an ingestion "
            "gateway, which runs continuously and is billed even when the source "
            "is idle. This is often the largest single line item."
        )
    if not pricing.get("last_reviewed"):
        caveats.append("data/pricing.yaml has no last_reviewed date; verify the rates.")
    return caveats
