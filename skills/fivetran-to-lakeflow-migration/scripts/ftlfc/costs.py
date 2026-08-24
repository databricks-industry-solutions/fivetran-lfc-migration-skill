"""Fivetran vs Lakeflow Connect cost modelling.

The two sides of this comparison are not equally knowable, and pretending
otherwise is how migration business cases fall apart in front of a customer.

**Fivetran cost is measurable.** MAR comes from the Platform Connector and the
rate is a published-ish per-million figure, so the number is defensible.

**Lakeflow Connect cost is not predictable in advance.** Databricks publishes no
DBU-per-row or DBU-per-GB coefficient for managed ingestion. Their own guidance
for serverless is to "run and benchmark a representative workload and then
analyze the billing system table." So anything this module produces for the
Databricks side is a *scenario built on stated assumptions*, not a forecast.

Three consequences shape the design:

1. Every assumption is explicit, overridable, and echoed in the output. A
   comparison whose assumptions are invisible is worse than no comparison.
2. The ingestion-gateway floor is modelled separately, because it is continuous
   classic compute and therefore roughly deterministic -- often the true floor
   of the bill regardless of data volume.
3. Classic-compute cloud infrastructure (EC2/VM, EBS, egress) never appears in
   system.billing.usage at all, so it is added as an explicit uplift rather than
   quietly omitted.

Use ``measured_lakeflow_cost`` with real pilot data whenever it exists. The
modelled path is for sizing conversations before a pilot is possible.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

HOURS_PER_MONTH = 730.0

# Observed live in system.billing.list_prices (AWS, USD, currently effective).
# Managed ingestion resolves to the serverless *jobs* SKU, not a DLT SKU, and
# there is no dedicated Lakeflow Connect SKU at all.
DEFAULT_SERVERLESS_DBU_USD = 0.45      # ENTERPRISE_JOBS_SERVERLESS_COMPUTE_<REGION>
DEFAULT_GATEWAY_DBU_USD = 0.36         # ENTERPRISE_DLT_ADVANCED_COMPUTE

# Derived from Fivetran's own pricing-page examples, which land consistently on
# ~$500 per million MAR below 1M. Fivetran publishes no rate card.
DEFAULT_FIVETRAN_USD_PER_MILLION_MAR = 500.0
DEFAULT_FIVETRAN_BASE_CHARGE = 5.0

# Classic compute carries a cloud bill Databricks never sees. FinOps rule of
# thumb is 50-100% on top of the DBU cost; the midpoint is the default.
DEFAULT_INFRA_UPLIFT = 0.75


@dataclass
class RateCard:
    """Prices and modelling assumptions. Every field is an input, not a fact."""

    serverless_dbu_usd: float = DEFAULT_SERVERLESS_DBU_USD
    gateway_dbu_usd: float = DEFAULT_GATEWAY_DBU_USD
    fivetran_usd_per_million_mar: float = DEFAULT_FIVETRAN_USD_PER_MILLION_MAR
    fivetran_base_charge_per_connection: float = DEFAULT_FIVETRAN_BASE_CHARGE

    #: DBU/hour for a continuously-running ingestion gateway. Depends entirely
    #: on the node types chosen; Databricks recommends a large driver with small
    #: workers at a minimum of 8 cores. UNVERIFIED default -- measure it.
    gateway_dbu_per_hour: float = 2.0

    #: Minutes of serverless compute per ingestion pipeline run. The single
    #: most load-bearing assumption in the model, and the one a pilot replaces.
    minutes_per_run: float = 6.0

    #: Cloud infrastructure multiplier applied to classic-compute (gateway) DBU
    #: cost only. Serverless already includes infrastructure in the DBU price.
    infra_uplift: float = DEFAULT_INFRA_UPLIFT

    def describe_assumptions(self) -> list[str]:
        """The caveats that must travel with any number this module produces."""
        return [
            f"Serverless ingestion priced at ${self.serverless_dbu_usd:.2f}/DBU and gateway "
            f"compute at ${self.gateway_dbu_usd:.2f}/DBU. These are AWS list prices; they vary "
            "by region and tier, and list price is not invoiced price.",
            f"Each ingestion run is assumed to consume {self.minutes_per_run:g} minutes of "
            "serverless compute. Databricks publishes no DBU-per-row or DBU-per-GB "
            "coefficient, so this is the model's largest source of error.",
            f"A continuously-running gateway is assumed to draw {self.gateway_dbu_per_hour:g} "
            "DBU/hour. This depends entirely on node sizing and is unverified.",
            f"Classic-compute cloud infrastructure is added at {self.infra_uplift:.0%} on top of "
            "gateway DBU cost. It never appears in system.billing.usage.",
            f"Fivetran cost is modelled at ${self.fivetran_usd_per_million_mar:,.0f} per million "
            "MAR. Fivetran publishes no rate card; this is derived from their pricing examples "
            "and is wrong for Enterprise, Business Critical, and any ELA.",
            "Excluded from both sides: storage, egress, private networking, and downstream "
            "transformation compute.",
        ]


@dataclass
class ComponentCost:
    label: str
    monthly_usd: float
    detail: str = ""


@dataclass
class Comparison:
    fivetran_monthly_usd: float
    lakeflow_monthly_usd: float
    components: list[ComponentCost] = field(default_factory=list)
    basis: str = "modelled"
    assumptions: list[str] = field(default_factory=list)
    caveats: list[str] = field(default_factory=list)

    @property
    def delta_usd(self) -> float:
        return self.fivetran_monthly_usd - self.lakeflow_monthly_usd

    @property
    def delta_pct(self) -> float | None:
        if not self.fivetran_monthly_usd:
            return None
        return self.delta_usd / self.fivetran_monthly_usd * 100

    def to_dict(self) -> dict[str, Any]:
        return {
            "basis": self.basis,
            "fivetran_monthly_usd": round(self.fivetran_monthly_usd, 2),
            "lakeflow_monthly_usd": round(self.lakeflow_monthly_usd, 2),
            "delta_usd": round(self.delta_usd, 2),
            "delta_pct": round(self.delta_pct, 1) if self.delta_pct is not None else None,
            "components": [
                {"label": c.label, "monthly_usd": round(c.monthly_usd, 2), "detail": c.detail}
                for c in self.components
            ],
            "assumptions": self.assumptions,
            "caveats": self.caveats,
        }


# -- Fivetran side (measurable) ---------------------------------------------


def fivetran_monthly_cost(
    monthly_mar: int, connection_count: int, rates: RateCard, reported_spend: float | None = None
) -> tuple[float, str]:
    """Fivetran monthly cost, preferring the customer's reported spend.

    Reported spend from the Platform Connector's usage_cost table beats any
    model, because it reflects the customer's actual contract, discounts, and
    tier. The modelled figure is the fallback.
    """
    if reported_spend is not None:
        return reported_spend, "reported"

    usage = monthly_mar / 1_000_000 * rates.fivetran_usd_per_million_mar
    base = connection_count * rates.fivetran_base_charge_per_connection
    return usage + base, "modelled"


# -- Lakeflow Connect side (scenario, unless measured) ----------------------


def runs_per_month(schedule: dict[str, Any]) -> float:
    """How often a pipeline's companion job fires each month."""
    if schedule.get("mode") == "continuous":
        # A continuous job is not a run count; it is billed as sustained
        # compute. Model it as the full month.
        return HOURS_PER_MONTH
    minutes = schedule.get("fivetran_minutes") or 360
    return HOURS_PER_MONTH * 60 / minutes


def model_lakeflow_cost(plan: dict[str, Any], rates: RateCard) -> list[ComponentCost]:
    """Build the Databricks-side scenario from the migration plan."""
    components: list[ComponentCost] = []

    migratable = [i for i in plan["items"] if not i["blockers"]]

    # Serverless ingestion: run frequency x assumed duration x DBU rate.
    serverless_hours = 0.0
    continuous_pipelines = 0
    for item in migratable:
        schedule = item["schedule"]
        if schedule.get("mode") == "continuous":
            continuous_pipelines += 1
            serverless_hours += HOURS_PER_MONTH
        else:
            serverless_hours += runs_per_month(schedule) * (rates.minutes_per_run / 60)

    # Serverless DBU/hour is folded into the per-DBU price; treating one hour of
    # serverless as one DBU keeps the assumption visible in exactly one place.
    serverless_cost = serverless_hours * rates.serverless_dbu_usd
    detail = f"{serverless_hours:,.0f} compute-hours/month across {len(migratable)} pipelines"
    if continuous_pipelines:
        detail += f", of which {continuous_pipelines} run continuously"
    components.append(ComponentCost("Serverless ingestion", serverless_cost, detail))

    # Gateways: continuous classic compute, roughly deterministic.
    gateway_count = sum(1 for i in migratable if i["target"]["gateway"] == "required")
    if gateway_count:
        gateway_dbus = gateway_count * HOURS_PER_MONTH * rates.gateway_dbu_per_hour
        gateway_cost = gateway_dbus * rates.gateway_dbu_usd
        components.append(
            ComponentCost(
                "Ingestion gateways (continuous)",
                gateway_cost,
                f"{gateway_count} gateway(s) running 24/7 at {rates.gateway_dbu_per_hour:g} DBU/hr",
            )
        )
        components.append(
            ComponentCost(
                "Cloud infrastructure for gateways",
                gateway_cost * rates.infra_uplift,
                "EC2/VM, storage, and egress for classic compute. Not billed by Databricks.",
            )
        )

    return components


def measured_lakeflow_cost(usage_rows: list[dict[str, Any]]) -> list[ComponentCost]:
    """Build the Databricks side from real system.billing.usage rows.

    Expects rows shaped like the per-pipeline cost query in the bundles-and-cost
    reference: pipeline_type and list_cost_usd per pipeline per month.
    """
    by_type: dict[str, float] = {}
    for row in usage_rows:
        pipeline_type = str(row.get("pipeline_type") or "unknown")
        try:
            cost = float(row.get("list_cost_usd") or 0)
        except (TypeError, ValueError):
            continue
        by_type[pipeline_type] = by_type.get(pipeline_type, 0.0) + cost

    label = {
        "INGESTION_PIPELINE": "Serverless ingestion (measured)",
        "INGESTION_GATEWAY": "Ingestion gateways (measured)",
    }
    return [
        ComponentCost(label.get(k, f"{k} (measured)"), v, "From system.billing.usage at list price")
        for k, v in sorted(by_type.items(), key=lambda kv: -kv[1])
    ]


# -- Comparison --------------------------------------------------------------


def compare(
    plan: dict[str, Any],
    mar: dict[str, Any] | None,
    rates: RateCard,
    reported_spend: float | None = None,
    measured_usage: list[dict[str, Any]] | None = None,
) -> Comparison:
    """Put the two sides next to each other, with the caveats attached."""
    monthly_mar = (mar or {}).get("latest_month_mar", 0)
    connection_count = plan["summary"]["connections_total"]

    fivetran_cost, fivetran_basis = fivetran_monthly_cost(
        monthly_mar, connection_count, rates, reported_spend
    )

    if measured_usage:
        components = measured_lakeflow_cost(measured_usage)
        basis = "measured"
    else:
        components = model_lakeflow_cost(plan, rates)
        basis = "modelled"

    caveats = _caveats(plan, mar, fivetran_basis, basis)

    return Comparison(
        fivetran_monthly_usd=fivetran_cost,
        lakeflow_monthly_usd=sum(c.monthly_usd for c in components),
        components=components,
        basis=basis,
        assumptions=rates.describe_assumptions() if basis == "modelled" else [],
        caveats=caveats,
    )


def _caveats(
    plan: dict[str, Any], mar: dict[str, Any] | None, fivetran_basis: str, lakeflow_basis: str
) -> list[str]:
    caveats: list[str] = []

    if lakeflow_basis == "modelled":
        caveats.append(
            "The Lakeflow Connect figure is a scenario, not a forecast. Databricks' own "
            "guidance is to benchmark a representative workload and read the billing system "
            "table. Run a 3-7 day pilot on a representative table subset before quoting it."
        )

    if not mar:
        caveats.append(
            "No MAR data supplied, so the Fivetran side is not grounded in the customer's "
            "actual volume. Collect it with fivetran_mar.py first."
        )
    elif fivetran_basis == "modelled":
        caveats.append(
            "Fivetran cost is modelled from MAR rather than read from usage_cost. If the "
            "customer is on an ELA, MAR does not correlate with spend at all and this "
            "comparison is meaningless -- check their invoice."
        )

    blocked = plan["summary"]["connections_blocked"]
    if blocked:
        caveats.append(
            f"{blocked} connection(s) have no automatable Lakeflow Connect path and are excluded "
            "from the Databricks estimate. Their Fivetran cost does not go away on migration."
        )

    scd2 = plan["summary"]["tables_scd2"]
    if scd2:
        caveats.append(
            f"{scd2} table(s) need SCD type 2. History tracking materially increases write "
            "volume and therefore cost, which this model does not size separately."
        )

    if plan["summary"]["gateways_required"]:
        caveats.append(
            "Gateways run continuously whether or not data is flowing, so they set a floor "
            "on the bill that is independent of volume. Consolidating database sources behind "
            "fewer gateways is usually the largest single saving."
        )

    return caveats


def rate_card_from_overrides(overrides: dict[str, Any] | None) -> RateCard:
    if not overrides:
        return RateCard()
    known = set(asdict(RateCard()))
    unknown = set(overrides) - known
    if unknown:
        raise ValueError(
            f"Unknown rate card field(s): {', '.join(sorted(unknown))}. "
            f"Valid fields: {', '.join(sorted(known))}"
        )
    return RateCard(**overrides)
