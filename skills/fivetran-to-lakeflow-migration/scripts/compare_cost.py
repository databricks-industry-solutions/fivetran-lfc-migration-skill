#!/usr/bin/env python3
"""Stage 4 - compare Fivetran spend against projected Lakeflow Connect cost.

    python3 compare_cost.py -p out/plan.json --mar out/mar.json -o out/cost.json
    python3 compare_cost.py -p out/plan.json --telemetry out/telemetry.json
    python3 compare_cost.py -p out/plan.json --rates '{"minutes_per_run": 12}'

The Fivetran side is measurable. The Databricks side is not: Databricks
publishes no DBU-per-row coefficient for managed ingestion, so unless you supply
measured pilot usage the projection is a scenario built on stated assumptions,
all of which are printed alongside the result.

If ``--telemetry`` is supplied (from ``databricks_telemetry.py``, stage 2.5),
the rate card is pre-seeded with observed system-table values. Explicit
``--rates`` flags take highest priority and override both defaults and telemetry.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ftlfc.costs import compare, rate_card_from_overrides


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-p", "--plan", type=Path, default=Path("out/plan.json"))
    parser.add_argument("--mar", type=Path, help="MAR JSON from fivetran_mar.py")
    parser.add_argument(
        "--measured",
        type=Path,
        help="JSON array of system.billing.usage rows from a pilot, which replaces the model",
    )
    parser.add_argument(
        "--telemetry",
        type=Path,
        help="Telemetry JSON from databricks_telemetry.py (stage 2.5). Its rate_card "
        "values take priority over defaults but are overridden by --rates.",
    )
    parser.add_argument(
        "--rates", help="JSON object overriding rate card fields, e.g. '{\"minutes_per_run\": 12}'"
    )
    parser.add_argument("-o", "--output", default="out/cost.json")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
        stream=sys.stderr,
    )

    if not args.plan.exists():
        print(f"error: {args.plan} not found. Run plan_migration.py first.", file=sys.stderr)
        return 1
    plan = json.loads(args.plan.read_text())

    mar = None
    reported_spend = None
    if args.mar:
        mar_doc = json.loads(args.mar.read_text())
        mar = mar_doc.get("mar")
        reported_spend = _latest_reported_spend(mar_doc, mar)

    measured = json.loads(args.measured.read_text()) if args.measured else None

    try:
        merged_overrides = _merge_rate_overrides(args.telemetry, args.rates)
        rates = rate_card_from_overrides(merged_overrides)
    except (ValueError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    comparison = compare(plan, mar, rates, reported_spend, measured)

    destination = Path(args.output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(comparison.to_dict(), indent=2) + "\n")

    _report(comparison, destination)
    return 0


def _merge_rate_overrides(
    telemetry_path: Path | None, rates_json: str | None
) -> dict | None:
    """Layer telemetry rate_card under explicit --rates overrides.

    Telemetry values (from system tables) replace defaults, but explicit
    --rates flags take highest priority so the user can always override.
    """
    merged: dict = {}
    if telemetry_path and telemetry_path.exists():
        telemetry = json.loads(telemetry_path.read_text())
        rc = telemetry.get("rate_card", {})
        merged.update(rc)
        grounded = telemetry.get("enrichment_log", {}).get("grounded_fields", [])
        if grounded:
            logging.info(
                "Loaded %d grounded rate(s) from telemetry: %s",
                len(grounded),
                telemetry_path,
            )
    if rates_json:
        explicit = json.loads(rates_json)
        merged.update(explicit)
    return merged or None


def _latest_reported_spend(mar_doc: dict, mar: dict | None) -> float | None:
    """Total reported Fivetran spend for the latest month, if available."""
    spend = mar_doc.get("spend") or []
    latest = (mar or {}).get("latest_month")
    if not spend or not latest:
        return None
    rows = [
        r
        for r in spend
        if r.get("cost_usd") is not None and str(r.get("measured_month", "")).startswith(latest)
    ]
    if not rows:
        return None
    return sum(float(r["cost_usd"]) for r in rows)


def _report(comparison, destination: Path) -> None:
    data = comparison.to_dict()
    print(f"Wrote {destination}")
    print()
    print(f"  Fivetran today:      ${data['fivetran_monthly_usd']:>12,.2f} / month")
    print(
        f"  Lakeflow Connect:    ${data['lakeflow_monthly_usd']:>12,.2f} / month  ({data['basis']})"
    )
    print("  " + "-" * 44)
    delta = data["delta_usd"]
    direction = "saving" if delta >= 0 else "increase"
    pct = f" ({abs(data['delta_pct']):.0f}%)" if data["delta_pct"] is not None else ""
    print(f"  Projected {direction}:   ${abs(delta):>12,.2f} / month{pct}")

    if data["components"]:
        print("\n  Databricks breakdown:")
        for component in data["components"]:
            print(f"    ${component['monthly_usd']:>10,.2f}  {component['label']}")
            if component["detail"]:
                print(f"                 {component['detail']}")

    if data["assumptions"]:
        print("\n  Assumptions:", file=sys.stderr)
        for assumption in data["assumptions"]:
            print(f"    - {assumption}", file=sys.stderr)

    if data["caveats"]:
        print("\n  Caveats:", file=sys.stderr)
        for caveat in data["caveats"]:
            print(f"    - {caveat}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
