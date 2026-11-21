"""The skill records Fivetran consumption and spend but never prices Lakeflow Connect.

Runs every offline stage on the sample inventory, with recorded spend, and checks
everything printed or written for cost-comparison language.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SKILL = REPO / "skills/fivetran-to-lakeflow-migration"
sys.path.insert(0, str(SKILL / "scripts"))

import fivetran_consumption
import generate_bundle
import plan_migration

COST_COMPARISON = [
    r"\bsavings?\b",
    r"\bsave[sd]? (you|money|\$)",
    r"\bcheaper\b",
    r"\b(more|less) expensive\b",
    r"\bDBUs?\b",
    r"\bTCO\b",
    r"\bROI\b",
    r"\blist[_ ]prices?\b",
    r"\bcost (comparison|projection|estimate)s?\b",
    r"\b(estimated|projected) (monthly |annual )?(cost|spend)\b",
    r"\bcost estimators?\b",
    r"lakeflow connect (would |will )?costs?\b",
    r"\bcompared? (to|with) fivetran\b",
    r"\$\s?[\d,.]+\s*(/|per)\s*(month|mo|year|yr)\b.*\b(lakeflow|databricks)\b",
]

DATABRICKS_PRICING_SOURCES = [
    r"system\.billing\.",
    r"\blist[_ ]prices?\b",
    r"\bDBUs?\b",
    r"\$\s?/\s?DBU\b",
    r"\brate[_ ]card\b",
    r"\bcost model\b",
    r"\bstage 2\.5\b",
]

REMOVED_COST_MODULES = [
    "scripts/compare_cost.py",
    "scripts/databricks_telemetry.py",
    "scripts/build_update_cost_table.py",
    "scripts/ftlfc/costs.py",
    "scripts/ftlfc/system_tables.py",
    "scripts/ftlfc/update_costs.py",
    "references/bundles-and-cost.md",
]


def _offenders(text: str) -> list[str]:
    return [p for p in COST_COMPARISON if re.search(p, text, re.IGNORECASE)]


def _all_outputs(out: Path, capsys: pytest.CaptureFixture) -> dict[str, str]:
    texts: dict[str, str] = {}

    def run(name: str, main, argv: list[str]) -> None:
        assert main(argv) == 0, name
        captured = capsys.readouterr()
        texts[name] = captured.out + captured.err

    plan = out / "plan.json"
    spend = out / "fivetran_spend.json"
    run(
        "plan_migration",
        plan_migration.main,
        ["-i", str(SKILL / "fixtures/inventory.sample.json"), "-c", "main_prod", "-o", str(plan)],
    )
    run(
        "spend",
        fivetran_consumption.main,
        [
            "spend",
            "--month",
            "2026-10",
            "--monthly-usd",
            "4200",
            "--basis",
            "invoice",
            "-o",
            str(spend),
        ],
    )
    run("summary", fivetran_consumption.main, ["summary", "-p", str(plan), "--spend", str(spend)])
    run(
        "export",
        fivetran_consumption.main,
        [
            "export",
            "-p",
            str(plan),
            "--spend",
            str(spend),
            "--cloud",
            "aws",
            "--region",
            "us-west-2",
            "-o",
            str(out / "export"),
        ],
    )
    run("generate_bundle", generate_bundle.main, ["-p", str(plan), "-o", str(out / "bundle")])

    for path in sorted(out.rglob("*")):
        if path.is_file():
            texts[str(path.relative_to(out))] = path.read_text()
    return texts


def test_generated_output_never_compares_cost(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    texts = _all_outputs(tmp_path, capsys)
    assert any(name.startswith("bundle/") for name in texts)
    assert any(name.startswith("export/") for name in texts)
    found = {name: hits for name, text in texts.items() if (hits := _offenders(text))}
    assert found == {}


def test_docs_and_scripts_never_point_to_a_cost_estimator() -> None:
    paths = [REPO / "README.md", *SKILL.rglob("*.md"), *SKILL.rglob("*.py")]
    found = [
        str(p.relative_to(REPO))
        for p in paths
        if re.search(r"\bcost estimators?\b", p.read_text(), re.IGNORECASE)
    ]
    assert found == []


def test_docs_and_scripts_never_read_databricks_pricing() -> None:
    paths = [REPO / "README.md", *SKILL.rglob("*.md"), *SKILL.rglob("*.py")]
    found = {
        str(p.relative_to(REPO)): hits
        for p in paths
        if (
            hits := [
                pat
                for pat in DATABRICKS_PRICING_SOURCES
                if re.search(pat, p.read_text(), re.IGNORECASE)
            ]
        )
    }
    assert found == {}


@pytest.mark.parametrize("relative", REMOVED_COST_MODULES)
def test_cost_model_modules_stay_removed(relative: str) -> None:
    assert not (SKILL / relative).exists()


@pytest.mark.parametrize(
    "line",
    [
        "Queries system.billing.list_prices for the customer's DBU rates.",
        "Ground the cost model with Databricks telemetry.",
        "## Stage 2.5: Telemetry",
        "Serverless jobs run at $0.35/DBU.",
    ],
)
def test_the_pricing_check_catches_rate_lookups(line: str) -> None:
    assert any(re.search(p, line, re.IGNORECASE) for p in DATABRICKS_PRICING_SOURCES)


def test_recorded_spend_is_reported_as_given(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    texts = _all_outputs(tmp_path, capsys)
    assert "4200" in texts["summary"] or "4,200" in texts["summary"]


@pytest.mark.parametrize(
    "sentence",
    [
        "Lakeflow Connect would cost $21.90 a month against $200 a month on Fivetran.",
        "Estimated monthly cost: 40 DBUs.",
        "Moving saves you 60%.",
        "Lakeflow Connect is cheaper.",
        "These columns match the Lakeflow Connect cost estimator inputs.",
    ],
)
def test_the_check_catches_comparison_language(sentence: str) -> None:
    assert _offenders(sentence)
