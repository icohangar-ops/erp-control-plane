"""Tests for the GenBI evaluation gate (analytics/evals)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from analytics.evals import run_evals
from analytics.evals.generate_golden import QUESTION_TEMPLATES, WINDOW_COLUMNS
from analytics.metrics.registry import MetricDefinition, MetricRegistry, Population

GOLDEN_PATH = Path(__file__).resolve().parent.parent / "analytics" / "evals" / "golden_qa.yaml"

# Every non-window metric column of main_marts.kpi_headline must have a
# golden question; regenerate with analytics/evals/generate_golden.py.
EXPECTED_METRICS = {
    "gmroi",
    "inventory_turns",
    "dio_days",
    "weeks_of_supply",
    "gross_margin_pct",
    "line_fill_rate",
    "order_fill_rate",
    "otd_pct",
    "otif_pct",
    "backorder_rate",
    "vendor_fill_rate",
    "ppv_pct",
    "dso_days",
    "dpo_days",
    "ccc_days",
    "close_cycle_days",
    "same_branch_revenue_pct",
    "organic_revenue_pct",
    "acquired_revenue_pct",
    "sales_per_fte_annualized",
    "avg_ticket",
}


def test_question_templates_cover_every_mart_metric() -> None:
    assert set(QUESTION_TEMPLATES) == EXPECTED_METRICS
    assert not (set(QUESTION_TEMPLATES) & WINDOW_COLUMNS)


def test_golden_set_schema_and_coverage() -> None:
    document = run_evals.load_golden(GOLDEN_PATH)
    cases = document["cases"]
    assert {case["id"] for case in cases} == EXPECTED_METRICS
    for case in cases:
        assert case["question"].endswith("?")
        assert case["unit"] in run_evals.VALID_UNITS
        assert case["tolerance"] > 0
        assert isinstance(case["expected"], (int, float))


def test_percent_answers_accepted_in_either_convention() -> None:
    assert run_evals.is_within_tolerance("percent", 0.2443, 0.005, 0.2443)
    assert run_evals.is_within_tolerance("percent", 0.2443, 0.005, 24.43)
    assert not run_evals.is_within_tolerance("percent", 0.2443, 0.005, 25.0)


def test_extract_first_number_handles_formatting() -> None:
    assert run_evals.extract_first_number("GMROI is 1.73x") == 1.73
    assert run_evals.extract_first_number("The value is $13,306.99") == 13306.99
    assert run_evals.extract_first_number("line fill rate 91.13%") == 91.13
    assert run_evals.extract_first_number("no numbers here") is None


def test_replay_mode_scores_recorded_answers(tmp_path: Path) -> None:
    golden = run_evals.load_golden(GOLDEN_PATH)
    replay_path = tmp_path / "replay.json"
    replay_path.write_text(
        json.dumps(
            {
                "cases": [
                    {"id": "gmroi", "status": "finished", "answer_text": "GMROI is 1.73x"},
                    {"id": "dio_days", "status": "finished", "answer_text": "about 70 days"},
                ]
            }
        )
    )
    results = run_evals.run_replay(golden, replay_path, duckdb_path="unused")
    by_id = {r["id"]: r for r in results}
    assert by_id["gmroi"]["correct"] is True
    assert by_id["dio_days"]["correct"] is False
    assert all(r["id"] in EXPECTED_METRICS for r in results)


def test_parity_against_local_mart() -> None:
    mart = Path("data/analytics/analytics.duckdb")
    if not mart.exists():
        pytest.skip("dbt mart not built locally; CI dbt job runs the real gate")
    golden = run_evals.load_golden(GOLDEN_PATH)
    assert run_evals.check_parity(golden, str(mart)) == []


# ---------------------------------------------------------------------------
# Metric-registry refusal (the NL surface may only pick registry-qualified names)


def _open_po_entry(name: str, measured: int) -> MetricDefinition:
    source = name.split("@", 1)[1]
    return MetricDefinition(
        name=name,
        description=f"{source} open purchase-order lines.",
        dbt_model="fact_purchase_order_line",
        expression="count(*) filter (where not is_received_in_full)",
        population=Population(
            count_sql=(
                "select count(*) from main_canonical.fact_purchase_order_line "
                f"where source_system = '{source}' and not is_received_in_full"
            ),
            measured=measured,
        ),
    )


def test_parity_refuses_non_registry_metric() -> None:
    refusals = run_evals.refuse_non_registry_metrics(
        {"cases": [{"id": "bogus", "metric": "not_a_registered_metric"}]},
        run_evals.load_registry(),
    )
    assert len(refusals) == 1
    assert "bogus" in refusals[0]
    assert "closed vocabulary" in refusals[0]


def test_parity_refuses_metric_bound_outside_headline_mart() -> None:
    refusals = run_evals.refuse_non_registry_metrics(
        {"cases": [{"id": "po", "metric": "open_po@csv_sftp"}]},
        run_evals.load_registry(),
    )
    assert len(refusals) == 1
    assert "parity gate scores 'kpi_headline' columns only" in refusals[0]


def test_parity_refuses_ambiguous_bare_metric() -> None:
    registry = MetricRegistry(
        [
            _open_po_entry("open_po@csv_sftp", 69),
            _open_po_entry("open_po@dynamics", 123),
        ]
    )
    refusals = run_evals.refuse_non_registry_metrics(
        {"cases": [{"id": "po", "metric": "open_po"}]},
        registry,
    )
    assert len(refusals) == 1
    assert "Refusing to guess" in refusals[0]


def test_main_refuses_non_registry_golden(tmp_path: Path) -> None:
    golden_path = tmp_path / "golden.yaml"
    golden_path.write_text(
        yaml.safe_dump(
            {
                "schema_version": 1,
                "cases": [
                    {
                        "id": "bogus",
                        "question": "What is the bogus metric?",
                        "metric": "not_a_registered_metric",
                        "unit": "ratio",
                        "expected": 1.0,
                        "tolerance": 0.5,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    # The refusal fires before the warehouse existence check: the gate refuses
    # non-registry names without needing a built mart.
    assert (
        run_evals.main(
            ["--check-parity", "--golden", str(golden_path), "--duckdb", "missing.duckdb"]
        )
        == 1
    )
