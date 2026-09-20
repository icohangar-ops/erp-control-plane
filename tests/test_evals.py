"""Tests for the GenBI evaluation gate (analytics/evals)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from analytics.evals import run_evals
from analytics.evals.generate_golden import QUESTION_TEMPLATES, WINDOW_COLUMNS

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
