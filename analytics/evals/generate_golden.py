"""Regenerate the golden Q→A set from the dbt-built kpi_headline mart.

The golden set is committed (analytics/evals/golden_qa.yaml) so evaluation
truth is versioned alongside the code that produces it. Regenerate whenever
the dbt mart legitimately changes:

    make dbt-build
    python analytics/evals/generate_golden.py

Every non-window metric column in main_marts.kpi_headline MUST have a
question template below; a new mart column without a template fails the run
so golden coverage can never silently lag the warehouse.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb
import yaml

DEFAULT_DUCKDB_PATH = "./data/analytics/analytics.duckdb"
GOLDEN_PATH = Path(__file__).with_name("golden_qa.yaml")
SCHEMA_VERSION = 1
WINDOW_COLUMNS = {"window_start", "window_end", "window_days"}

# metric column -> (question, unit, tolerance). Units: ratio | percent | days | usd.
# percent-unit metrics are stored in the mart as ratios (0.2443 == 24.43%); the
# scorer normalizes generated answers to the same convention.
QUESTION_TEMPLATES: dict[str, tuple[str, str, float]] = {
    "gmroi": (
        "What is our GMROI (gross margin return on inventory investment)?",
        "ratio",
        0.005,
    ),
    "inventory_turns": ("How many times do we turn our inventory?", "ratio", 0.005),
    "dio_days": ("What is our days inventory outstanding?", "days", 0.05),
    "weeks_of_supply": ("How many weeks of supply do we carry?", "days", 0.05),
    "gross_margin_pct": ("What is our gross margin percentage?", "percent", 0.005),
    "line_fill_rate": ("What is our line fill rate?", "percent", 0.005),
    "order_fill_rate": ("What is our order fill rate?", "percent", 0.005),
    "otd_pct": ("What share of orders ship on time?", "percent", 0.005),
    "otif_pct": ("What is our OTIF (on-time in-full) rate?", "percent", 0.005),
    "backorder_rate": ("What share of lines end up backordered?", "percent", 0.005),
    "vendor_fill_rate": ("What is our vendor fill rate?", "percent", 0.005),
    "ppv_pct": ("What is our purchase price variance as a percentage?", "percent", 0.005),
    "dso_days": ("How many days of sales are outstanding (DSO)?", "days", 0.05),
    "dpo_days": ("How many days payable outstanding do we run?", "days", 0.05),
    "ccc_days": ("What is our cash conversion cycle in days?", "days", 0.05),
    "close_cycle_days": ("How many days does the monthly close take?", "days", 0.05),
    "same_branch_revenue_pct": ("What share of revenue is same-branch?", "percent", 0.005),
    "organic_revenue_pct": ("What share of revenue is organic?", "percent", 0.005),
    "acquired_revenue_pct": ("What share of revenue comes from acquisitions?", "percent", 0.005),
    "sales_per_fte_annualized": ("What is annualized sales per FTE?", "usd", 1.0),
    "avg_ticket": ("What is our average ticket?", "usd", 0.01),
}


def read_mart_row(duckdb_path: str) -> tuple[list[str], dict]:
    con = duckdb.connect(duckdb_path, read_only=True)
    try:
        cur = con.execute("select * from main_marts.kpi_headline")
        columns = [d[0] for d in cur.description]
        row = cur.fetchone()
        if row is None:
            raise SystemExit(
                "main_marts.kpi_headline is empty — build the mart first (make dbt-build)."
            )
        return columns, dict(zip(columns, row, strict=False))
    finally:
        con.close()


def build_document(duckdb_path: str) -> dict:
    columns, values = read_mart_row(duckdb_path)
    metric_columns = [c for c in columns if c not in WINDOW_COLUMNS]

    missing = sorted(set(metric_columns) - set(QUESTION_TEMPLATES))
    if missing:
        raise SystemExit(
            "kpi_headline gained metrics without golden questions: "
            + ", ".join(missing)
            + " — add QUESTION_TEMPLATES entries and regenerate."
        )

    cases = []
    for metric in metric_columns:
        question, unit, tolerance = QUESTION_TEMPLATES[metric]
        cases.append(
            {
                "id": metric,
                "question": question,
                "metric": metric,
                "unit": unit,
                "expected": float(values[metric]),
                "tolerance": tolerance,
            }
        )

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_from": "main_marts.kpi_headline",
        "window": {
            "start": str(values["window_start"]) if "window_start" in values else None,
            "end": str(values["window_end"]) if "window_end" in values else None,
            "days": int(values["window_days"]) if "window_days" in values else None,
        },
        "cases": cases,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--duckdb",
        default=os.environ.get("ANALYTICS_DUCKDB_PATH", DEFAULT_DUCKDB_PATH),
        help="Path to the dbt-built DuckDB analytics file.",
    )
    args = parser.parse_args()

    document = build_document(args.duckdb)
    GOLDEN_PATH.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    print(
        f"wrote {GOLDEN_PATH} with {len(document['cases'])} golden cases "
        f"(window {document['window']['start']} .. {document['window']['end']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
