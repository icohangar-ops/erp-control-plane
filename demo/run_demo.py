#!/usr/bin/env python3
"""Run the seeded Ridgeline dealer end-to-end: extract -> dbt build -> KPI report.

This is the 10-minute-demo entry point invoked by ``make demo``. Steps:

1. Extract the seeded CSV/SFTP dealer export to Parquet through the real
   connector (idempotent — a second run extracts zero new rows).
2. Run ``dbt build`` against the DuckDB analytics target (staging -> canonical
   -> marts, including all data tests).
3. Read the headline KPI mart and print a report.

Exit code is non-zero if any step fails, so CI and Docker can rely on it.
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from connectors.base import ExtractionMode  # noqa: E402
from connectors.registry import build_connector, load_source_configs  # noqa: E402
from control_plane.config import ControlPlaneConfig  # noqa: E402
from control_plane.store import open_store  # noqa: E402

KPI_LABELS = [
    ("gmroi", "GMROI (annualized)"),
    ("inventory_turns", "Inventory turns (annualized)"),
    ("dio_days", "Days inventory outstanding"),
    ("weeks_of_supply", "Weeks of supply"),
    ("gross_margin_pct", "Gross margin"),
    ("line_fill_rate", "Line fill rate"),
    ("order_fill_rate", "Order fill rate"),
    ("otd_pct", "On-time delivery"),
    ("otif_pct", "OTIF"),
    ("backorder_rate", "Backorder rate"),
    ("vendor_fill_rate", "Vendor fill rate"),
    ("ppv_pct", "Purchase price variance"),
    ("dso_days", "DSO"),
    ("dpo_days", "DPO"),
    ("ccc_days", "Cash conversion cycle"),
    ("close_cycle_days", "Close cycle time"),
    ("same_branch_revenue_pct", "Same-branch revenue"),
    ("organic_revenue_pct", "Organic revenue"),
    ("acquired_revenue_pct", "Acquired revenue"),
    ("sales_per_fte_annualized", "Sales per FTE (annualized)"),
    ("avg_ticket", "Average ticket"),
]


def step(title: str) -> None:
    print(f"\n=== {title} " + "=" * max(0, 62 - len(title)))


def extract(config: ControlPlaneConfig) -> int:
    step("STEP 1 · connector extraction (csv_sftp -> Parquet)")
    store = open_store(config)
    sources = [s for s in load_source_configs() if s.enabled]
    if not sources:
        print("ERROR: no enabled sources in connectors/sources.yml")
        return 1
    total_rows = 0
    for source in sources:
        connector = build_connector(source, config=config, store=store)
        connector.register()
        for entity in connector.entities():
            result = connector.extract(entity, ExtractionMode.BACKFILL)
            total_rows += result.rows_extracted
            print(
                f"  {source.source_id}/{entity}: "
                f"{result.rows_extracted:>6} rows -> {result.parquet_path}"
            )
    print(f"  total newly extracted rows: {total_rows}")
    return 0


def dbt_build(config: ControlPlaneConfig) -> int:
    step("STEP 2 · dbt build (staging -> canonical -> marts + tests)")
    # dbt-duckdb creates the database file but not its parent directories.
    config.analytics_duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    completed = subprocess.run(
        [
            "dbt",
            "build",
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt",
            "--target",
            "demo",
        ],
        cwd=REPO_ROOT,
        text=True,
    )
    if completed.returncode != 0:
        print("ERROR: dbt build failed — see output above")
        return completed.returncode
    print(f"  dbt build finished in {time.monotonic() - started:.1f}s")
    return 0


def kpi_report(config: ControlPlaneConfig) -> int:
    step("STEP 3 · headline KPIs (main_marts.kpi_headline)")
    import duckdb

    con = duckdb.connect(str(config.analytics_duckdb_path), read_only=True)
    try:
        row = con.execute("SELECT * FROM main_marts.kpi_headline").fetchdf().iloc[0]
    finally:
        con.close()
    print(
        f"  window: {row['window_start']:%Y-%m-%d} -> {row['window_end']:%Y-%m-%d}"
        f" ({int(row['window_days'])} days)\n"
    )
    for column, label in KPI_LABELS:
        value = row[column]
        rendered = f"{value:,.4f}" if isinstance(value, (int, float)) else str(value)
        print(f"  {label:34s} {rendered}")
    return 0


def main() -> int:
    config = ControlPlaneConfig.from_env()
    print(
        "construction-supplies-erp-control-plane · demo run (seeded dealer: Ridgeline Lumber & Supply)"
    )
    if extract(config) != 0:
        return 1
    if dbt_build(config) != 0:
        return 2
    if kpi_report(config) != 0:
        return 3
    step("DEMO COMPLETE")
    print("data/lake/parquet   — extracted staging Parquet")
    print(str(config.analytics_duckdb_path) + " — DuckDB analytics engine")
    print("re-run any time:    make demo (idempotent)\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
