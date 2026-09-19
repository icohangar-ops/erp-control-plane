"""Asset checks: lake integrity after extraction, reconciliation after dbt build."""

from __future__ import annotations

from pathlib import Path

import duckdb
from dagster import AssetCheckResult, AssetChecksDefinition, asset_check

REPO_ROOT = Path(__file__).resolve().parents[1]
ENTITIES = [
    "items",
    "salespeople",
    "customers",
    "vendors",
    "sales_order_lines",
    "purchase_order_lines",
    "invoice_lines",
    "inventory_snapshots",
    "gl_entries",
]


def _lake_checks() -> AssetChecksDefinition:
    @asset_check(asset=["csvsftp_ridgeline", "invoice_lines"], name="lake_has_all_entities")
    def _check() -> AssetCheckResult:
        missing = [
            e
            for e in ENTITIES
            if not (REPO_ROOT / "data/lake/parquet/csvsftp_ridgeline" / f"{e}.parquet").exists()
        ]
        return AssetCheckResult(
            passed=not missing,
            metadata={"missing_entities": missing},
        )

    return _check


def _reconciliation_check() -> AssetChecksDefinition:
    @asset_check(asset="fact_invoice_line", name="facts_reconcile_to_lake")
    def _check() -> AssetCheckResult:
        analytics = REPO_ROOT / "data/analytics/analytics.duckdb"
        lake = REPO_ROOT / "data/lake/parquet/csvsftp_ridgeline/invoice_lines.parquet"
        if not analytics.exists() or not lake.exists():
            return AssetCheckResult(passed=False, metadata={"reason": "build outputs missing"})
        con = duckdb.connect(str(analytics), read_only=True)
        try:
            fact_rows = con.execute(
                "SELECT count(*) FROM main_canonical.fact_invoice_line"
            ).fetchone()[0]
        finally:
            con.close()
        lake_rows = duckdb.sql(f"SELECT count(*) FROM '{lake}'").fetchone()[0]
        return AssetCheckResult(
            passed=fact_rows == lake_rows,
            metadata={"fact_rows": fact_rows, "lake_rows": lake_rows},
        )

    return _check


asset_checks: list[AssetChecksDefinition] = [_lake_checks(), _reconciliation_check()]
