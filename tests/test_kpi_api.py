"""Regression tests for the demo KPI API (api/index.py).

The expected values below were read from ``main_marts.kpi_headline`` in the
analytics DuckDB produced by ``make demo`` (dbt build, PASS=103, 0 errors).
They pin the API's mirrored SQL to the authoritative dbt pipeline output on
the static seed data, so any drift between api/index.py and dbt/models/
fails here.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from api import index as api_index

# main_marts.kpi_headline -- dbt-built ground truth for the seeded demo data.
EXPECTED_KPIS = {
    "gmroi": 1.73,
    "inventory_turns": 5.34,
    "dio_days": 68.3,
    "weeks_of_supply": 9.8,
    "gross_margin_pct": 0.2443,
    "line_fill_rate": 0.9113,
    "order_fill_rate": 0.6809,
    "otd_pct": 0.8119,
    "otif_pct": 0.5444,
    "backorder_rate": 0.2099,
    "vendor_fill_rate": 0.7453,
    "ppv_pct": 0.0124,
    "dso_days": 50.7,
    "dpo_days": 29.3,
    "ccc_days": 89.7,
    "close_cycle_days": 15.0,
    "same_branch_revenue_pct": 0.5128,
    "organic_revenue_pct": 0.8301,
    "acquired_revenue_pct": 0.1699,
    "sales_per_fte_annualized": 316718.0,
    "avg_ticket": 13306.99,
}

# seed/dealer_export/*.csv data rows (9 domains) extracted by the demo run.
EXPECTED_TOTAL_ROWS = 5004


@pytest.fixture(scope="module")
def connection():
    con = api_index.build_connection()
    yield con
    con.close()


def test_kpis_match_dbt_mart(connection):
    kpis = api_index.compute_kpis(connection)
    for key, expected in EXPECTED_KPIS.items():
        assert key in kpis, f"KPI {key} missing from API output"
        assert kpis[key] == pytest.approx(expected, abs=1e-9), (
            f"KPI {key} drifted from the dbt mart: {kpis[key]} != {expected}"
        )


def test_kpis_expose_window(connection):
    kpis = api_index.compute_kpis(connection)
    assert kpis["window_start"] == "2026-01-06"
    assert kpis["window_end"] == "2026-08-25"
    assert kpis["window_days"] == 232


def test_summary_totals(connection):
    summary = api_index.summarize_data(connection)
    assert summary["total_rows"] == EXPECTED_TOTAL_ROWS
    assert len(summary["domains"]) == 9
    assert summary["source_system"] == "csvsftp_ridgeline"
    assert summary["date_spans"]["invoices"]["start"] == "2026-01-06"
    assert summary["branches"]["total_fte"] > 0


def test_http_routes():
    client = TestClient(api_index.app)
    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"

    kpis = client.get("/kpis")
    assert kpis.status_code == 200
    assert kpis.json()["gmroi"] == pytest.approx(1.73, abs=1e-9)

    summary = client.get("/data/summary")
    assert summary.status_code == 200
    assert summary.json()["total_rows"] == EXPECTED_TOTAL_ROWS

    assert client.get("/").status_code == 200


def test_root_serves_json_to_api_clients():
    client = TestClient(api_index.app)
    response = client.get("/", headers={"Accept": "application/json"})
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    assert response.json()["endpoints"] == [
        "/health",
        "/data/summary",
        "/kpis",
        "/api/v1/genbi/answers/promote",
        "/api/v1/genbi/coverage-requests",
        "/api/v1/genbi/audit",
        "/api/v1/genbi/contracts",
        "/api/v1/genbi/approval-receipts",
        "/api/v1/genbi/data-room/search",
        "/api/v1/genbi/data-room/audit",
        "/api/v1/genbi/health/protocols",
    ]


def test_root_serves_html_landing_page_to_browsers():
    client = TestClient(api_index.app)
    response = client.get("/", headers={"Accept": "text/html"})
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "/kpis" in response.text
    assert "GMROI" in response.text
    assert "1.73" in response.text  # rendered from the mirrored mart SQL
