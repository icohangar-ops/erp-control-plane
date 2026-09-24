"""Honest badges: LIVE -> CACHE -> MOCK resolution on every KPI surface.

A mock number must never render as a real KPI. These tests pin the resolver's
fail-closed behavior, the /kpis badge payload, the landing-page tripwire, the
dashboard header legend, and the Superset runbook's probe-then-badge rule.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from api import index as api_index
from control_plane.badges import (
    CACHE_KPI_MARKER,
    LIVE_KPI_MARKER,
    MOCK_KPI_MARKER,
    MockKpiRenderError,
    ProvenanceBadge,
    assert_real,
    resolve_badge,
)

# --- resolution order --------------------------------------------------------


def test_live_and_cache_tiers_are_real() -> None:
    assert resolve_badge(live=True).tier == "LIVE"
    assert resolve_badge(cached=True).tier == "CACHE"
    assert resolve_badge(live=True).is_real
    assert resolve_badge(cached=True).is_real


def test_unknown_provenance_fails_closed_to_mock() -> None:
    badge = resolve_badge()
    assert badge.tier == "MOCK"
    assert not badge.is_real
    assert badge.marker == MOCK_KPI_MARKER


def test_none_evidence_is_not_real_evidence() -> None:
    assert resolve_badge(live=None, cached=None).tier == "MOCK"
    assert resolve_badge(live=False, cached=False).tier == "MOCK"


def test_mock_evidence_beats_any_real_tier_claim() -> None:
    badge = resolve_badge(live=True, mock=True)
    assert badge.tier == "MOCK"


def test_cache_fallback_carries_its_detail() -> None:
    badge = resolve_badge(cached=True, detail="serving the last real KPI set (42s old)")
    assert badge.marker == CACHE_KPI_MARKER
    assert "42s" in badge.detail


def test_unknown_tier_marker_refuses() -> None:
    badge = ProvenanceBadge(tier="TBD")
    with pytest.raises(MockKpiRenderError, match="unknown provenance tier"):
        _marker = badge.marker  # the raise IS the assertion


# --- the render tripwire ------------------------------------------------------


def test_tripwire_refuses_mock_values_in_real_kpi_slots() -> None:
    with pytest.raises(MockKpiRenderError, match="refusing to render"):
        assert_real(resolve_badge(mock=True), context="KPI tile GMROI")


def test_tripwire_passes_real_values_through() -> None:
    badge = assert_real(resolve_badge(live=True), context="KPI tile GMROI")
    assert badge.marker == LIVE_KPI_MARKER


# --- the API KPI surface ------------------------------------------------------


@pytest.fixture()
def client() -> TestClient:
    return TestClient(api_index.app)


def _reset_kpi_cache() -> None:
    api_index._kpi_cache = None
    api_index._kpi_cache_at = 0.0


def test_kpis_endpoint_carries_a_live_badge(client: TestClient) -> None:
    response = client.get("/kpis")
    assert response.status_code == 200
    badge = response.json()["provenance_badge"]
    assert badge["tier"] == "LIVE"
    assert badge["is_real"] is True
    assert badge["marker"] == LIVE_KPI_MARKER


def test_kpis_values_survive_with_the_badge_added(client: TestClient) -> None:
    body = client.get("/kpis").json()
    assert body["gmroi"] == pytest.approx(1.73, abs=1e-9)  # the dbt mart pin still holds
    assert body["window_days"] == 232


def test_kpi_cache_fallback_is_labeled_cache_not_live(monkeypatch: pytest.MonkeyPatch) -> None:
    _reset_kpi_cache()
    api_index._kpi_cache = {"gmroi": 1.73, "provenance": "seed"}
    api_index._kpi_cache_at = 0.0  # ancient cache: still a real number, labeled CACHE

    def broken() -> dict[str, Any]:
        raise RuntimeError("data layer down")

    monkeypatch.setattr(api_index, "compute_kpis", broken)
    try:
        kpis, badge = api_index._resolve_kpis()
    finally:
        _reset_kpi_cache()
    assert kpis["gmroi"] == pytest.approx(1.73, abs=1e-9)
    assert badge.tier == "CACHE"
    assert badge.is_real  # cached real KPI — honestly labeled, never live-claimed


def test_uncertified_kpis_are_never_cached_as_a_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A MOCK-tainted set must not later serve as a trusted CACHE fallback."""
    _reset_kpi_cache()
    monkeypatch.setattr(api_index, "compute_kpis", lambda: {"gmroi": None, "provenance": "seed"})
    try:
        _, badge = api_index._resolve_kpis()
        assert badge.tier == "MOCK"
        assert not badge.is_real
        assert api_index._kpi_cache is None, "uncertified values must not populate the cache"

        def broken() -> dict[str, Any]:
            raise RuntimeError("data layer down")

        monkeypatch.setattr(api_index, "compute_kpis", broken)
        with pytest.raises(RuntimeError):  # refuse — not the MOCK-tainted set
            api_index._resolve_kpis()
    finally:
        _reset_kpi_cache()


def test_data_layer_failure_with_no_cache_refuses_rather_than_mocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_kpi_cache()

    def broken() -> dict[str, Any]:
        raise RuntimeError("data layer down")

    monkeypatch.setattr(api_index, "compute_kpis", broken)
    with pytest.raises(RuntimeError):  # never fabricate a number into a KPI slot
        api_index._resolve_kpis()


def test_uncertifiable_value_downgrades_the_whole_surface_to_mock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_kpi_cache()
    monkeypatch.setattr(api_index, "compute_kpis", lambda: {"gmroi": None, "provenance": "seed"})
    try:
        _kpis, badge = api_index._resolve_kpis()
    finally:
        _reset_kpi_cache()
    assert badge.tier == "MOCK"
    assert "gmroi" in badge.detail


def test_landing_page_tiles_carry_the_badge_marker() -> None:
    html = api_index._render_landing_page()
    assert LIVE_KPI_MARKER in html
    assert "GMROI" in html


def test_landing_page_renders_mock_marker_when_the_data_layer_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken() -> dict[str, Any]:
        raise RuntimeError("data layer down")

    monkeypatch.setattr(api_index, "_resolve_kpis", broken)
    html = api_index._render_landing_page()
    assert MOCK_KPI_MARKER in html  # the outage is honest, not a silent blank or fake
    assert "KPIs unavailable" in html


def test_mock_badge_never_reaches_the_landing_tiles() -> None:
    badge = resolve_badge(mock=True)
    kpis = {
        "gmroi": 1.0,
        "inventory_turns": 2.0,
        "gross_margin_pct": 0.3,
        "line_fill_rate": 0.9,
        "vendor_fill_rate": 0.8,
    }
    with pytest.raises(MockKpiRenderError):
        api_index._kpi_tiles(kpis, badge)


# --- the Superset runbook + dashboard header ---------------------------------


def _load_runbook() -> Any:
    repo_root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        "build_dashboard_runbook", repo_root / "analytics" / "superset" / "build_dashboard.py"
    )
    assert spec and spec.loader
    runbook = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runbook)
    return runbook


def test_runbook_tiles_are_badged_live_in_the_subheader() -> None:
    runbook = _load_runbook()
    metric = runbook.simple("gmroi", "AVG", "GMROI")
    params = runbook.tile_params(7, metric, "SMART_NUMBER")
    assert params["subheader"].startswith(f"{LIVE_KPI_MARKER} dbt mart")
    assert runbook.LIVE_KPI_MARKER == LIVE_KPI_MARKER  # one source of badge truth
    assert runbook.MOCK_KPI_MARKER == MOCK_KPI_MARKER


def test_big_number_tiles_carry_both_metric_form_keys() -> None:
    """big_number_total renders from the singular 'metric' form key while the
    SPA's query builder reads the plural 'metrics' — a chart saved with only
    one of them queries fine but displays the literal string 'undefined'."""
    runbook = _load_runbook()
    metric = runbook.simple("gmroi", "AVG", "GMROI")
    params = runbook.tile_params(7, metric, ".2f")
    assert params["metric"] == metric
    assert params["metrics"] == [metric]


def _stub_guard_api(
    monkeypatch: pytest.MonkeyPatch, runbook: Any, saved_params: dict, data: Any
) -> None:
    def fake_api(method: str, path: str, **_kw: Any) -> Any:
        if method == "GET":
            return {"result": {"params": json.dumps(saved_params)}}
        return data

    monkeypatch.setattr(runbook, "api", fake_api)


def _guard(monkeypatch: pytest.MonkeyPatch, runbook: Any, saved_params: dict, data: Any) -> None:
    _stub_guard_api(monkeypatch, runbook, saved_params, data)
    ids = {
        name: i + 1
        for i, (name, viz, *_r) in enumerate(runbook.CHARTS)
        if viz == "big_number_total"
    }
    ds_map = {name: i + 100 for i, name in enumerate(runbook.VIRTUAL_DATASETS)}
    ds_map.update({k: 200 + i for i, k in enumerate(runbook.PHYSICAL_DATASETS)})
    runbook.assert_big_number_values(ids, ds_map)


def test_value_guard_fails_closed_without_the_singular_metric_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The committed bug shape: metric saved only under plural 'metrics'."""
    runbook = _load_runbook()
    metric = runbook.simple("gmroi", "AVG", "GMROI")
    saved = {"metric_missing": True, "metrics": [metric], "time_range": "No filter"}
    data = {"result": [{"status": "success", "data": [{"GMROI": 1.73}]}]}
    with pytest.raises(SystemExit, match="value-less big-number tiles"):
        _guard(monkeypatch, runbook, saved, data)


def test_value_guard_fails_closed_on_non_numeric_metric_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runbook = _load_runbook()
    metric = runbook.simple("gmroi", "AVG", "GMROI")
    saved = {"metric": metric, "metrics": [metric], "time_range": "No filter"}
    for data in (
        {"result": [{"status": "success", "data": [{"GMROI": None}]}]},
        {"result": [{"status": "success", "data": [{"GMROI": "oops"}]}]},
        {"result": [{"status": "failed", "data": []}]},
        {"result": [{"status": "success", "data": []}]},
    ):
        with pytest.raises(SystemExit, match="no finite numeric value"):
            _guard(monkeypatch, runbook, saved, data)


def test_value_guard_passes_a_finite_numeric_metric(monkeypatch: pytest.MonkeyPatch) -> None:
    runbook = _load_runbook()
    metric = runbook.simple("gmroi", "AVG", "GMROI")
    saved = {"metric": metric, "metrics": [metric], "time_range": "No filter"}
    data = {"result": [{"status": "success", "data": [{"GMROI": 1.73}]}]}
    _guard(monkeypatch, runbook, saved, data)  # must not exit


def test_value_guard_treats_nan_and_bool_as_valueless(monkeypatch: pytest.MonkeyPatch) -> None:
    runbook = _load_runbook()
    metric = runbook.simple("gmroi", "AVG", "GMROI")
    saved = {"metric": metric, "metrics": [metric], "time_range": "No filter"}
    for value in (float("nan"), float("inf"), True):
        data = {"result": [{"status": "success", "data": [{"GMROI": value}]}]}
        with pytest.raises(SystemExit, match="no finite numeric value"):
            _guard(monkeypatch, runbook, saved, data)


def test_dashboard_header_declares_the_badge_legend() -> None:
    from api.genbi.layout import HEADER_TEXT

    assert LIVE_KPI_MARKER in HEADER_TEXT
    assert CACHE_KPI_MARKER in HEADER_TEXT
    assert MOCK_KPI_MARKER in HEADER_TEXT
    assert "never presented as a real KPI" in HEADER_TEXT
