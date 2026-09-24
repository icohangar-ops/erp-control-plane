"""Fixture-based tests for the Epicor Eclipse connector (session-token REST).

The connector is coded against the documented Eclipse API surface (spec
art_Hp74a48b: KB-verified endpoint paths, license gates, Search Index Builder
dependency) but is UNEXERCISED against a live tenant — these tests prove the
documented behaviors (POST /Sessions session tokens, /SessionRefresh recovery,
per-tenant-pinned query-param paging, updatedAfter watermarks, LineItems
flattening, dated inventory sweeps, quarantine fail-closed, anti-join
reconciliation, disabled-first [D]-pin validation) against httpx.MockTransport
fixtures. No live ERP connection is ever made.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest

from connectors.base import (
    ConnectorError,
    ConnectorNotConfigured,
    ConnectorNotImplemented,
    ExtractionMode,
)
from connectors.epicor_eclipse.connector import EpicorEclipseConnector
from connectors.registry import load_source_configs
from control_plane.config import ControlPlaneConfig
from control_plane.models import SourceConfig
from control_plane.store import SqliteControlPlaneStore

ECLIPSE_SETTINGS = {
    "base_url": "http://eclipse.fixture:5000",
    "username": "svc-integration",
    "password": "fixture-password",
    # [D] pins — the per-tenant wire contract from the tenant's deployed docs
    # (spec §2.1/§5); fixtures pin plausible shapes and the fail-closed
    # behavior proves what happens when a tenant disagrees.
    "session_user_field": "user",
    "session_password_field": "password",
    "token_header": "X-Session-Token",
    "watermark_field": "LastModified",
    "page_size_param": "pageSize",
    "page_number_param": "page",
    "page_number_start": "1",
    "page_size": "2",
    "go_live_date": "2026-09-01",
}

EPOCH = "1900-01-01T00:00:00"


def _connector(tmp_path: Path, settings: dict[str, str] | None = None) -> EpicorEclipseConnector:
    """One connector instance wired to a tmp control plane (no live network)."""
    config = ControlPlaneConfig(
        backend="sqlite",
        sqlite_path=tmp_path / "cp.db",
        control_plane_dsn=None,
        lake_root=tmp_path / "lake",
        analytics_duckdb_path=tmp_path / "analytics.duckdb",
        quarantine_root=tmp_path / "quarantine",
        environment="test",
    )
    store = SqliteControlPlaneStore(tmp_path / "cp.db")
    store.initialize()
    merged = {**ECLIPSE_SETTINGS, **(settings if settings is not None else {})}
    source = SourceConfig(
        source_id="epicor_eclipse_template",
        erp="epicor_eclipse",
        description="fixture source",
        settings=merged,
        enabled=False,
    )
    return EpicorEclipseConnector(source, store, config)


def _routing_handler(
    routes: dict[str, list[httpx.Response]],
    calls: list[httpx.Request],
) -> Callable[[httpx.Request], httpx.Response]:
    """Pop one canned response per request, routed by URL path suffix."""

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        for suffix, responses in routes.items():
            if request.url.path.endswith(suffix):
                if not responses:
                    raise AssertionError(f"unexpected extra call to {request.url}")
                return responses.pop(0)
        return httpx.Response(404, json={"error": f"no fixture route for {request.url}"})

    return handler


def _json_response(payload: object) -> httpx.Response:
    return httpx.Response(200, json=payload)


def _session_response(token: str = "tok-1", refresh: str | None = "refresh-1") -> httpx.Response:
    body: dict[str, object] = {"sessionToken": token}
    if refresh is not None:
        body["refreshToken"] = refresh
    return _json_response(body)


def _product_row(code: str, modified: str) -> dict[str, object]:
    return {"product_code": code, "description": f"Product {code}", "LastModified": modified}


# ---------------------------------------------------------------------------
# Session-token auth (spec §2.1)
# ---------------------------------------------------------------------------


def test_eclipse_session_token_flows_to_data_calls(tmp_path: Path) -> None:
    """POST /Sessions runs first with the pinned body keys; every data call
    then carries the sessionToken in the pinned header with the pinned paging
    params and the epoch updatedAfter on backfill."""
    connector = _connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Sessions": [_session_response()],
                "/Products": [_json_response([_product_row("PROD-001", "2026-09-01T10:00:00")])],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("items")

    assert result.rows_extracted == 1
    assert [str(c.url).split("?")[0] for c in calls] == [
        "http://eclipse.fixture:5000/Sessions",
        "http://eclipse.fixture:5000/Products",
    ]
    assert json.loads(calls[0].content) == {
        "user": "svc-integration",
        "password": "fixture-password",
    }
    assert calls[1].headers["X-Session-Token"] == "tok-1"
    assert calls[1].headers["Accept"] == "application/json"
    assert dict(calls[1].url.params) == {
        "pageSize": "2",
        "page": "1",
        "updatedAfter": EPOCH,  # backfill sends the epoch-equivalent
    }

    table = pq.read_table(result.parquet_path)
    assert table.column("product_code").to_pylist() == ["PROD-001"]
    assert table.column("source_id").to_pylist() == ["PROD-001"]


def test_eclipse_failed_session_fails_closed(tmp_path: Path) -> None:
    """A session response without a usable sessionToken never authenticates
    with an empty token (spec §2.1 [G]: the field names are per-tenant)."""
    connector = _connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Sessions": [_json_response({"sessionToken": ""})],
                "/Products": [],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    with pytest.raises(ConnectorError, match="sessionToken"):
        connector.extract("items")
    # no data call was ever made against a dead session
    assert [str(c.url) for c in calls] == ["http://eclipse.fixture:5000/Sessions"]


def test_eclipse_session_creation_http_error_fails_the_run(tmp_path: Path) -> None:
    connector = _connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler({"/Sessions": [httpx.Response(500, text="boom")]}, [])
    )
    connector._http_client = httpx.Client(transport=transport)

    with pytest.raises(ConnectorError, match="session creation failed"):
        connector.extract("items")


# ---------------------------------------------------------------------------
# Paging: pinned params, short-page termination, runaway guard (spec §5)
# ---------------------------------------------------------------------------


def test_eclipse_paging_walks_pinned_params(tmp_path: Path) -> None:
    """Query-param pages walk the pinned page-number param from the pinned
    start; a short page terminates the walk."""
    connector = _connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Sessions": [_session_response()],
                "/Products": [
                    _json_response(
                        [
                            _product_row("PROD-001", "2026-09-01T10:00:00"),
                            _product_row("PROD-002", "2026-09-01T11:00:00"),
                        ]
                    ),
                    _json_response([_product_row("PROD-003", "2026-09-01T12:00:00")]),
                ],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("items")

    assert result.rows_extracted == 3
    assert len(calls) == 3  # login + two pages
    assert [dict(c.url.params)["page"] for c in calls[1:]] == ["1", "2"]
    assert all(dict(c.url.params)["pageSize"] == "2" for c in calls[1:])

    table = pq.read_table(result.parquet_path)
    assert table.column("product_code").to_pylist() == ["PROD-001", "PROD-002", "PROD-003"]


def test_eclipse_runaway_paging_refuses_to_spin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A tenant silently ignoring the paging params would page forever — the
    runaway guard fails the run instead."""
    connector = _connector(tmp_path)
    monkeypatch.setattr(connector, "MAX_PAGES", 2)
    pages = [
        _json_response(
            [
                _product_row("PROD-001", "2026-09-01T10:00:00"),
                _product_row("PROD-002", "2026-09-01T11:00:00"),
            ]
        )
    ] * 5
    transport = httpx.MockTransport(
        _routing_handler({"/Sessions": [_session_response()], "/Products": pages}, [])
    )
    connector._http_client = httpx.Client(transport=transport)

    with pytest.raises(ConnectorError, match="refusing to spin"):
        connector.extract("items")


# ---------------------------------------------------------------------------
# Watermarks: updatedAfter (spec §4)
# ---------------------------------------------------------------------------


def test_eclipse_backfill_sends_epoch_and_advances_nothing(tmp_path: Path) -> None:
    """Backfill sends the epoch-equivalent updatedAfter; house checkpoint
    semantics (P21/BC/DMSi precedent): backfill does not advance a watermark
    — the incremental run that follows starts from the epoch and advances it."""
    connector = _connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Sessions": [_session_response()],
                "/Products": [_json_response([_product_row("PROD-001", "2026-09-01T10:00:00")])],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("items", ExtractionMode.BACKFILL)

    assert dict(calls[1].url.params)["updatedAfter"] == EPOCH
    assert result.watermark_after is None


def test_eclipse_incremental_watermark_flows_to_next_run(tmp_path: Path) -> None:
    """Incremental sends the stored stamp checkpoint; a zero-row run advances
    nothing and leaves the previous staging file untouched."""
    first = _connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Sessions": [_session_response()],
                "/Products": [
                    _json_response(
                        [
                            _product_row("PROD-001", "2026-09-01T10:00:00"),
                            # out-of-order stamps: the parsed max wins
                            _product_row("PROD-002", "2026-09-02T09:30:00"),
                        ]
                    ),
                    _json_response([]),
                ],
            },
            [],
        )
    )
    first._http_client = httpx.Client(transport=transport)

    result = first.extract("items", ExtractionMode.INCREMENTAL)
    assert result.rows_extracted == 2
    assert result.watermark_after == "2026-09-02T09:30:00"

    second = _connector(tmp_path)
    calls: list[httpx.Request] = []
    second_transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Sessions": [_session_response()],
                "/Products": [_json_response([])],
            },
            calls,
        )
    )
    second._http_client = httpx.Client(transport=second_transport)

    result_2 = second.extract("items", ExtractionMode.INCREMENTAL)

    assert result_2.rows_extracted == 0
    assert result_2.watermark_after == "2026-09-02T09:30:00"  # unchanged
    assert dict(calls[1].url.params)["updatedAfter"] == "2026-09-02T09:30:00"
    assert len(pq.read_table(result.parquet_path).column("source_id").to_pylist()) == 2


def test_eclipse_incremental_rows_without_stamps_fail_closed(tmp_path: Path) -> None:
    """Rows staged with zero watermark stamps mean the pinned watermark_field
    does not exist on the payload — refusing to persist a checkpoint beats
    silently re-pulling forever."""
    connector = _connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Sessions": [_session_response()],
                "/Products": [_json_response([{"product_code": "PROD-001"}])],
            },
            [],
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    with pytest.raises(ConnectorError, match="watermark_field"):
        connector.extract("items", ExtractionMode.INCREMENTAL)
    assert connector.store.get_watermark("epicor_eclipse_template", "items", "incremental") is None


# ---------------------------------------------------------------------------
# vendors and gl_entries extraction (the two entities this PR adds)
# ---------------------------------------------------------------------------


def test_eclipse_vendors_extraction(tmp_path: Path) -> None:
    connector = _connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Sessions": [_session_response()],
                "/Vendors": [
                    _json_response(
                        [
                            {
                                "vendor_id": "VEND-001",
                                "vendor_name": "Acme Supply",
                                "LastModified": "2026-09-01T10:00:00",
                            },
                            {
                                "vendor_id": "VEND-002",
                                "vendor_name": "Bolt & Nut Co",
                                "LastModified": "2026-09-01T11:00:00",
                            },
                        ]
                    ),
                    _json_response([]),
                ],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("vendors")

    assert result.rows_extracted == 2
    assert str(calls[1].url).startswith("http://eclipse.fixture:5000/Vendors")
    table = pq.read_table(result.parquet_path)
    assert table.column("vendor_id").to_pylist() == ["VEND-001", "VEND-002"]
    assert table.column("source_id").to_pylist() == ["VEND-001", "VEND-002"]


def test_eclipse_gl_entries_extraction(tmp_path: Path) -> None:
    """GL rides GLInquiryDetail (Accounting API license, spec §2.4/§4) with
    the same pinned paging + updatedAfter contract as every family."""
    connector = _connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Sessions": [_session_response()],
                "/GLInquiryDetail": [
                    _json_response(
                        [
                            {
                                "journal_no": "J-1",
                                "line_no": 1,
                                "gl_account": "5000-SALES",
                                "posting_date": "2026-09-05",
                                "amount": 1200.50,
                                "LastModified": "2026-09-05T10:00:00",
                            },
                            {
                                "journal_no": "J-1",
                                "line_no": 2,
                                "gl_account": "1200-AR",
                                "posting_date": "2026-09-05",
                                "amount": -1200.50,
                                "LastModified": "2026-09-05T10:00:00",
                            },
                        ]
                    ),
                    _json_response([]),
                ],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("gl_entries")

    assert result.rows_extracted == 2
    assert str(calls[1].url).startswith("http://eclipse.fixture:5000/GLInquiryDetail")
    table = pq.read_table(result.parquet_path)
    assert table.column("journal_no").to_pylist() == ["J-1", "J-1"]
    assert table.column("gl_account").to_pylist() == ["5000-SALES", "1200-AR"]
    assert table.column("source_id").to_pylist() == ["J-1:1", "J-1:2"]


# ---------------------------------------------------------------------------
# Document lines: search -> detail -> LineItems flatten (spec §3)
# ---------------------------------------------------------------------------


def _order_row(order_no: str, modified: str) -> dict[str, object]:
    return {"order_no": order_no, "LastModified": modified}


def _order_detail(order_no: str, lines: list[dict[str, object]]) -> dict[str, object]:
    return {"order_no": order_no, "LineItems": lines}


def test_eclipse_sales_order_lines_flatten_detail(tmp_path: Path) -> None:
    """Search pages + per-order detail read flattening the LineItems
    collection; header context joins in this layer, never in the API."""
    connector = _connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Sessions": [_session_response()],
                "/SalesOrders/SO-100": [
                    _json_response(
                        _order_detail(
                            "SO-100",
                            [
                                {
                                    "line_no": 1,
                                    "product_code": "PROD-001",
                                    "uom": "EA",
                                    "ordered_qty": 10,
                                    "unit_price": 10.5,
                                },
                                {
                                    "line_no": 2,
                                    "product_code": "PROD-002",
                                    "uom": "BF",
                                    "ordered_qty": 100,
                                    "unit_price": 2.25,
                                },
                            ],
                        )
                    )
                ],
                "/SalesOrders": [_json_response([_order_row("SO-100", "2026-09-05T10:00:00")])],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("sales_order_lines", ExtractionMode.BACKFILL)

    assert result.rows_extracted == 2
    assert len(calls) == 3  # login + search page + detail read
    assert str(calls[2].url) == "http://eclipse.fixture:5000/SalesOrders/SO-100"

    table = pq.read_table(result.parquet_path)
    assert table.column("order_no").to_pylist() == ["SO-100", "SO-100"]
    assert table.column("line_no").to_pylist() == [1, 2]
    assert table.column("product_code").to_pylist() == ["PROD-001", "PROD-002"]
    assert table.column("unit_price").to_pylist() == [10.5, 2.25]
    assert table.column("source_id").to_pylist() == ["SO-100:1", "SO-100:2"]


def test_eclipse_purchase_order_lines_flatten_detail(tmp_path: Path) -> None:
    connector = _connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Sessions": [_session_response()],
                "/PurchaseOrders/PO-200": [
                    _json_response(
                        {
                            "po_no": "PO-200",
                            "LineItems": [
                                {
                                    "line_no": 1,
                                    "product_code": "PROD-001",
                                    "uom": "EA",
                                    "ordered_qty": 5,
                                    "unit_cost": 4.0,
                                }
                            ],
                        }
                    )
                ],
                "/PurchaseOrders": [
                    _json_response([{"po_no": "PO-200", "LastModified": "2026-09-06T10:00:00"}])
                ],
            },
            [],
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("purchase_order_lines", ExtractionMode.BACKFILL)

    assert result.rows_extracted == 1
    table = pq.read_table(result.parquet_path)
    assert table.column("po_no").to_pylist() == ["PO-200"]
    assert table.column("unit_cost").to_pylist() == [4.0]
    assert table.column("source_id").to_pylist() == ["PO-200:1"]


def test_eclipse_detail_without_line_items_quarantines(tmp_path: Path) -> None:
    """A detail object without the LineItems collection quarantines fail-closed
    — silently staging a header with no lines would drop the document's
    children."""
    connector = _connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Sessions": [_session_response()],
                "/SalesOrders/SO-100": [_json_response({"order_no": "SO-100"})],
                "/SalesOrders": [_json_response([_order_row("SO-100", "2026-09-05T10:00:00")])],
            },
            [],
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    with pytest.raises(ConnectorError, match="LineItems"):
        connector.extract("sales_order_lines", ExtractionMode.BACKFILL)

    records = connector.store.list_quarantine()
    assert len(records) == 1
    assert records[0].reason_code == "MALFORMED_PAGE"
    assert json.loads(Path(records[0].quarantine_path).read_text(encoding="utf-8")) == {
        "order_no": "SO-100"
    }
    # a failed run never advances a checkpoint
    assert (
        connector.store.get_watermark("epicor_eclipse_template", "sales_order_lines", "backfill")
        is None
    )


# ---------------------------------------------------------------------------
# Inventory snapshots (spec §4: no watermark possible)
# ---------------------------------------------------------------------------


def test_eclipse_inventory_snapshot_stamps_date_and_sends_no_updated_after(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import datetime as dt

    monkeypatch.setattr("connectors.epicor_eclipse.connector._today", lambda: dt.date(2026, 9, 22))
    connector = _connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Sessions": [_session_response()],
                "/ProductInventoryList": [
                    _json_response(
                        [{"branch": "BR-1", "product_code": "PROD-001", "on_hand_qty": 120}]
                    )
                ],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("inventory_snapshots")

    assert result.rows_extracted == 1
    assert "updatedAfter" not in dict(calls[1].url.params)  # snapshot sweep: no watermark

    table = pq.read_table(result.parquet_path)
    assert table.column("snapshot_date").to_pylist() == ["2026-09-22"]
    assert table.column("branch").to_pylist() == ["BR-1"]
    assert table.column("on_hand_qty").to_pylist() == [120]
    assert table.column("source_id").to_pylist() == ["2026-09-22:BR-1:PROD-001"]


# ---------------------------------------------------------------------------
# Customer deleted flag (spec §7.7)
# ---------------------------------------------------------------------------


def test_eclipse_customers_drop_deleted_flagged_accounts(tmp_path: Path) -> None:
    """Only an EXPLICITLY set deleted flag drops a customer — a missing flag
    is 'flag absent on this tenant', not 'dead' (P21 polarity discipline)."""
    connector = _connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Sessions": [_session_response()],
                "/Customers": [
                    _json_response(
                        [
                            {
                                "customer_id": "CUST-001",
                                "customer_name": "Dead Account",
                                "deleted": True,
                            },
                            {
                                "customer_id": "CUST-002",
                                "customer_name": "Live Account",
                                "deleted": False,
                            },
                        ]
                    ),
                    _json_response([{"customer_id": "CUST-003", "customer_name": "No Flag"}]),
                ],
            },
            [],
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("customers")

    assert result.rows_extracted == 2
    table = pq.read_table(result.parquet_path)
    assert table.column("customer_id").to_pylist() == ["CUST-002", "CUST-003"]


# ---------------------------------------------------------------------------
# Search Index Builder condition (spec §4)
# ---------------------------------------------------------------------------


def test_eclipse_search_index_error_surfaced_distinctly(tmp_path: Path) -> None:
    """'first index the records' must be distinguishable from an empty result
    — the connector raises its own SearchIndexNotBuilt failure."""
    from connectors.epicor_eclipse.connector import SearchIndexNotBuilt

    connector = _connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Sessions": [_session_response()],
                "/Products": [
                    httpx.Response(
                        400,
                        text="To use the search, first index the records. Please run the Search Index Builder.",
                    )
                ],
            },
            [],
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    with pytest.raises(SearchIndexNotBuilt, match="Search Index Builder"):
        connector.extract("items")


# ---------------------------------------------------------------------------
# Backoff and session-rejection recovery (spec §2.1/§5)
# ---------------------------------------------------------------------------


def test_eclipse_backoff_honors_retry_after(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No rate limits are published (spec §5) — the governor is ours: 429/503
    back off honoring Retry-After, then the run succeeds."""
    connector = _connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Sessions": [_session_response()],
                "/Products": [
                    httpx.Response(429, headers={"Retry-After": "0"}),
                    httpx.Response(503),
                    _json_response([_product_row("PROD-001", "2026-09-01T10:00:00")]),
                ],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)
    slept: list[float] = []
    monkeypatch.setattr(time, "sleep", slept.append)

    result = connector.extract("items")

    assert result.rows_extracted == 1
    assert len(calls) == 4  # login + two throttled attempts + success
    assert 0.0 in slept  # Retry-After honored; exponential doubling follows


def test_eclipse_session_refresh_on_rejection(tmp_path: Path) -> None:
    """One session rejection triggers the documented /SessionRefresh — the
    same token header identifies the session — and the retried call adopts the
    refreshed token; the walk never double-stages rows."""
    connector = _connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Sessions": [_session_response()],
                "/SessionRefresh": [_json_response({"sessionToken": "tok-2"})],
                "/Products": [
                    httpx.Response(401),  # expired session mid-walk
                    _json_response([_product_row("PROD-001", "2026-09-01T10:00:00")]),
                ],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("items")

    assert result.rows_extracted == 1
    assert [str(c.url).split("?")[0] for c in calls] == [
        "http://eclipse.fixture:5000/Sessions",
        "http://eclipse.fixture:5000/Products",
        "http://eclipse.fixture:5000/SessionRefresh",
        "http://eclipse.fixture:5000/Products",
    ]
    assert calls[2].headers["X-Session-Token"] == "tok-1"  # refresh carries the current token
    assert calls[3].headers["X-Session-Token"] == "tok-2"  # retry adopts the refreshed token
    assert dict(calls[3].url.params)["page"] == "1"  # client-side page state: same page resumes


def test_eclipse_refresh_failure_falls_back_to_relogin(tmp_path: Path) -> None:
    """A failed refresh (session deleted) falls back to a full POST /Sessions
    re-login; the run recovers."""
    connector = _connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Sessions": [_session_response(), _session_response(token="tok-2", refresh=None)],
                "/SessionRefresh": [httpx.Response(500)],
                "/Products": [
                    httpx.Response(401),
                    _json_response([_product_row("PROD-001", "2026-09-01T10:00:00")]),
                ],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("items")

    assert result.rows_extracted == 1
    assert sum(1 for c in calls if c.url.path.endswith("/Sessions")) == 2  # login + re-login
    assert any(c.url.path.endswith("/SessionRefresh") for c in calls)
    assert calls[-1].headers["X-Session-Token"] == "tok-2"


def test_eclipse_second_session_rejection_fails_the_run(tmp_path: Path) -> None:
    """A second rejection after the one recovery attempt fails honestly — no
    endless refresh/re-login loop."""
    connector = _connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Sessions": [_session_response()],
                "/SessionRefresh": [_json_response({"sessionToken": "tok-2"})],
                "/Products": [httpx.Response(401), httpx.Response(401)],
            },
            [],
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    with pytest.raises(ConnectorError, match="session token"):
        connector.extract("items")


# ---------------------------------------------------------------------------
# Quarantine fail-closed (wave parity)
# ---------------------------------------------------------------------------


def test_eclipse_quarantines_malformed_page(tmp_path: Path) -> None:
    """A structurally malformed page (object where an array is expected) is
    quarantined with a machine-readable reason and fails the run."""
    connector = _connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Sessions": [_session_response()],
                "/Products": [_json_response({"Products": []})],
            },
            [],
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    with pytest.raises(ConnectorError, match="malformed"):
        connector.extract("items")

    records = connector.store.list_quarantine()
    assert len(records) == 1
    assert records[0].reason_code == "MALFORMED_PAGE"
    assert records[0].source_id == "epicor_eclipse_template"
    assert json.loads(Path(records[0].quarantine_path).read_text(encoding="utf-8")) == {
        "Products": []
    }
    assert connector.store.get_watermark("epicor_eclipse_template", "items", "backfill") is None


def test_eclipse_row_missing_natural_key_quarantines(tmp_path: Path) -> None:
    """A row without its natural identity must never stage (provenance would
    be garbage) — quarantine fail-closed."""
    connector = _connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Sessions": [_session_response()],
                "/Vendors": [_json_response([{"vendor_name": "No Id Vendor"}])],
            },
            [],
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    with pytest.raises(ConnectorError, match="natural-key"):
        connector.extract("vendors")

    records = connector.store.list_quarantine()
    assert len(records) == 1
    assert records[0].reason_code == "MALFORMED_PAGE"


# ---------------------------------------------------------------------------
# Anti-join delete reconciliation (spec §6 cross-cutting rule)
# ---------------------------------------------------------------------------


def test_eclipse_key_inventory_and_anti_join_delete(tmp_path: Path) -> None:
    """Watermarks are delete-blind, so the scheduled anti-join tombstones
    warehouse keys the source no longer returns (live-to-live: deleted-flagged
    customers drop from the key scan too)."""
    connector = _connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Sessions": [_session_response()],
                "/Customers": [
                    # extract sweep: two pages, then the key scan: two pages
                    _json_response(
                        [
                            {"customer_id": "CUST-001", "customer_name": "A", "deleted": True},
                            {"customer_id": "CUST-002", "customer_name": "B"},
                        ]
                    ),
                    _json_response([]),
                    # key scan: CUST-002 gone
                    _json_response(
                        [{"customer_id": "CUST-001", "customer_name": "A", "deleted": True}]
                    ),
                    _json_response([]),
                ],
            },
            [],
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    connector.extract("customers")
    result = connector.reconcile_deletes("customers")

    assert result.tombstoned_keys == ("CUST-002",)


# ---------------------------------------------------------------------------
# Plan-only surfaces and disabled-first behavior
# ---------------------------------------------------------------------------


def test_eclipse_invoice_lines_are_a_documented_plan(tmp_path: Path) -> None:
    """No /Invoices endpoint exists (spec §3 — verified absence): invoice
    lines surface as a documented plan — never improvised."""
    connector = _connector(tmp_path)

    plan = connector.describe_extraction("invoice_lines")
    assert "No /Invoices endpoint exists" in plan.surface
    assert "ARInquiry" in plan.surface
    assert "hybrid" in plan.surface
    assert plan.incremental_key is None

    with pytest.raises(ConnectorNotImplemented, match="documented plan"):
        list(connector._iter_records("invoice_lines", ExtractionMode.BACKFILL, None))

    with pytest.raises(ConnectorError, match="not exposed"):
        connector.extract("pricing")


def test_eclipse_disabled_first_without_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Disabled-first: without credentials AND the [D] wire-contract pins the
    adapter validates to an honest problem list, refuses registration and
    extraction before any network attempt, and the registry template resolves
    to exactly that state."""
    for var in (
        "ECLIPSE_BASE_URL",
        "ECLIPSE_USERNAME",
        "ECLIPSE_PASSWORD",
        "ECLIPSE_SESSION_USER_FIELD",
        "ECLIPSE_SESSION_PASSWORD_FIELD",
        "ECLIPSE_TOKEN_HEADER",
        "ECLIPSE_WATERMARK_FIELD",
        "ECLIPSE_PAGE_SIZE_PARAM",
        "ECLIPSE_PAGE_NUMBER_PARAM",
        "ECLIPSE_PAGE_NUMBER_START",
        "ECLIPSE_PAGE_SIZE",
        "ECLIPSE_GO_LIVE_DATE",
    ):
        monkeypatch.delenv(var, raising=False)

    # inline construction: the shared helper always merges ECLIPSE_SETTINGS,
    # and this test needs a connector with a genuinely empty settings surface
    config = ControlPlaneConfig(
        backend="sqlite",
        sqlite_path=tmp_path / "cp.db",
        control_plane_dsn=None,
        lake_root=tmp_path / "lake",
        analytics_duckdb_path=tmp_path / "analytics.duckdb",
        quarantine_root=tmp_path / "quarantine",
        environment="test",
    )
    store = SqliteControlPlaneStore(tmp_path / "cp.db")
    store.initialize()
    source = SourceConfig(
        source_id="epicor_eclipse_template",
        erp="epicor_eclipse",
        description="fixture source",
        settings={},
        enabled=False,
    )
    connector = EpicorEclipseConnector(source, store, config)
    problems = connector.validate_config()
    assert problems and "session_user_field" in problems[0] and "token_header" in problems[0]

    with pytest.raises(ConnectorNotConfigured):
        connector.register()
    with pytest.raises(ConnectorNotConfigured):
        connector.extract("items")
    assert connector._http_client is None  # no network machinery was ever built

    # the registry template resolves all ${VAR:-} to empty and stays disabled
    source = next(s for s in load_source_configs() if s.source_id == "epicor_eclipse_template")
    assert source.enabled is False


def test_eclipse_validate_config_rejects_bad_paging_and_dates(tmp_path: Path) -> None:
    connector = _connector(
        tmp_path,
        settings={"page_size": "two", "page_number_start": "-1", "go_live_date": "2027-01-01"},
    )
    problems = connector.validate_config()
    assert any("page_size" in p for p in problems)
    assert any("page_number_start" in p for p in problems)
    assert any("go_live_date" in p for p in problems)


def test_eclipse_extraction_plans_document_license_gates(tmp_path: Path) -> None:
    """The license-gated families and Search Index dependency ride the plans
    (spec §2.4/§4) — reviewable without any network."""
    connector = _connector(tmp_path)

    so_plan = connector.describe_extraction("sales_order_lines")
    assert "Sales Order API license" in so_plan.surface
    assert "LineItems" in so_plan.surface

    po_plan = connector.describe_extraction("purchase_order_lines")
    assert "Purchase Order API license" in po_plan.surface

    gl_plan = connector.describe_extraction("gl_entries")
    assert "Accounting API license" in gl_plan.surface

    inv_plan = connector.describe_extraction("inventory_snapshots")
    assert "no watermark possible" in inv_plan.incremental_key

    assert "Search Index Builder" in connector.extraction_notes
    assert "search index" in connector.describe_extraction("items").notes.lower()
