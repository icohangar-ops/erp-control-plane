"""Fixture-based tests for the DMSi Agility connector (AgilityPublic REST).

The connector is coded against the documented AgilityPublic surface but is
UNEXERCISED against a live dealer — these tests prove the documented behaviors
(Session/Login context headers, chunk-pointer paging, FetchOnlyChangedSince
watermarks, customer-scoped orders/invoices, header/detail joining, dated
inventory snapshots, quarantine fail-closed, anti-join reconciliation) against
httpx.MockTransport fixtures. No live ERP connection is ever made.
"""

from __future__ import annotations

import datetime as dt
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
from connectors.dmsi_agility.connector import DmsiAgilityConnector
from connectors.registry import load_source_configs
from control_plane.config import ControlPlaneConfig
from control_plane.models import SourceConfig
from control_plane.store import SqliteControlPlaneStore

DMSI_SETTINGS = {
    "api_url": "https://agility.fixture",
    "login_id": "integration-user",
    "password": "fixture-password",
    "customers": "CUST-0001,CUST-0002",
    "go_live_date": "2026-09-01",
}


def _connector(tmp_path: Path, settings: dict[str, str] | None = None) -> DmsiAgilityConnector:
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
    merged = {**DMSI_SETTINGS, **(settings if settings is not None else {})}
    source = SourceConfig(
        source_id="dmsi_agility_template",
        erp="dmsi_agility",
        description="fixture source",
        settings=merged,
        enabled=False,
    )
    return DmsiAgilityConnector(source, store, config)


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
                    body = request.read().decode("utf-8", "replace")
                    raise AssertionError(
                        f"unexpected extra call to {request.url} with body {body[:300]}"
                    )
                return responses.pop(0)
        return httpx.Response(404, json={"error": f"no fixture route for {request.url}"})

    return handler


def _json_response(payload: dict[str, object]) -> httpx.Response:
    return httpx.Response(200, json=payload)


def _page(rows_key: str, rows: list[dict[str, object]], **extra: object) -> dict[str, object]:
    """A well-formed AgilityPublic chunk page: all-or-nothing envelope, rowset,
    and the chunking contract fields (spec §4/§5)."""
    return {
        "ReturnCode": 0,
        rows_key: rows,
        "MoreResultsAvailable": False,
        **extra,
    }


def _login_response() -> dict[str, object]:
    return {"SessionContextId": "ctx-1", "InitialBranch": "BR-1"}


def _customer_row(customer_id: str, changed: str) -> dict[str, object]:
    return {
        "CustomerID": customer_id,
        "CustomerName": f"Dealer Customer {customer_id}",
        "CreditLimit": 50000,
        "OpenARAmount": 1200.5,
        "OpenSOAmount": 300.0,
        "HomeBranch": "BR-1",
        "LastChanged": changed,
    }


# ---------------------------------------------------------------------------
# Session/Login and context headers
# ---------------------------------------------------------------------------


def test_dmsi_login_issues_context_headers(tmp_path: Path) -> None:
    """Session/Login runs first; every data call then carries ContextId +
    Branch headers with JSON content type (spec §2)."""
    connector = _connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Session/Login": [_json_response(_login_response())],
                "/Customer/CustomersList": [
                    _json_response(
                        _page(
                            "dtCustomersListResponse",
                            [_customer_row("CUST-0001", "2026-09-01T10:00:00")],
                        )
                    )
                ],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("customers")

    assert result.rows_extracted == 1
    assert [str(c.url) for c in calls] == [
        "https://agility.fixture/Session/Login",
        "https://agility.fixture/Customer/CustomersList",
    ]
    assert json.loads(calls[0].content) == {
        "LoginID": "integration-user",
        "Password": "fixture-password",
    }
    assert calls[1].headers["ContextId"] == "ctx-1"
    assert calls[1].headers["Branch"] == "BR-1"
    assert calls[1].headers["Content-Type"] == "application/json"

    table = pq.read_table(result.parquet_path)
    assert table.column("customer_no").to_pylist() == ["CUST-0001"]
    assert table.column("source_id").to_pylist() == ["CUST-0001"]


def test_dmsi_failed_login_fails_closed(tmp_path: Path) -> None:
    """A login response without a usable SessionContextId/InitialBranch never
    authenticates with an empty context (spec §2)."""
    connector = _connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Session/Login": [_json_response({"SessionContextId": "", "InitialBranch": ""})],
                "/Customer/CustomersList": [],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    with pytest.raises(ConnectorError, match="SessionContextId"):
        connector.extract("customers")
    # no data call was ever made against a dead context
    assert [str(c.url) for c in calls] == ["https://agility.fixture/Session/Login"]


# ---------------------------------------------------------------------------
# Chunk-pointer paging (spec §4)
# ---------------------------------------------------------------------------


def test_dmsi_chunk_pointer_paging(tmp_path: Path) -> None:
    """ChunkStartPointer walks NextChunkStartPointer until MoreResultsAvailable
    is false; RecordFetchLimit rides every request."""
    connector = _connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Session/Login": [_json_response(_login_response())],
                "/Customer/CustomersList": [
                    _json_response(
                        _page(
                            "dtCustomersListResponse",
                            [_customer_row("CUST-0001", "2026-09-01T10:00:00")],
                            MoreResultsAvailable=True,
                            NextChunkStartPointer=1,
                        )
                    ),
                    _json_response(
                        _page(
                            "dtCustomersListResponse",
                            [_customer_row("CUST-0002", "2026-09-01T11:00:00")],
                        )
                    ),
                ],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("customers")

    assert result.rows_extracted == 2
    assert len(calls) == 3
    first_body = json.loads(calls[1].content)
    second_body = json.loads(calls[2].content)
    assert first_body["ChunkStartPointer"] == 0
    assert second_body["ChunkStartPointer"] == 1
    assert first_body["RecordFetchLimit"] == 500  # Appian-tuned default (spec §5)

    table = pq.read_table(result.parquet_path)
    assert table.column("customer_no").to_pylist() == ["CUST-0001", "CUST-0002"]


def test_dmsi_non_advancing_pointer_refuses_to_spin(tmp_path: Path) -> None:
    """A NextChunkStartPointer that does not advance is a contract violation,
    not a paging loop."""
    connector = _connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Session/Login": [_json_response(_login_response())],
                "/Customer/CustomersList": [
                    _json_response(
                        _page(
                            "dtCustomersListResponse",
                            [_customer_row("CUST-0001", "2026-09-01T10:00:00")],
                            MoreResultsAvailable=True,
                            NextChunkStartPointer=0,
                        )
                    )
                ],
            },
            [],
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    with pytest.raises(ConnectorError, match="did not advance"):
        connector.extract("customers")


# ---------------------------------------------------------------------------
# Watermarks: FetchOnlyChangedSince (spec §4)
# ---------------------------------------------------------------------------


def test_dmsi_backfill_sends_epoch_changed_since(tmp_path: Path) -> None:
    """Backfill sends the spec §6 epoch-equivalent watermark to capture
    everything."""
    connector = _connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Session/Login": [_json_response(_login_response())],
                "/Customer/CustomersList": [
                    _json_response(
                        _page(
                            "dtCustomersListResponse",
                            [_customer_row("CUST-0001", "2026-09-01T10:00:00")],
                        )
                    )
                ],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("customers", ExtractionMode.BACKFILL)

    assert json.loads(calls[1].content)["FetchOnlyChangedSince"] == "1900-01-01T00:00:00"
    # house checkpoint semantics (P21/BC precedent): backfill does not advance
    # a watermark — the incremental run that follows starts from the epoch and
    # advances it (the flow test below proves that half).
    assert result.watermark_after is None


def test_dmsi_incremental_watermark_flows_to_next_run(tmp_path: Path) -> None:
    """Incremental sends the stored LastChanged checkpoint; a zero-row run
    advances nothing and leaves the previous staging file untouched."""
    first = _connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Session/Login": [_json_response(_login_response())],
                "/Customer/CustomersList": [
                    _json_response(
                        _page(
                            "dtCustomersListResponse",
                            [
                                _customer_row("CUST-0001", "2026-09-01T10:00:00"),
                                # out-of-order stamps: the parsed max wins
                                _customer_row("CUST-0002", "2026-09-02T09:30:00"),
                            ],
                        )
                    )
                ],
            },
            [],
        )
    )
    first._http_client = httpx.Client(transport=transport)

    result = first.extract("customers", ExtractionMode.INCREMENTAL)
    assert result.rows_extracted == 2
    assert result.watermark_after == "2026-09-02T09:30:00"

    second = _connector(tmp_path)
    calls: list[httpx.Request] = []
    second_transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Session/Login": [_json_response(_login_response())],
                "/Customer/CustomersList": [_json_response(_page("dtCustomersListResponse", []))],
            },
            calls,
        )
    )
    second._http_client = httpx.Client(transport=second_transport)

    result_2 = second.extract("customers", ExtractionMode.INCREMENTAL)

    assert result_2.rows_extracted == 0
    assert result_2.watermark_after == "2026-09-02T09:30:00"  # unchanged
    assert json.loads(calls[1].content)["FetchOnlyChangedSince"] == "2026-09-02T09:30:00"
    assert len(pq.read_table(result.parquet_path).column("source_id").to_pylist()) == 2


# ---------------------------------------------------------------------------
# Customer-scoped orders and invoices (spec §3 rows 7-8)
# ---------------------------------------------------------------------------


def _order_fixture(order_id: str, customer_id: str, changed: str) -> dict[str, object]:
    return {
        "OrderID": order_id,
        "OrderDate": "2026-09-05",
        "CustomerID": customer_id,
        "LastChanged": changed,
    }


def test_dmsi_orders_are_customer_scoped_and_joined(tmp_path: Path) -> None:
    """SalesOrderList runs once per configured customer (CustomerID <all> is
    never a bulk path, spec §3 row 7); dtOrder/dtOrderDetail are parallel
    top-level rowsets joined on OrderID; headers without line rows yield
    nothing."""
    connector = _connector(tmp_path)
    calls: list[httpx.Request] = []
    details = [
        {
            "OrderID": "SO-100",
            "LineNo": 1,
            "ItemCode": "ITEM-0001",
            "UOM": "EA",
            "QuantityOrdered": 10,
            "QuantityFilled": 0,
            "UnitPrice": 10.5,
            "StatusCode": "New",
        },
        {
            "OrderID": "SO-100",
            "LineNo": 2,
            "ItemCode": "ITEM-0002",
            "UOM": "BF",
            "QuantityOrdered": 100,
            "QuantityFilled": 40,
            "UnitPrice": 2.25,
            "StatusCode": "Shipped",
        },
    ]
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Session/Login": [_json_response(_login_response())],
                "/Orders/SalesOrderList": [
                    _json_response(
                        _page(
                            "dtOrder",
                            [
                                _order_fixture("SO-100", "CUST-0001", "2026-09-05T10:00:00"),
                                # header without any detail rows — must not yield a row
                                _order_fixture("SO-101", "CUST-0001", "2026-09-05T11:00:00"),
                            ],
                            dtOrderDetail=details,
                        )
                    ),
                    _json_response(_page("dtOrder", [], dtOrderDetail=[])),  # CUST-0002: nothing
                ],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("sales_order_lines", ExtractionMode.BACKFILL)

    assert result.rows_extracted == 2
    assert len(calls) == 3  # login + one per configured customer
    first_body = json.loads(calls[1].content)
    second_body = json.loads(calls[2].content)
    assert first_body["CustomerID"] == "CUST-0001"
    assert second_body["CustomerID"] == "CUST-0002"
    assert first_body["IncludeOpenOrders"] is True
    assert first_body["IncludeInvoicedOrders"] is True
    assert first_body["IncludeCanceledOrders"] is True
    assert first_body["OrderDateRangeStart"] == "2026-09-01"  # dealer go-live (spec §6)
    assert first_body["FetchOnlyChangedSince"] == "1900-01-01T00:00:00"

    table = pq.read_table(result.parquet_path)
    assert table.column("order_no").to_pylist() == ["SO-100", "SO-100"]
    assert table.column("line_no").to_pylist() == [1, 2]
    assert table.column("item_no").to_pylist() == ["ITEM-0001", "ITEM-0002"]
    assert table.column("unit_price").to_pylist() == [10.5, 2.25]
    assert table.column("source_id").to_pylist() == ["SO-100:1", "SO-100:2"]


def test_dmsi_unjoinable_detail_row_quarantines_fail_closed(tmp_path: Path) -> None:
    """A detail row whose header is absent from the same payload quarantines
    the page and fails the run — never a silent context drop."""
    connector = _connector(tmp_path)
    bad_page: dict[str, object] = {
        "ReturnCode": 0,
        "dtOrder": [
            _order_fixture("SO-100", "CUST-0001", "2026-09-05T10:00:00"),
        ],
        "dtOrderDetail": [
            # references an order absent from this payload's dtOrder
            {"OrderID": "SO-999", "LineNo": 1, "ItemCode": "ITEM-0001", "QuantityOrdered": 3}
        ],
        "MoreResultsAvailable": False,
    }
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Session/Login": [_json_response(_login_response())],
                "/Orders/SalesOrderList": [
                    _json_response(bad_page),
                    _json_response(_page("dtOrder", [])),
                ],
            },
            [],
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    with pytest.raises(ConnectorError, match="unjoinable"):
        connector.extract("sales_order_lines", ExtractionMode.BACKFILL)

    records = connector.store.list_quarantine()
    assert len(records) == 1
    assert records[0].reason_code == "MALFORMED_PAGE"
    assert json.loads(Path(records[0].quarantine_path).read_text(encoding="utf-8")) == bad_page


def test_dmsi_invoice_windows_backfill(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Invoices have no changed-since filter: backfill slides go-live-forward
    InvoiceDateRangeStart/End windows per customer (spec §4/§6)."""
    monkeypatch.setattr("connectors.dmsi_agility.connector._today", lambda: dt.date(2026, 9, 22))
    connector = _connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Session/Login": [_json_response(_login_response())],
                "/AccountsReceivable/InvoicesList": [
                    _json_response(_page("dtInvoicesListResponse", [], dtInvoiceDetailResponse=[]))
                    for _ in range(4)
                ],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("invoice_lines", ExtractionMode.BACKFILL)

    assert result.rows_extracted == 0
    assert len(calls) == 5  # login + 2 customers x 2 windows (go-live 09-01 -> 09-22)
    bodies = [json.loads(c.content) for c in calls[1:]]
    assert [b["CustomerID"] for b in bodies] == [
        "CUST-0001",
        "CUST-0001",
        "CUST-0002",
        "CUST-0002",
    ]
    assert [(b["InvoiceDateRangeStart"], b["InvoiceDateRangeEnd"]) for b in bodies] == [
        ("2026-09-01", "2026-09-14"),
        ("2026-09-15", "2026-09-22"),
        ("2026-09-01", "2026-09-14"),
        ("2026-09-15", "2026-09-22"),
    ]
    for body in bodies:
        assert body["ShiptoSequence"] == 0  # all ship-tos (spec §3 row 8)
        assert body["IncludeOnlyOpenInvoices"] is False  # closed history included
        assert "FetchOnlyChangedSince" not in body  # no changed-since exists


def test_dmsi_invoice_incremental_uses_trailing_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Incremental re-pulls the trailing invoice_window_days window (spec §4:
    re-pull trailing 7-14 days daily; close windows only after BalancesList
    reconciliation)."""
    monkeypatch.setattr("connectors.dmsi_agility.connector._today", lambda: dt.date(2026, 9, 22))
    connector = _connector(tmp_path, settings={"invoice_window_days": "7"})
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Session/Login": [_json_response(_login_response())],
                "/AccountsReceivable/InvoicesList": [
                    _json_response(_page("dtInvoicesListResponse", [], dtInvoiceDetailResponse=[]))
                    for _ in range(2)
                ],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    connector.extract("invoice_lines", ExtractionMode.INCREMENTAL)

    bodies = [json.loads(c.content) for c in calls[1:]]
    assert [(b["InvoiceDateRangeStart"], b["InvoiceDateRangeEnd"]) for b in bodies] == [
        ("2026-09-15", "2026-09-22"),
        ("2026-09-15", "2026-09-22"),
    ]


def test_dmsi_invoice_join(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """dtInvoicesListResponse/dtInvoiceDetailResponse join on InvoiceNumber."""
    monkeypatch.setattr("connectors.dmsi_agility.connector._today", lambda: dt.date(2026, 9, 22))
    connector = _connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Session/Login": [_json_response(_login_response())],
                "/AccountsReceivable/InvoicesList": [
                    _json_response(
                        _page(
                            "dtInvoicesListResponse",
                            [
                                {
                                    "InvoiceNumber": "INV-500",
                                    "InvoiceDate": "2026-09-10",
                                    "CustomerID": "CUST-0001",
                                }
                            ],
                            dtInvoiceDetailResponse=[
                                {
                                    "InvoiceNumber": "INV-500",
                                    "LineNo": 1,
                                    "ItemCode": "ITEM-0001",
                                    "UOM": "EA",
                                    "QuantityInvoiced": 4,
                                    "UnitPrice": 9.0,
                                }
                            ],
                        )
                    ),
                    # backfill walks two 14-day windows per customer (spec §4):
                    # (2026-09-01→09-14) and (2026-09-15→09-22) x CUST-0001/0002
                    _json_response(_page("dtInvoicesListResponse", [], dtInvoiceDetailResponse=[])),
                    _json_response(_page("dtInvoicesListResponse", [], dtInvoiceDetailResponse=[])),
                    _json_response(_page("dtInvoicesListResponse", [], dtInvoiceDetailResponse=[])),
                ],
            },
            [],
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("invoice_lines", ExtractionMode.BACKFILL)

    assert result.rows_extracted == 1
    table = pq.read_table(result.parquet_path)
    assert table.column("invoice_no").to_pylist() == ["INV-500"]
    assert table.column("item_no").to_pylist() == ["ITEM-0001"]
    assert table.column("invoiced_qty").to_pylist() == [4]
    assert table.column("source_id").to_pylist() == ["INV-500:1"]


# ---------------------------------------------------------------------------
# Inventory snapshots (spec §3 row 10 / §4)
# ---------------------------------------------------------------------------


def test_dmsi_inventory_snapshot_stamps_date_and_branch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Inventory is a quantity-inclusive item walk stamped as a dated snapshot
    for the logged-in branch (spec §4); the item master walk excludes prices."""
    monkeypatch.setattr("connectors.dmsi_agility.connector._today", lambda: dt.date(2026, 9, 22))
    connector = _connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Session/Login": [_json_response(_login_response())],
                "/Inventory/ItemsInChunksList": [
                    # first extract: items; second extract: inventory_snapshots
                    _json_response(
                        _page(
                            "dtItemsInChunksListResponse",
                            [
                                {
                                    "ItemCode": "ITEM-0001",
                                    "ItemDescription": "2x4x8 SPF",
                                    "ItemGroupMajor": "LUMBER",
                                    "DisplayUOM": "EA",
                                    "StockStatusCode": "STOCK",
                                    "QuantityOnHand": 120,
                                    "QuantityAvailable": 100,
                                    "QuantityOnOrder": 0,
                                    "QuantityCommitted": 20,
                                }
                            ],
                        )
                    ),
                    _json_response(
                        _page(
                            "dtItemsInChunksListResponse",
                            [
                                {
                                    "ItemCode": "ITEM-0001",
                                    "ItemDescription": "2x4x8 SPF",
                                    "ItemGroupMajor": "LUMBER",
                                    "DisplayUOM": "EA",
                                    "StockStatusCode": "STOCK",
                                    "QuantityOnHand": 118,
                                    "QuantityAvailable": 96,
                                    "QuantityOnOrder": 0,
                                    "QuantityCommitted": 22,
                                }
                            ],
                        )
                    ),
                ],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    items = connector.extract("items")
    snapshot = connector.extract("inventory_snapshots")

    items_body = json.loads(calls[1].content)
    assert items_body["IncludePriceData"] is False  # user-default-customer pricing (spec §7)
    assert "IncludeQuantityData" not in items_body

    snapshot_body = json.loads(calls[2].content)
    assert snapshot_body["IncludeQuantityData"] is True

    assert items.rows_extracted == 1
    assert pq.read_table(items.parquet_path).column("item_no").to_pylist() == ["ITEM-0001"]

    table = pq.read_table(snapshot.parquet_path)
    assert table.column("snapshot_date").to_pylist() == ["2026-09-22"]
    assert table.column("branch_code").to_pylist() == ["BR-1"]  # the session's InitialBranch
    assert table.column("on_hand_qty").to_pylist() == [118]
    assert table.column("source_id").to_pylist() == ["2026-09-22:BR-1:ITEM-0001"]


# ---------------------------------------------------------------------------
# All-or-nothing envelope, backoff, session expiry (spec §2/§5)
# ---------------------------------------------------------------------------


def test_dmsi_return_code_failure_fails_the_run(tmp_path: Path) -> None:
    """AgilityPublic processes all-or-nothing: a non-zero ReturnCode fails the
    run with MessageText — nothing is staged (spec §5)."""
    connector = _connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Session/Login": [_json_response(_login_response())],
                "/Customer/CustomersList": [
                    _json_response({"ReturnCode": 1, "MessageText": "Invalid search criteria."})
                ],
            },
            [],
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    with pytest.raises(ConnectorError, match=r"ReturnCode 1.*Invalid search criteria"):
        connector.extract("customers")


def test_dmsi_backoff_honors_retry_after(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """No rate limits are published (spec §5) — the governor is ours: 429/503
    back off honoring Retry-After, then the run succeeds."""
    connector = _connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Session/Login": [_json_response(_login_response())],
                "/Customer/CustomersList": [
                    httpx.Response(429, headers={"Retry-After": "0"}),
                    httpx.Response(503),
                    _json_response(
                        _page(
                            "dtCustomersListResponse",
                            [_customer_row("CUST-0001", "2026-09-01T10:00:00")],
                        )
                    ),
                ],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)
    slept: list[float] = []
    monkeypatch.setattr(time, "sleep", slept.append)

    result = connector.extract("customers")

    assert result.rows_extracted == 1
    assert len(calls) == 4  # login + two throttled attempts + success
    assert 0.0 in slept  # Retry-After honored; exponential doubling follows


def test_dmsi_session_expiry_relogins_and_restarts_walk(tmp_path: Path) -> None:
    """Contexts expire unused (4 h default, spec §2/§4): one ContextId
    rejection re-logins and restarts the entity's chunk walk from pointer 0;
    the restart never double-stages rows."""
    connector = _connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Session/Login": [
                    _json_response(_login_response()),
                    _json_response(_login_response()),
                ],
                "/Customer/CustomersList": [
                    httpx.Response(401),  # expired context mid-walk
                    _json_response(
                        _page(
                            "dtCustomersListResponse",
                            [
                                _customer_row("CUST-0001", "2026-09-01T10:00:00"),
                                _customer_row("CUST-0002", "2026-09-01T11:00:00"),
                            ],
                        )
                    ),
                ],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("customers")

    assert result.rows_extracted == 2  # the restarted walk never double-stages
    assert len(calls) == 4  # login, data(401), re-login, data from pointer 0
    assert json.loads(calls[3].content)["ChunkStartPointer"] == 0
    # the re-login minted a fresh context for the restart
    assert calls[3].headers["ContextId"] == "ctx-1"


def test_dmsi_second_session_rejection_fails_the_run(tmp_path: Path) -> None:
    """A second ContextId rejection after the re-login restart fails honestly —
    no endless re-login loop."""
    connector = _connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Session/Login": [
                    _json_response(_login_response()),
                    _json_response(_login_response()),
                ],
                "/Customer/CustomersList": [httpx.Response(401), httpx.Response(401)],
            },
            [],
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    with pytest.raises(ConnectorError, match="ContextId"):
        connector.extract("customers")


def test_dmsi_quarantines_malformed_page(tmp_path: Path) -> None:
    """A structurally malformed page is quarantined with a machine-readable
    reason and fails the run — never a silent drop, never a partial promote."""
    connector = _connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Session/Login": [_json_response(_login_response())],
                "/Customer/CustomersList": [
                    _json_response({"ReturnCode": 0, "MoreResultsAvailable": False})
                ],
            },
            [],
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    with pytest.raises(ConnectorError, match="malformed"):
        connector.extract("customers")

    records = connector.store.list_quarantine()
    assert len(records) == 1
    assert records[0].reason_code == "MALFORMED_PAGE"
    assert records[0].source_id == "dmsi_agility_template"
    assert json.loads(Path(records[0].quarantine_path).read_text(encoding="utf-8")) == {
        "ReturnCode": 0,
        "MoreResultsAvailable": False,
    }
    # a failed run never advances a checkpoint
    assert connector.store.get_watermark("dmsi_agility_template", "customers", "backfill") is None


# ---------------------------------------------------------------------------
# Anti-join delete reconciliation (spec §6 cross-cutting rule)
# ---------------------------------------------------------------------------


def test_dmsi_key_inventory_and_anti_join_delete(tmp_path: Path) -> None:
    """Spec §6 cross-cutting rule: watermarks are delete-blind, so the
    scheduled anti-join tombstones warehouse keys the source no longer
    returns."""
    connector = _connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/Session/Login": [_json_response(_login_response())],
                "/Customer/CustomersList": [
                    _json_response(
                        _page(
                            "dtCustomersListResponse",
                            [
                                _customer_row("CUST-0001", "2026-09-01T10:00:00"),
                                _customer_row("CUST-0002", "2026-09-01T11:00:00"),
                            ],
                        )
                    ),
                    # key scan: CUST-0001 gone
                    _json_response(
                        _page(
                            "dtCustomersListResponse",
                            [_customer_row("CUST-0002", "2026-09-01T11:00:00")],
                        )
                    ),
                ],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    connector.extract("customers")
    result = connector.reconcile_deletes("customers")

    assert result.tombstoned_keys == ("CUST-0001",)


# ---------------------------------------------------------------------------
# Plan-only surfaces and disabled-first behavior
# ---------------------------------------------------------------------------


def test_dmsi_gl_and_po_lists_are_documented_plans(tmp_path: Path) -> None:
    """GL transactions and PO lists have no AgilityPublic service (spec §3
    rows 9/11): they surface as documented plans — never improvised."""
    connector = _connector(tmp_path)

    gl_plan = connector.describe_extraction("gl_entries")
    assert "No AgilityPublic service" in gl_plan.surface
    assert "Data Warehouse" in gl_plan.surface  # hybrid vendor-mediated channel
    assert gl_plan.incremental_key is None

    po_plan = connector.describe_extraction("purchase_order_lines")
    assert "PurchaseOrderGet" in po_plan.surface
    assert "no list method exists" in po_plan.surface

    for entity in ("gl_entries", "purchase_order_lines"):
        with pytest.raises(ConnectorNotImplemented, match="documented plan"):
            list(connector._iter_records(entity, ExtractionMode.BACKFILL, None))

    with pytest.raises(ConnectorError, match="not exposed"):
        connector.extract("pricing")


def test_dmsi_disabled_first_without_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Disabled-first: without credentials the adapter validates to an honest
    problem list, refuses registration and extraction before any network
    attempt, and the registry template resolves to exactly that state."""
    for var in (
        "DMSI_API_URL",
        "DMSI_LOGIN_ID",
        "DMSI_PASSWORD",
        "DMSI_CUSTOMERS",
        "DMSI_GO_LIVE_DATE",
    ):
        monkeypatch.delenv(var, raising=False)

    # inline construction: the shared helper always merges DMSI_SETTINGS, and
    # this test needs a connector with a genuinely empty settings surface
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
        source_id="dmsi_agility_template",
        erp="dmsi_agility",
        description="fixture source",
        settings={},
        enabled=False,
    )
    connector = DmsiAgilityConnector(source, store, config)
    problems = connector.validate_config()
    assert problems and "login_id" in problems[0] and "customers" in problems[0]

    with pytest.raises(ConnectorNotConfigured):
        connector.register()
    with pytest.raises(ConnectorNotConfigured):
        connector.extract("customers")
    assert connector._http_client is None  # no network machinery was ever built

    # the registry template resolves all ${VAR:-} to empty and stays disabled
    source = next(s for s in load_source_configs() if s.source_id == "dmsi_agility_template")
    assert source.enabled is False


def test_dmsi_validate_config_rejects_http_and_empty_customers(tmp_path: Path) -> None:
    """HTTPS only (spec §5); an empty customer list fails closed — <all> is
    not a bulk path (spec §3 row 7)."""
    connector = _connector(
        tmp_path, settings={"api_url": "http://agility.fixture", "customers": " , "}
    )
    problems = connector.validate_config()
    assert any("https://" in p for p in problems)
    assert any("customers" in p for p in problems)
