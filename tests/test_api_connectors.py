"""Fixture-based tests for the API connectors: Business Central + Prophet 21.

Both connectors are coded against their documented API surface but are
UNEXERCISED against live tenants — these tests prove the documented behaviors
(paging, backoff, flattening, key inventory, anti-join reconciliation) against
httpx MockTransport fixtures, which is the CI-required ingestion shape per the
spec. No live ERP connection is ever made.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest

from connectors.base import ExtractionMode
from connectors.d365_bc.connector import DynamicsBcConnector
from connectors.epicor_p21.connector import EpicorP21Connector
from control_plane.config import ControlPlaneConfig
from control_plane.models import SourceConfig
from control_plane.store import SqliteControlPlaneStore


def _connector(tmp_path: Path, cls: type, settings: dict[str, str] | None = None) -> object:
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
    source = SourceConfig(
        source_id=f"{cls.erp_id}_template",
        erp=cls.erp_id,
        description="fixture source",
        settings=settings or {},
        enabled=False,
    )
    return cls(source, store, config)


def _bc_connector(tmp_path: Path) -> DynamicsBcConnector:
    connector = _connector(
        tmp_path,
        DynamicsBcConnector,
        settings={
            "tenant_id": "11111111-1111",
            "client_id": "22222222-2222",
            "client_secret": "fixture-secret",
            "environment": "fixture",
            "company_id": "33333333-3333",
        },
    )
    assert isinstance(connector, DynamicsBcConnector)
    # Pre-seed the token: tests never exercise Entra ID.
    connector._cached_token = "fixture-token"
    connector._token_expiry = time.time() + 3600
    return connector


def _p21_connector(tmp_path: Path) -> EpicorP21Connector:
    connector = _connector(
        tmp_path, EpicorP21Connector, settings={"odata_base_url": "https://p21.fixture"}
    )
    assert isinstance(connector, EpicorP21Connector)
    return connector


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


def _json_response(payload: dict[str, object]) -> httpx.Response:
    return httpx.Response(200, json=payload)


BC_VENDOR_PAGE = {
    "value": [
        {"number": "VENDOR-0001", "displayName": "Acme", "paymentTermsCode": "NET30"},
        {"number": "VENDOR-0002", "displayName": "Beta", "paymentTermsCode": "NET30"},
    ]
}


# ---------------------------------------------------------------------------
# Business Central: paging, backoff, flattening, reconciliation
# ---------------------------------------------------------------------------


def test_bc_paging_follows_next_link(tmp_path: Path) -> None:
    connector = _bc_connector(tmp_path)
    calls: list[httpx.Request] = []
    # Realistic BC continuation: same entity path, server-side query attached.
    next_link = (
        "https://api.businesscentral.dynamics.com/v2.0/fixture/api/v2.0/"
        "companies(33333333-3333)/vendors?$skip=1000"
    )
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/vendors": [
                    _json_response({**BC_VENDOR_PAGE, "@odata.nextLink": next_link}),
                    _json_response(
                        {"value": [dict(BC_VENDOR_PAGE["value"][0], number="VENDOR-0003")]}
                    ),
                ]
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("vendors")

    assert result.rows_extracted == 3
    assert len(calls) == 2
    first_params = dict(calls[0].url.params)
    assert first_params["$top"] == "1000"
    assert first_params["Data-Access-Intent"] == "ReadOnly"
    assert dict(calls[1].url.params).get("$top") is None  # nextLink carries its own query


def test_bc_backoff_honors_retry_after(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    connector = _bc_connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/vendors": [
                    httpx.Response(429, headers={"Retry-After": "0"}),
                    httpx.Response(429),  # no Retry-After -> exponential fallback
                    _json_response(BC_VENDOR_PAGE),
                ]
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)
    slept: list[float] = []
    monkeypatch.setattr(time, "sleep", slept.append)

    result = connector.extract("vendors")

    assert result.rows_extracted == 2
    assert len(calls) == 3
    assert 0.0 in slept  # Retry-After honored; exponential doubling follows


def test_bc_backoff_gives_up_after_max_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from connectors.base import ConnectorError

    connector = _bc_connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler({"/vendors": [httpx.Response(429) for _ in range(6)]}, [])
    )
    connector._http_client = httpx.Client(transport=transport)
    monkeypatch.setattr(time, "sleep", lambda _s: None)

    with pytest.raises(ConnectorError, match="after 5 backoff attempts"):
        connector.extract("vendors")


def test_bc_line_flattening(tmp_path: Path) -> None:
    connector = _bc_connector(tmp_path)
    calls: list[httpx.Request] = []
    header = {
        "number": "SO-0001",
        "orderDate": "2026-08-01",
        "customerNumber": "CUSTOMER-0001",
        "lastModifiedDateTime": "2026-08-01T10:00:00Z",
        "SalesOrderLines": [
            {
                "lineNumber": 1,
                "itemNumber": "ITEM-0001",
                "unitOfMeasure": "EA",
                "quantity": 10,
                "quantityShipped": 0,
                "quantityCancelled": 0,
                "unitPrice": 10.5,
                "promisedDeliveryDate": None,
                "shipmentDate": None,
                "lineStatus": "New",
            },
            {
                "lineNumber": 2,
                "itemNumber": "ITEM-0002",
                "unitOfMeasure": "EA",
                "quantity": 4,
                "quantityShipped": 4,
                "quantityCancelled": 0,
                "unitPrice": 2.25,
                "promisedDeliveryDate": None,
                "shipmentDate": None,
                "lineStatus": "Shipped",
            },
        ],
    }
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/salesOrders": [
                    _json_response(
                        {
                            "value": [
                                header,
                                # header without line data — must not yield a row
                                {"number": "SO-0002", "SalesOrderLines": None},
                            ]
                        }
                    )
                ]
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("sales_order_lines")

    assert result.rows_extracted == 2
    table = pq.read_table(result.parquet_path)
    assert table.column("order_no").to_pylist() == ["SO-0001", "SO-0001"]
    assert table.column("line_no").to_pylist() == [1, 2]
    assert table.column("source_id").to_pylist() == ["SO-0001:1", "SO-0001:2"]


def test_bc_key_inventory_and_anti_join_delete(tmp_path: Path) -> None:
    """Spec §6 cross-cutting rule applies to API sources too: anti-join, tombstone."""
    connector = _bc_connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/vendors": [
                    _json_response(BC_VENDOR_PAGE),  # backfill: two vendors
                    _json_response({"value": [BC_VENDOR_PAGE["value"][1]]}),  # one left
                ]
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    connector.extract("vendors")
    result = connector.reconcile_deletes("vendors")

    assert result.tombstoned_keys == ("VENDOR-0001",)
    key_scan = dict(calls[-1].url.params)
    assert key_scan["$select"] == "number"  # key-only scan


def test_bc_dry_run_without_network(tmp_path: Path) -> None:
    connector = _bc_connector(tmp_path)
    plan = connector.dry_run()
    assert plan["source_id"] == "d365_bc_template"
    assert not connector.validate_config()  # fixture settings resolve
    assert "$expand=SalesOrderLines" in plan["entities"][3]["surface"]  # header-driven entity


# ---------------------------------------------------------------------------
# Prophet 21: explicit $top/$skip paging, header-driven lines, reconciliation
# ---------------------------------------------------------------------------


def test_p21_explicit_top_skip_paging(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    connector = _p21_connector(tmp_path)
    calls: list[httpx.Request] = []
    items_page_1 = {
        "value": [
            {
                "item_id": "ITEM-0001",
                "item_desc": "A",
                "product_group_id": "PG",
                "avg_cost": 1.0,
                "price_1": 2.0,
                "date_last_modified": "2026-08-01T10:00:00",
            },
            {
                "item_id": "ITEM-0002",
                "item_desc": "B",
                "product_group_id": "PG",
                "avg_cost": 1.0,
                "price_1": 2.0,
                "date_last_modified": "2026-08-01T09:00:00",
            },
        ]
    }
    items_page_2 = {
        "value": [
            {
                "item_id": "ITEM-0003",
                "item_desc": "C",
                "product_group_id": "PG",
                "avg_cost": 1.0,
                "price_1": 2.0,
                "date_last_modified": "2026-08-01T08:00:00",
            },
        ]
    }
    transport = httpx.MockTransport(
        _routing_handler(
            {"/inv_mast": [_json_response(items_page_1), _json_response(items_page_2)]}, calls
        )
    )
    connector._http_client = httpx.Client(transport=transport)
    monkeypatch.setattr(EpicorP21Connector, "PAGE_SIZE", 2)

    # Incremental run with no stored checkpoint: full history scan, pages via
    # $top/$skip, and the date_last_modified checkpoint advances from the rows.
    result = connector.extract("items", ExtractionMode.INCREMENTAL)

    assert result.rows_extracted == 3
    assert [dict(c.url.params)["$skip"] for c in calls] == ["0", "2"]
    assert all(dict(c.url.params)["$top"] == "2" for c in calls)  # $top ALWAYS set
    assert result.watermark_after == "2026-08-01T10:00:00"


def test_p21_header_driven_lines(tmp_path: Path) -> None:
    connector = _p21_connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/oe_hdr": [
                    _json_response(
                        {
                            "value": [
                                {
                                    "oe_hdr_uid": "H1",
                                    "order_no": "SO-0001",
                                    "date_last_modified": "2026-08-01T10:00:00",
                                }
                            ]
                        }
                    )
                ],
                "/oe_line": [
                    _json_response(
                        {
                            "value": [
                                {
                                    "line_no": 1,
                                    "item_id": "ITEM-0001",
                                    "qty_ordered": 10,
                                    "qty_filled": 0,
                                    "unit_price": 10.5,
                                    "oe_status": "Open",
                                },
                                {
                                    "line_no": 2,
                                    "item_id": "ITEM-0002",
                                    "qty_ordered": 4,
                                    "qty_filled": 4,
                                    "unit_price": 2.25,
                                    "oe_status": "Closed",
                                },
                            ]
                        }
                    )
                ],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("sales_order_lines")

    assert result.rows_extracted == 2
    table = pq.read_table(result.parquet_path)
    assert table.column("order_no").to_pylist() == ["SO-0001", "SO-0001"]  # header context
    assert table.column("source_id").to_pylist() == ["SO-0001:1", "SO-0001:2"]
    line_filter = json.loads(json.dumps(dict(calls[-1].url.params)))["$filter"]
    assert line_filter == "oe_hdr_uid eq 'H1'"  # explicit FK filter


def test_p21_key_inventory_and_anti_join_delete(tmp_path: Path) -> None:
    connector = _p21_connector(tmp_path)
    calls: list[httpx.Request] = []
    items = {
        "value": [
            {
                "item_id": "ITEM-0001",
                "item_desc": "A",
                "product_group_id": "PG",
                "avg_cost": 1.0,
                "price_1": 2.0,
            },
            {
                "item_id": "ITEM-0002",
                "item_desc": "B",
                "product_group_id": "PG",
                "avg_cost": 1.0,
                "price_1": 2.0,
            },
        ]
    }
    remaining = {
        "value": [
            {
                "item_id": "ITEM-0002",
                "item_desc": "B",
                "product_group_id": "PG",
                "avg_cost": 1.0,
                "price_1": 2.0,
            },
        ]
    }
    transport = httpx.MockTransport(
        _routing_handler({"/inv_mast": [_json_response(items), _json_response(remaining)]}, calls)
    )
    connector._http_client = httpx.Client(transport=transport)

    connector.extract("items")
    result = connector.reconcile_deletes("items")

    assert result.tombstoned_keys == ("ITEM-0001",)


def test_p21_dry_run_without_network(tmp_path: Path) -> None:
    connector = _p21_connector(tmp_path)
    plan = connector.dry_run()
    assert plan["source_id"] == "epicor_p21_template"
    assert not connector.validate_config()
    surfaces = [entry["surface"] for entry in plan["entities"]]
    assert any("$top=500" in s for s in surfaces)  # $top is always set


def test_p21_requires_base_url(tmp_path: Path) -> None:
    connector = _connector(tmp_path, EpicorP21Connector)
    problems = connector.validate_config()
    assert problems and "odata_base_url" in problems[0]


# ---------------------------------------------------------------------------
# Direct-connect warehouse configs (spec §5 last row)
# ---------------------------------------------------------------------------


def test_warehouse_configs_parse_and_validate() -> None:
    from connectors.warehouses import load_warehouse_configs

    configs = load_warehouse_configs()
    assert [w.warehouse_id for w in configs] == [
        "snowflake",
        "bigquery",
        "clickhouse",
        "trino",
        "databricks",
    ]
    assert all(w.read_only for w in configs)
    # Empty (unconfigured) settings: every warehouse reports its missing keys.
    problems = [w.validate() for w in configs]
    assert all(problems)
    assert any("account" in p[0] for p in problems if "snowflake" in p[0])


def test_warehouse_validate_passes_when_settings_resolve(tmp_path: Path) -> None:
    from connectors.warehouses import WarehouseConfig, load_warehouse_configs

    configured = replace(
        load_warehouse_configs()[0],
        settings={
            "account": "acme",
            "warehouse": "WH",
            "database": "DB",
            "schema": "PUBLIC",
            "user": "etl",
            "password": "pw",
        },
    )
    assert configured.validate() == []
    assert isinstance(configured, WarehouseConfig)


def test_warehouse_registry_malformed_raises(tmp_path: Path) -> None:
    from connectors.warehouses import WarehouseConfigError, load_warehouse_configs

    bad = tmp_path / "warehouses.yml"
    bad.write_text("version: 1\n", encoding="utf-8")
    with pytest.raises(WarehouseConfigError, match="warehouses"):
        load_warehouse_configs(bad)
