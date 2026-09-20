"""Fixture-based tests for the Cloud ERP REST connector: Plex + Dynamics 365.

The connector is coded to the dlt rest_api verified-source approach (per-tenant
auth profiles, explicit pagination, modified-timestamp watermarks, no-delete-
feed anti-join reconciliation) but is UNEXERCISED against live tenants — these
tests prove the documented behaviors against httpx MockTransport fixtures, the
CI-required ingestion shape per the spec. No live ERP connection is ever made.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest

from connectors.base import ConnectorError, ConnectorNotConfigured, ExtractionMode
from connectors.cloud_erp_rest.connector import CloudErpRestConnector
from control_plane.config import ControlPlaneConfig
from control_plane.models import SourceConfig
from control_plane.store import SqliteControlPlaneStore


def _connector(tmp_path: Path, settings: dict[str, str] | None = None) -> CloudErpRestConnector:
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
        source_id="cloud_erp_rest_template",
        erp=CloudErpRestConnector.erp_id,
        description="fixture source",
        settings=settings or {},
        enabled=False,
    )
    return CloudErpRestConnector(source, store, config)


PLEX_SETTINGS = {
    "provider": "plex",
    "base_url": "https://plex.fixture",
    "auth_mode": "api_key",
    "api_key": "fixture-key",
}

D365_SETTINGS = {
    "provider": "d365",
    "base_url": "https://d365.fixture",
    "client_id": "fixture-id",
    "client_secret": "fixture-secret",
    "tenant_id": "11111111-1111",
}


def _plex_connector(tmp_path: Path) -> CloudErpRestConnector:
    return _connector(tmp_path, PLEX_SETTINGS)


def _d365_connector(tmp_path: Path) -> CloudErpRestConnector:
    return _connector(tmp_path, D365_SETTINGS)


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


def _json_response(payload: dict[str, object] | list[dict[str, object]]) -> httpx.Response:
    return httpx.Response(200, json=payload)


def _plex_item(item_id: str, last_modified: str) -> dict[str, object]:
    return {
        "item_id": item_id,
        "item_description": f"Item {item_id}",
        "product_class": "PC",
        "product_subclass": "PS",
        "uom": "EA",
        "avg_cost": 1.5,
        "list_price": 2.5,
        "item_status": "A",
        "last_modified": last_modified,
    }


# ---------------------------------------------------------------------------
# Plex: API-key auth, page-number paging, watermarks, reconciliation
# ---------------------------------------------------------------------------


def test_plex_page_number_paging_stops_on_short_page(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connector = _plex_connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/items": [
                    _json_response(
                        [
                            _plex_item("PLX-1", "2026-08-01T10:00:00"),
                            _plex_item("PLX-2", "2026-08-01T09:00:00"),
                        ]
                    ),
                    _json_response([_plex_item("PLX-3", "2026-08-01T08:00:00")]),
                ]
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)
    monkeypatch.setattr(CloudErpRestConnector, "PAGE_SIZE", 2)

    result = connector.extract("items")

    assert result.rows_extracted == 3
    assert len(calls) == 2  # short second page ends the scan
    assert [dict(c.url.params)["page"] for c in calls] == ["1", "2"]
    assert all(dict(c.url.params)["pageSize"] == "2" for c in calls)
    assert all(c.headers["X-API-Key"] == "fixture-key" for c in calls)


def test_plex_canonical_mapping_and_provenance(tmp_path: Path) -> None:
    connector = _plex_connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {"/items": [_json_response([_plex_item("PLX-1", "2026-08-01T10:00:00")])]}, calls
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("items")

    table = pq.read_table(result.parquet_path)
    assert table.column("item_no").to_pylist() == ["PLX-1"]
    assert table.column("description").to_pylist() == ["Item PLX-1"]
    assert table.column("unit_cost").to_pylist() == [1.5]
    assert {"source_system", "source_id", "loaded_at"}.issubset(set(table.column_names))


def test_plex_incremental_filter_carries_the_observed_watermark(tmp_path: Path) -> None:
    connector = _plex_connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/items": [
                    _json_response(
                        [
                            _plex_item("PLX-1", "2026-08-01T10:00:00"),
                            _plex_item("PLX-2", "2026-08-02T09:00:00"),
                        ]
                    ),
                    _json_response([_plex_item("PLX-3", "2026-08-03T08:00:00")]),
                ]
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    first = connector.extract("items", ExtractionMode.INCREMENTAL)
    assert first.watermark_after == "2026-08-02T09:00:00"  # observed, not wall-clock

    second = connector.extract("items", ExtractionMode.INCREMENTAL)
    assert second.rows_extracted == 1
    incremental_params = dict(calls[-1].url.params)
    assert incremental_params["modified_since"] == "2026-08-02T09:00:00"


def test_plex_bare_array_envelope_is_required(tmp_path: Path) -> None:
    connector = _plex_connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler({"/items": [_json_response({"value": []})]}, [])
    )
    connector._http_client = httpx.Client(transport=transport)

    with pytest.raises(ConnectorError, match="bare JSON arrays"):
        connector.extract("items")


def test_plex_key_inventory_and_anti_join_delete(tmp_path: Path) -> None:
    """No delete feed on the API: the full-key scan drives the anti-join."""
    connector = _plex_connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/items": [
                    _json_response(
                        [_plex_item("PLX-1", "2026-08-01"), _plex_item("PLX-2", "2026-08-01")]
                    ),
                    _json_response([_plex_item("PLX-2", "2026-08-01")]),  # one deleted
                ]
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    connector.extract("items")
    result = connector.reconcile_deletes("items")

    assert result.tombstoned_keys == ("PLX-1",)
    assert dict(calls[-1].url.params).get("$select") is None  # Plex scans full payloads


def test_plex_requires_api_key_before_any_http_call(tmp_path: Path) -> None:
    connector = _connector(tmp_path, {**PLEX_SETTINGS, "api_key": "", "auth_mode": "api_key"})
    with pytest.raises(ConnectorNotConfigured, match="api_key is unset"):
        connector.extract("items")


def test_plex_dry_run_without_network(tmp_path: Path) -> None:
    connector = _plex_connector(tmp_path)
    plan = connector.dry_run()
    assert plan["source_id"] == "cloud_erp_rest_template"
    assert not connector.validate_config()  # fixture settings resolve
    surface = plan["entities"][0]["surface"]
    assert "page_number paging (pageSize=500)" in surface
    assert "/items" in surface


# ---------------------------------------------------------------------------
# Dynamics 365: OAuth2 client credentials, @odata.nextLink, $select scans
# ---------------------------------------------------------------------------


def test_d365_oauth2_token_then_next_link_paging(tmp_path: Path) -> None:
    connector = _d365_connector(tmp_path)
    calls: list[httpx.Request] = []
    next_link = "https://d365.fixture/data/VendorsV2?$skiptoken=ABC"
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/token": [_json_response({"access_token": "fixture-token", "expires_in": 3600})],
                "/data/VendorsV2": [
                    _json_response(
                        {
                            "value": [
                                {
                                    "VendorAccountNumber": "VENDOR-0001",
                                    "OrganizationName": "Acme",
                                    "PaymentTerms": "NET30",
                                    "PurchaseLeadTime": 7,
                                    "ModifiedDateTime": "2026-08-01T10:00:00Z",
                                }
                            ],
                            "@odata.nextLink": next_link,
                        }
                    ),
                    _json_response(
                        {
                            "value": [
                                {
                                    "VendorAccountNumber": "VENDOR-0002",
                                    "OrganizationName": "Beta",
                                    "PaymentTerms": "NET30",
                                    "PurchaseLeadTime": 5,
                                    "ModifiedDateTime": "2026-08-01T09:00:00Z",
                                }
                            ]
                        }
                    ),
                ],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("vendors")

    assert result.rows_extracted == 2
    assert len(calls) == 3  # one token POST + two data pages
    token_call, first, second = calls
    assert token_call.url.path.endswith("/oauth2/v2.0/token")
    assert "grant_type=client_credentials" in token_call.content.decode()
    assert "client_id=fixture-id" in token_call.content.decode()
    assert first.headers["Authorization"] == "Bearer fixture-token"
    assert dict(first.url.params)["$top"] == "500"  # explicit page size on first hop
    assert dict(second.url.params).get("$top") is None  # continuation carries its own query
    assert second.url.params["$skiptoken"] == "ABC"  # type: ignore[index]


def test_d365_incremental_odata_filter(tmp_path: Path) -> None:
    connector = _d365_connector(tmp_path)
    # Pre-seed the token: this test asserts the incremental filter, not the token flow.
    connector._cached_token = "fixture-token"
    connector._token_expiry = time.time() + 3600
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/data/ReleasedProductsV2": [
                    _json_response(
                        {
                            "value": [
                                {
                                    "ItemNumber": "ITEM-0001",
                                    "ItemName": "Rebar 12mm",
                                    "ItemGroupId": "FG",
                                    "ItemSubGroupId": "REBAR",
                                    "UnitOfMeasureSymbol": "EA",
                                    "StandardCost": 1.25,
                                    "SalesPrice": 2.0,
                                    "Stopped": "No",
                                    "ModifiedDateTime": "2026-08-01T10:00:00Z",
                                }
                            ]
                        }
                    ),
                    _json_response(
                        {
                            "value": [
                                {
                                    "ItemNumber": "ITEM-0001",
                                    "ItemName": "Rebar 12mm",
                                    "ItemGroupId": "FG",
                                    "ItemSubGroupId": "REBAR",
                                    "UnitOfMeasureSymbol": "EA",
                                    "StandardCost": 1.25,
                                    "SalesPrice": 2.0,
                                    "Stopped": "No",
                                    "ModifiedDateTime": "2026-08-01T10:00:00Z",
                                }
                            ]
                        }
                    ),
                ]
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    first = connector.extract("items", ExtractionMode.INCREMENTAL)
    assert first.watermark_after == "2026-08-01T10:00:00Z"
    connector.extract("items", ExtractionMode.INCREMENTAL)

    assert dict(calls[-1].url.params)["$filter"] == "ModifiedDateTime gt 2026-08-01T10:00:00Z"


def test_d365_key_scan_uses_select_projection(tmp_path: Path) -> None:
    connector = _d365_connector(tmp_path)
    # Pre-seed the token: this test asserts the $select scan, not the token flow.
    connector._cached_token = "fixture-token"
    connector._token_expiry = time.time() + 3600
    calls: list[httpx.Request] = []
    product = {
        "ItemNumber": "ITEM-0001",
        "ItemName": "Rebar 12mm",
        "ItemGroupId": "FG",
        "ItemSubGroupId": "REBAR",
        "UnitOfMeasureSymbol": "EA",
        "StandardCost": 1.25,
        "SalesPrice": 2.0,
        "Stopped": "No",
        "ModifiedDateTime": "2026-08-01T10:00:00Z",
    }
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/data/ReleasedProductsV2": [
                    _json_response({"value": [product]}),
                    _json_response({"value": [product]}),
                ]
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    connector.extract("items")
    connector.reconcile_deletes("items")  # same key still present -> no tombstones

    select_param = dict(calls[-1].url.params).get("$select")
    assert select_param == "ItemNumber"


def test_d365_validate_config_requires_client_credentials(tmp_path: Path) -> None:
    incomplete = _connector(tmp_path, {"provider": "d365", "base_url": "https://d365.fixture"})
    problems = incomplete.validate_config()
    assert problems and "client_id and client_secret" in problems[0]


def test_unknown_provider_fails_loudly(tmp_path: Path) -> None:
    connector = _connector(tmp_path, {"provider": "acme", "base_url": "https://acme.fixture"})
    problems = connector.validate_config()
    assert problems and "no cloud-ERP profile" in problems[0] and "d365" in problems[0]
    with pytest.raises(ConnectorError, match="no cloud-ERP profile"):
        connector.extract("items")


def test_invalid_page_size_is_rejected(tmp_path: Path) -> None:
    connector = _connector(tmp_path, {**PLEX_SETTINGS, "page_size": "0"})
    with pytest.raises(ConnectorError, match="page_size"):
        connector.extract("items")
