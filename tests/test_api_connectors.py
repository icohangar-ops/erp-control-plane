"""Fixture-based tests for the API connectors: Business Central + Prophet 21 + NetSuite.

All three connectors are coded against their documented API surfaces but are
UNEXERCISED against live tenants — these tests prove the documented behaviors
(paging, backoff, flattening, key inventory, anti-join reconciliation) against
httpx.MockTransport fixtures, which is the CI-required ingestion shape per the
spec. No live ERP connection is ever made.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest

from connectors.base import (
    ConnectorError,
    ConnectorNotConfigured,
    ExtractionMode,
    natural_id_for,
)
from connectors.d365_bc.connector import DynamicsBcConnector
from connectors.epicor_p21.connector import EpicorP21Connector
from connectors.netsuite.connector import NetsuiteConnector
from connectors.registry import load_source_configs
from control_plane.config import ControlPlaneConfig
from control_plane.models import SourceConfig, SyncCheckpoint
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


def _bc_connector_with_companies(tmp_path: Path, companies: str) -> DynamicsBcConnector:
    connector = _connector(
        tmp_path,
        DynamicsBcConnector,
        settings={
            "tenant_id": "11111111-1111",
            "client_id": "22222222-2222",
            "client_secret": "fixture-secret",
            "environment": "fixture",
            "companies": companies,
        },
    )
    assert isinstance(connector, DynamicsBcConnector)
    # Pre-seed the token: tests never exercise Entra ID.
    connector._cached_token = "fixture-token"
    connector._token_expiry = time.time() + 3600
    return connector


def _bc_connector(tmp_path: Path) -> DynamicsBcConnector:
    return _bc_connector_with_companies(tmp_path, "33333333-3333")


P21_SETTINGS = {
    "odata_base_url": "https://p21.fixture",
    "odata_user": "fixture-user",
    "odata_password": "fixture-password",
}


def _p21_connector(tmp_path: Path, settings: dict[str, str] | None = None) -> EpicorP21Connector:
    connector = _connector(
        tmp_path, EpicorP21Connector, settings={**P21_SETTINGS, **(settings or {})}
    )
    assert isinstance(connector, EpicorP21Connector)
    return connector


def _p21_with_seeded_token(tmp_path: Path) -> EpicorP21Connector:
    """Token pre-minted (the BC idiom): fixtures exercising paging, filtering,
    and reconciliation never need the token route."""
    connector = _p21_connector(tmp_path)
    assert isinstance(connector, EpicorP21Connector)
    connector._cached_token = "fixture-access-token"
    connector._token_expiry = time.time() + 3600
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
    # natural ids are company-prefixed (multi-company staging never collides)
    assert table.column("source_id").to_pylist() == [
        "33333333-3333:SO-0001:1",
        "33333333-3333:SO-0001:2",
    ]


def test_bc_key_inventory_and_anti_join_delete(tmp_path: Path) -> None:
    """Spec §6 cross-cutting rule, company-scoped (pitfall 3): the anti-join
    tombstones per company — deleting a vendor in company 1 must not tombstone
    company 2's vendor with the same document number."""
    connector = _bc_connector_with_companies(tmp_path, "33333333-3333,44444444-4444")
    calls: list[httpx.Request] = []
    company_1 = {
        "value": [
            {"number": "VENDOR-0001", "displayName": "Acme", "paymentTermsCode": "NET30"},
            {"number": "VENDOR-0002", "displayName": "Beta", "paymentTermsCode": "NET30"},
        ]
    }
    company_2 = {
        "value": [{"number": "VENDOR-0001", "displayName": "Acme 2", "paymentTermsCode": "NET30"}]
    }
    company_1_after_delete = {
        "value": [{"number": "VENDOR-0002", "displayName": "Beta", "paymentTermsCode": "NET30"}]
    }
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/vendors": [
                    _json_response(company_1),  # extract: company 1
                    _json_response(company_2),  # extract: company 2
                    _json_response(company_1_after_delete),  # key scan: VENDOR-0001 gone
                    _json_response(company_2),  # key scan: still present
                ]
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    connector.extract("vendors")
    result = connector.reconcile_deletes("vendors")

    assert result.tombstoned_keys == ("33333333-3333:VENDOR-0001",)
    key_scan = dict(calls[-1].url.params)
    assert key_scan["$select"] == "number"  # key-only scan


def test_bc_multi_company_loop_cross_company_watermark(tmp_path: Path) -> None:
    """Spec pitfall 3: one loop over the companies list; the checkpoint is the
    max lastModifiedDateTime observed across ALL of them — never a per-company
    max (a company with older data must not drag the checkpoint back)."""
    connector = _bc_connector_with_companies(tmp_path, "33333333-3333,44444444-4444")
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/vendors": [
                    # company 1, page 1 (continues)
                    _json_response(
                        {
                            "value": [
                                {
                                    "number": "V-1000",
                                    "displayName": "A",
                                    "paymentTermsCode": "NET30",
                                    "lastModifiedDateTime": "2026-08-02T10:00:00Z",
                                },
                                {
                                    "number": "V-1001",
                                    "displayName": "B",
                                    "paymentTermsCode": "NET30",
                                    "lastModifiedDateTime": "2026-08-01T09:30:00Z",
                                },
                            ],
                            "@odata.nextLink": (
                                "https://api.businesscentral.dynamics.com/v2.0/fixture"
                                "/api/v2.0/companies(33333333-3333)/vendors?$skip=1000"
                            ),
                        }
                    ),
                    # company 1, page 2 (final)
                    _json_response(
                        {
                            "value": [
                                {
                                    "number": "V-1002",
                                    "displayName": "C",
                                    "paymentTermsCode": "NET30",
                                    "lastModifiedDateTime": "2026-08-02T09:00:00Z",
                                },
                            ]
                        }
                    ),
                    # company 2: its own V-1000, stamped LATER than company 1's max
                    _json_response(
                        {
                            "value": [
                                {
                                    "number": "V-1000",
                                    "displayName": "A2",
                                    "paymentTermsCode": "NET30",
                                    "lastModifiedDateTime": "2026-08-03T08:00:00Z",
                                },
                            ]
                        }
                    ),
                ]
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("vendors", ExtractionMode.INCREMENTAL)

    assert result.rows_extracted == 4
    assert result.watermark_after == "2026-08-03T08:00:00Z"
    # natural ids are company-prefixed: the two companies' shared V-1000 never collide
    table = pq.read_table(result.parquet_path)
    assert sorted(table.column("source_id").to_pylist()) == [
        "33333333-3333:V-1000",
        "33333333-3333:V-1001",
        "33333333-3333:V-1002",
        "44444444-4444:V-1000",
    ]
    # the loop issued three page calls: two for company 1 (continuation), one for company 2
    assert len(calls) == 3
    assert "companies(33333333-3333)" in str(calls[0].url)
    assert "companies(33333333-3333)" in str(calls[1].url)
    assert "companies(44444444-4444)" in str(calls[2].url)


def test_bc_incremental_restart_is_a_no_op(tmp_path: Path) -> None:
    """Re-running incremental at the stored checkpoint extracts nothing: the
    cross-company watermark rides the request as a strict-gt filter, the run
    advances nothing, and the previous staging file survives untouched."""
    first = _bc_connector_with_companies(tmp_path, "33333333-3333,44444444-4444")
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/vendors": [
                    _json_response(
                        {
                            "value": [
                                # Mixed precision: .603Z is LATER in time but
                                # lexically SMALLER than .6Z — the parsed
                                # comparison must win the checkpoint (pitfall 5).
                                {
                                    "number": "V-1000",
                                    "displayName": "A",
                                    "paymentTermsCode": "NET30",
                                    "lastModifiedDateTime": "2026-08-02T10:00:00.603Z",
                                },
                                {
                                    "number": "V-1001",
                                    "displayName": "B",
                                    "paymentTermsCode": "NET30",
                                    "lastModifiedDateTime": "2026-08-02T10:00:00.6Z",
                                },
                            ]
                        }
                    ),
                    _json_response({"value": []}),  # company 2: nothing
                ]
            },
            [],
        )
    )
    first._http_client = httpx.Client(transport=transport)

    result = first.extract("vendors", ExtractionMode.INCREMENTAL)
    assert result.rows_extracted == 2
    assert result.watermark_after == "2026-08-02T10:00:00.603Z"

    # A fresh connector over the same store re-runs incremental at the checkpoint.
    second = _bc_connector_with_companies(tmp_path, "33333333-3333,44444444-4444")
    calls: list[httpx.Request] = []
    second_transport = httpx.MockTransport(
        _routing_handler(
            {"/vendors": [_json_response({"value": []}), _json_response({"value": []})]},
            calls,
        )
    )
    second._http_client = httpx.Client(transport=second_transport)

    result_2 = second.extract("vendors", ExtractionMode.INCREMENTAL)

    assert result_2.rows_extracted == 0
    assert result_2.watermark_after == "2026-08-02T10:00:00.603Z"
    assert dict(calls[0].url.params)["$filter"] == (
        "lastModifiedDateTime gt 2026-08-02T10:00:00.603Z"
    )
    # the zero-row run must not have rewritten (or emptied) the staging file
    assert len(pq.read_table(result.parquet_path).column("source_id").to_pylist()) == 2


def test_bc_quarantines_malformed_page(tmp_path: Path) -> None:
    """A structurally malformed page is quarantined with a machine-readable
    reason and fails the run — never a silent drop, never a partial promote."""
    connector = _bc_connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler({"/vendors": [_json_response({"value": "not-a-list"})]}, [])
    )
    connector._http_client = httpx.Client(transport=transport)

    with pytest.raises(ConnectorError, match="malformed"):
        connector.extract("vendors")

    records = connector.store.list_quarantine()
    assert len(records) == 1
    record = records[0]
    assert record.reason_code == "MALFORMED_PAGE"
    assert record.source_id == "d365_bc_template"
    assert json.loads(Path(record.quarantine_path).read_text(encoding="utf-8")) == {
        "value": "not-a-list"
    }
    # a failed run never advances a checkpoint
    assert connector.store.get_watermark(connector.source.source_id, "vendors", "backfill") is None


def test_bc_504_shrinks_page_and_retries(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec §5: on 504, split the request into smaller ones (halve $top), then
    back off — never raise to the caller on the first gateway timeout."""
    connector = _bc_connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler({"/vendors": [httpx.Response(504), _json_response(BC_VENDOR_PAGE)]}, calls)
    )
    connector._http_client = httpx.Client(transport=transport)
    monkeypatch.setattr(time, "sleep", lambda _s: None)

    result = connector.extract("vendors")

    assert result.rows_extracted == 2
    assert [dict(c.url.params).get("$top") for c in calls] == ["1000", "500"]


def test_bc_disabled_first_without_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Disabled-first: without credentials the adapter validates to an honest
    problem list, refuses registration and extraction before any network
    attempt, and the registry template resolves to exactly that state."""
    for var in (
        "D365BC_TENANT_ID",
        "D365BC_CLIENT_ID",
        "D365BC_CLIENT_SECRET",
        "D365BC_ENVIRONMENT",
        "D365BC_COMPANIES",
        "D365BC_API_VERSION",
    ):
        monkeypatch.delenv(var, raising=False)

    connector = _connector(tmp_path, DynamicsBcConnector)
    assert isinstance(connector, DynamicsBcConnector)
    problems = connector.validate_config()
    assert problems and "tenant_id" in problems[0] and "companies" in problems[0]

    with pytest.raises(ConnectorNotConfigured):
        connector.register()
    with pytest.raises(ConnectorNotConfigured):
        connector.extract("vendors")
    assert connector._http_client is None  # no network machinery was ever built

    # the registry template resolves all ${VAR:-} to empty and stays disabled
    source = next(s for s in load_source_configs() if s.source_id == "d365_bc_template")
    assert source.enabled is False
    assert source.settings["companies"] == ""
    template = _connector(tmp_path, DynamicsBcConnector, settings=source.settings)
    assert isinstance(template, DynamicsBcConnector)
    assert template.validate_config()  # honestly unconfigurable, exactly like the fixture


def test_bc_dry_run_without_network(tmp_path: Path) -> None:
    connector = _bc_connector(tmp_path)
    plan = connector.dry_run()
    assert plan["source_id"] == "d365_bc_template"
    assert not connector.validate_config()  # fixture settings resolve
    assert "$expand=SalesOrderLines" in plan["entities"][3]["surface"]  # header-driven entity
    assert "1 configured company" in plan["entities"][0]["surface"]
    # spec pitfall 6 rides the invoice_lines plan: the document-aggregate caveat
    assert "posted-invoice archive" in plan["entities"][4]["notes"]
    # price lists / item attributes have no v2.0 entity — custom AL page per tenant
    assert "custom AL API page" in plan["entities"][0]["notes"]
    # ship-to masters likewise (customers plan)
    assert "custom AL API page" in plan["entities"][1]["notes"]
    # historical backfill is restore-side, never an in-connector path
    assert "BACPAC" in plan["entities"][0]["notes"]


# ---------------------------------------------------------------------------
# Prophet 21: token-endpoint auth, explicit $top/$skip paging, header-driven
# lines, soft-delete filtering, reconciliation
# ---------------------------------------------------------------------------


def test_p21_token_endpoint_auth_mints_and_reuses(tmp_path: Path) -> None:
    """Spec §2.2: JSON credentials mint a bearer token at
    /api/security/token/v2 (credentials in the body, never headers); the token
    is minted once and reused across data calls — the token endpoint
    throttles re-minting."""
    connector = _p21_connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/token/v2": [_json_response({"AccessToken": "fixture-access-token"})],
                "/inv_mast": [_json_response({"value": [{"item_id": "ITEM-0001"}]})],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("items")

    assert result.rows_extracted == 1
    token_calls = [c for c in calls if c.url.path.endswith("/token/v2")]
    data_calls = [c for c in calls if not c.url.path.endswith("/token/v2")]
    assert len(token_calls) == 1  # minted once, not per data call
    assert json.loads(token_calls[0].content) == {
        "username": "fixture-user",
        "password": "fixture-password",
    }
    assert token_calls[0].headers["Content-Type"] == "application/json"
    assert [c.headers["Authorization"] for c in data_calls] == ["Bearer fixture-access-token"]


def test_p21_token_response_may_be_xml(tmp_path: Path) -> None:
    """Spec §2.2: some middleware answers XML even when JSON is requested —
    the AccessToken is parsed defensively instead of failing the run."""
    connector = _p21_connector(tmp_path)
    calls: list[httpx.Request] = []
    xml_body = (
        '<?xml version="1.0" encoding="utf-8"?>'
        "<TokenResponse><AccessToken>xml-token</AccessToken></TokenResponse>"
    )
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/token/v2": [httpx.Response(200, content=xml_body)],
                "/inv_mast": [_json_response({"value": [{"item_id": "ITEM-0001"}]})],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("items")

    assert result.rows_extracted == 1
    data_calls = [c for c in calls if not c.url.path.endswith("/token/v2")]
    assert data_calls[0].headers["Authorization"] == "Bearer xml-token"


def test_p21_reauths_once_on_mid_run_401(tmp_path: Path) -> None:
    """A ~24 h token can lapse mid-run: one 401 clears the cache, mints once,
    and retries the same request."""
    connector = _p21_connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/token/v2": [
                    _json_response({"AccessToken": "token-1"}),
                    _json_response({"AccessToken": "token-2"}),
                ],
                "/inv_mast": [
                    httpx.Response(401),
                    _json_response({"value": [{"item_id": "ITEM-0001"}]}),
                ],
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("items")

    assert result.rows_extracted == 1
    data_calls = [c for c in calls if not c.url.path.endswith("/token/v2")]
    assert [c.headers["Authorization"] for c in data_calls] == [
        "Bearer token-1",
        "Bearer token-2",
    ]


def test_p21_second_401_fails_honestly(tmp_path: Path) -> None:
    """One re-mint per request, no more: a persistent 401 is a hard error."""
    connector = _p21_connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/token/v2": [
                    _json_response({"AccessToken": "token-1"}),
                    _json_response({"AccessToken": "token-2"}),
                ],
                "/inv_mast": [httpx.Response(401), httpx.Response(401)],
            },
            [],
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    with pytest.raises(ConnectorError, match="Prophet 21 call failed"):
        connector.extract("items")


def test_p21_backoff_gives_up_after_max_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connector = _p21_with_seeded_token(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler({"/inv_mast": [httpx.Response(429) for _ in range(5)]}, [])
    )
    connector._http_client = httpx.Client(transport=transport)
    monkeypatch.setattr(time, "sleep", lambda _s: None)

    with pytest.raises(ConnectorError, match="after 5 backoff attempts"):
        connector.extract("items")


def test_p21_explicit_top_skip_paging(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    connector = _p21_with_seeded_token(tmp_path)
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
    connector = _p21_with_seeded_token(tmp_path)
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
    connector = _p21_with_seeded_token(tmp_path)
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
# NetSuite: LIMIT/OFFSET paging, watermark checkpoints, scoping, reconciliation
# ---------------------------------------------------------------------------


NS_SETTINGS = {
    "account_id": "1234567",
    "consumer_key": "fixture-consumer-key",
    "consumer_secret": "fixture-consumer-secret",
    "token_id": "fixture-token-id",
    "token_secret": "fixture-token-secret",
    "realm": "1234567",
    "subsidiaries": "",
    "accounting_book_id": "",
}


def _netsuite_connector(
    tmp_path: Path, settings: dict[str, str] | None = None
) -> NetsuiteConnector:
    connector = _connector(
        tmp_path, NetsuiteConnector, settings={**NS_SETTINGS, **(settings or {})}
    )
    assert isinstance(connector, NetsuiteConnector)
    return connector


def _suiteql_page(rows: list[dict[str, object]]) -> httpx.Response:
    return _json_response({"items": rows, "hasMore": False})


def _ns_q(request: httpx.Request) -> str:
    """The SuiteQL query text a fixture request carried (POST body ``q``)."""
    body = json.loads(request.content)
    assert isinstance(body["q"], str)
    return str(body["q"])


def _ns_row(entity: str, suffix: str) -> dict[str, object]:
    """One minimal canonical row: natural-key fields plus the audit stamp."""
    row: dict[str, object] = {
        "items": {"item_no": f"ITEM-{suffix}"},
        "customers": {"subsidiary_id": "1", "customer_no": f"CUST-{suffix}"},
        "vendors": {"subsidiary_id": "1", "vendor_no": f"V-{suffix}"},
        "sales_order_lines": {"subsidiary_id": "1", "order_no": f"SO-{suffix}", "line_no": 1},
        "invoice_lines": {"subsidiary_id": "1", "invoice_no": f"IN-{suffix}", "line_no": 1},
        "inventory_snapshots": {
            "snapshot_date": "2026-08-01",
            "branch_code": "1",
            "item_no": f"ITEM-{suffix}",
        },
        "gl_entries": {
            "subsidiary_id": "1",
            "accounting_book_id": "1",
            "journal_no": f"J-{suffix}",
            "line_no": 11,
        },
    }[entity]
    if entity != "inventory_snapshots":
        row["last_modified"] = "2026-08-02T10:00:00Z"
    return row


def test_ns_extraction_plans_cover_all_seven_entities(tmp_path: Path) -> None:
    connector = _netsuite_connector(tmp_path)
    plan = connector.dry_run()
    assert plan["source_id"] == "netsuite_template"
    assert not connector.validate_config()  # fixture credentials resolve
    entities = {entry["entity"]: entry for entry in plan["entities"]}
    assert set(entities) == {
        "items",
        "customers",
        "vendors",
        "sales_order_lines",
        "invoice_lines",
        "inventory_snapshots",
        "gl_entries",
    }
    # every plan rides the SuiteQL surface; scoping visibility per table kind
    assert all(
        entry["surface"].startswith("SuiteQL POST /services/rest/query/v1/suiteql")
        for entry in entities.values()
    )
    assert "all subsidiaries" in entities["vendors"]["surface"]
    assert "all accounting books" in entities["gl_entries"]["surface"]
    assert "scoped" not in entities["items"]["surface"]  # item is global
    # the subsidiary-scoped entities watermark on the UTC audit stamp...
    assert entities["vendors"]["incremental_key"] == "vendor.lastmodifieddate"
    assert entities["sales_order_lines"]["incremental_key"] == "t.lastmodifieddate"
    # ...while the inventory snapshot is a per-run full restage (no watermark)
    assert entities["inventory_snapshots"]["incremental_key"] is None
    # documented service limits ride every plan's notes (blueprint §4)
    for entry in entities.values():
        assert "100,000" in entry["notes"]
        assert "5/15/20" in entry["notes"]
    # per-entity caveats: item globality and multi-book GL duplication
    assert "global" in entities["items"]["notes"]
    assert "accounting book" in entities["gl_entries"]["notes"]


def test_ns_every_entity_extracts_under_fixtures(tmp_path: Path) -> None:
    """The full first-wave surface: all seven entities extract, stamp, and land
    in Parquet under MockTransport — nothing raises 'no SuiteQL query yet'."""
    connector = _netsuite_connector(tmp_path)
    for entity in connector.entities():
        calls: list[httpx.Request] = []
        transport = httpx.MockTransport(
            _routing_handler({"/suiteql": [_suiteql_page([_ns_row(entity, "0001")])]}, calls)
        )
        connector._http_client = httpx.Client(transport=transport)

        result = connector.extract(entity)

        assert result.rows_extracted == 1, entity
        table = pq.read_table(result.parquet_path)
        assert table.column("source_system").to_pylist() == ["netsuite"]
        assert table.column("source_id").to_pylist() == [
            natural_id_for(connector.natural_key_fields[entity], _ns_row(entity, "0001"))
        ]


def test_ns_limit_offset_paging(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    connector = _netsuite_connector(tmp_path)
    calls: list[httpx.Request] = []
    page_1 = [
        {"item_no": "ITEM-0001", "last_modified": "2026-08-01T10:00:00Z"},
        {"item_no": "ITEM-0002", "last_modified": "2026-08-01T09:00:00Z"},
    ]
    page_2 = [{"item_no": "ITEM-0003", "last_modified": "2026-08-01T08:00:00Z"}]
    transport = httpx.MockTransport(
        _routing_handler({"/suiteql": [_suiteql_page(page_1), _suiteql_page(page_2)]}, calls)
    )
    connector._http_client = httpx.Client(transport=transport)
    monkeypatch.setattr(NetsuiteConnector, "PAGE_SIZE", 2)

    result = connector.extract("items")

    assert result.rows_extracted == 3
    assert len(calls) == 2
    queries = [_ns_q(c) for c in calls]
    # LIMIT/OFFSET windows advance by the page size; the query is ORDERed by
    # the entity's unique key so windows are deterministic
    assert queries[0].startswith("SELECT item.itemid AS item_no")
    assert "ORDER BY item.itemid" in queries[0]
    assert queries[0].endswith("LIMIT 2 OFFSET 0")
    assert queries[1].endswith("LIMIT 2 OFFSET 2")


def test_ns_watermark_advancement_is_the_cross_page_max(tmp_path: Path) -> None:
    """The checkpoint is the max lastmodifieddate observed across ALL pages,
    compared as parsed instants — mixed-precision stamps (.6Z vs .603Z) must
    not let a lexically-larger-but-earlier stamp win (BC pitfall 5 parity)."""
    connector = _netsuite_connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/suiteql": [
                    _suiteql_page(
                        [
                            {"item_no": "ITEM-0001", "last_modified": "2026-08-02T10:00:00.6Z"},
                            {"item_no": "ITEM-0002", "last_modified": "2026-08-01T09:00:00Z"},
                        ]
                    ),
                    _suiteql_page(
                        [{"item_no": "ITEM-0003", "last_modified": "2026-08-02T10:00:00.603Z"}]
                    ),
                ]
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(NetsuiteConnector, "PAGE_SIZE", 2)
    try:
        result = connector.extract("items", ExtractionMode.INCREMENTAL)
    finally:
        monkeypatch.undo()

    assert result.rows_extracted == 3
    assert result.watermark_after == "2026-08-02T10:00:00.603Z"


def test_ns_incremental_restart_is_a_no_op(tmp_path: Path) -> None:
    """Re-running incremental at the stored checkpoint extracts nothing: the
    checkpoint rides the query as a strict-gt literal, the run advances
    nothing, and the previous staging file survives untouched."""
    first = _netsuite_connector(tmp_path)
    first_transport = httpx.MockTransport(
        _routing_handler(
            {
                "/suiteql": [
                    _suiteql_page(
                        [
                            {"item_no": "ITEM-0001", "last_modified": "2026-08-02T10:00:00.6Z"},
                            {
                                "item_no": "ITEM-0002",
                                "last_modified": "2026-08-02T10:00:00.603Z",
                            },
                        ]
                    )
                ]
            },
            [],
        )
    )
    first._http_client = httpx.Client(transport=first_transport)

    result = first.extract("items", ExtractionMode.INCREMENTAL)
    assert result.rows_extracted == 2
    assert result.watermark_after == "2026-08-02T10:00:00.603Z"

    second = _netsuite_connector(tmp_path)
    calls: list[httpx.Request] = []
    second_transport = httpx.MockTransport(
        _routing_handler({"/suiteql": [_suiteql_page([])]}, calls)
    )
    second._http_client = httpx.Client(transport=second_transport)

    result_2 = second.extract("items", ExtractionMode.INCREMENTAL)

    assert result_2.rows_extracted == 0
    assert result_2.watermark_after == "2026-08-02T10:00:00.603Z"
    assert "item.lastmodifieddate > '2026-08-02T10:00:00.603Z'" in _ns_q(calls[0])
    # the zero-row run must not have rewritten (or emptied) the staging file
    assert len(pq.read_table(result.parquet_path).column("source_id").to_pylist()) == 2


def test_ns_refuses_to_interpolate_an_unparseable_watermark(tmp_path: Path) -> None:
    """The stored checkpoint is interpolated into SuiteQL — a garbage value is
    rejected before any request is built, never filtered wrongly or injected."""
    connector = _netsuite_connector(tmp_path)
    connector.store.upsert_watermark(
        SyncCheckpoint(
            source_id=connector.source.source_id,
            entity="items",
            mode="incremental",
            watermark="2026-08-02T10:00:00Z' OR 1=1 --",
            updated_at=datetime.now(UTC),
        )
    )
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(_routing_handler({"/suiteql": []}, calls))
    connector._http_client = httpx.Client(transport=transport)

    with pytest.raises(ConnectorError, match="unparseable"):
        connector.extract("items", ExtractionMode.INCREMENTAL)

    assert calls == []  # no network attempt on a failed watermark validation


def test_ns_quarantines_malformed_page(tmp_path: Path) -> None:
    """A structurally malformed page is quarantined with a machine-readable
    reason and fails the run — never a silent drop, never a partial promote."""
    connector = _netsuite_connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler({"/suiteql": [_json_response({"items": "not-a-list"})]}, [])
    )
    connector._http_client = httpx.Client(transport=transport)

    with pytest.raises(ConnectorError, match="malformed"):
        connector.extract("items")

    records = connector.store.list_quarantine()
    assert len(records) == 1
    record = records[0]
    assert record.reason_code == "MALFORMED_PAGE"
    assert record.source_id == "netsuite_template"
    assert json.loads(Path(record.quarantine_path).read_text(encoding="utf-8")) == {
        "items": "not-a-list"
    }
    # a failed run never advances a checkpoint
    assert connector.store.get_watermark(connector.source.source_id, "items", "backfill") is None


def test_ns_quarantines_unparseable_json(tmp_path: Path) -> None:
    connector = _netsuite_connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler({"/suiteql": [httpx.Response(200, content=b"<html>gateway</html>")]}, [])
    )
    connector._http_client = httpx.Client(transport=transport)

    with pytest.raises(ConnectorError, match="not JSON"):
        connector.extract("items")

    records = connector.store.list_quarantine()
    assert len(records) == 1
    assert records[0].reason_code == "UNPARSEABLE_JSON"
    assert Path(records[0].quarantine_path).read_bytes().startswith(b"<html>")


def test_ns_key_inventory_and_anti_join_delete(tmp_path: Path) -> None:
    """Spec §6 cross-cutting rule, subsidiary-scoped: the anti-join tombstones
    per subsidiary — V-1001 deleted in subsidiary 1 must not tombstone
    subsidiary 2's V-1001, which shares the entity id."""
    connector = _netsuite_connector(tmp_path, settings={"subsidiaries": "1,2"})
    calls: list[httpx.Request] = []
    vendors = [_ns_row("vendors", "1000"), _ns_row("vendors", "1001")]
    subs1_after = []  # subsidiary 1 deleted both its vendors
    subs2_after = [{**_ns_row("vendors", "1001"), "subsidiary_id": "2"}]
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/suiteql": [
                    _suiteql_page(vendors),  # extract: both subsidiaries' vendors
                    _suiteql_page(subs1_after),  # key scan, subsidiary 1: empty now
                    _suiteql_page(subs2_after),  # key scan, subsidiary 2: V-1001 remains
                ]
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)

    connector.extract("vendors")
    result = connector.reconcile_deletes("vendors")

    assert result.tombstoned_keys == ("1:V-1000", "1:V-1001")
    key_scan_q = _ns_q(calls[-1])
    # the key scan is key-only — no full-row columns — and carries the scope
    assert "vendor.entityid AS vendor_no" in key_scan_q
    assert "vendor.companyname" not in key_scan_q
    assert "vendor.subsidiary IN ('1', '2')" in key_scan_q


def test_ns_subsidiary_scoping_predicate(tmp_path: Path) -> None:
    """The subsidiaries setting scopes queries server-side: parsed, deduplicated,
    order-preserved, quote-escaped."""
    connector = _netsuite_connector(tmp_path, settings={"subsidiaries": "2, 1, 2"})
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler({"/suiteql": [_suiteql_page([_ns_row("vendors", "1000")])]}, calls)
    )
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("vendors")

    assert result.rows_extracted == 1
    assert "vendor.subsidiary IN ('2', '1')" in _ns_q(calls[0])


def test_ns_accounting_book_scoping(tmp_path: Path) -> None:
    """Multi-book GL: the accounting_book_id setting scopes the query and the
    book id rides the natural id so per-book lines never collide."""
    connector = _netsuite_connector(tmp_path, settings={"accounting_book_id": "3"})
    calls: list[httpx.Request] = []
    row = {
        "journal_no": "J-1000",
        "line_no": 11,
        "entry_date": "2026-08-01",
        "account": "1100",
        "debit_amt": 100.0,
        "credit_amt": 0.0,
        "subsidiary_id": "1",
        "accounting_book_id": "3",
        "last_modified": "2026-08-02T10:00:00Z",
    }
    transport = httpx.MockTransport(_routing_handler({"/suiteql": [_suiteql_page([row])]}, calls))
    connector._http_client = httpx.Client(transport=transport)

    result = connector.extract("gl_entries")

    assert result.rows_extracted == 1
    assert "tal.accountingbookid = '3'" in _ns_q(calls[0])
    table = pq.read_table(result.parquet_path)
    assert table.column("source_id").to_pylist() == ["1:3:J-1000:11"]


def test_ns_backoff_honors_retry_after(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    connector = _netsuite_connector(tmp_path)
    calls: list[httpx.Request] = []
    transport = httpx.MockTransport(
        _routing_handler(
            {
                "/suiteql": [
                    httpx.Response(429, headers={"Retry-After": "0"}),
                    httpx.Response(503),  # no Retry-After -> exponential fallback
                    _suiteql_page([_ns_row("items", "0001")]),
                ]
            },
            calls,
        )
    )
    connector._http_client = httpx.Client(transport=transport)
    slept: list[float] = []
    monkeypatch.setattr(time, "sleep", slept.append)

    result = connector.extract("items")

    assert result.rows_extracted == 1
    assert len(calls) == 3
    assert 0.0 in slept  # Retry-After honored; exponential doubling follows


def test_ns_backoff_gives_up_after_max_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connector = _netsuite_connector(tmp_path)
    transport = httpx.MockTransport(
        _routing_handler({"/suiteql": [httpx.Response(429) for _ in range(6)]}, [])
    )
    connector._http_client = httpx.Client(transport=transport)
    monkeypatch.setattr(time, "sleep", lambda _s: None)

    with pytest.raises(ConnectorError, match="after 5 backoff attempts"):
        connector.extract("items")


def test_ns_disabled_first_without_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Disabled-first: without credentials the adapter validates to an honest
    problem list, refuses registration and extraction before any network
    attempt, and the registry template resolves to exactly that state."""
    for var in (
        "NETSUITE_ACCOUNT_ID",
        "NETSUITE_CONSUMER_KEY",
        "NETSUITE_CONSUMER_SECRET",
        "NETSUITE_TOKEN_ID",
        "NETSUITE_TOKEN_SECRET",
        "NETSUITE_REALM",
        "NETSUITE_SUBSIDIARIES",
        "NETSUITE_ACCOUNTING_BOOK_ID",
    ):
        monkeypatch.delenv(var, raising=False)

    connector = _connector(tmp_path, NetsuiteConnector)
    assert isinstance(connector, NetsuiteConnector)
    problems = connector.validate_config()
    assert problems and "account_id" in problems[0]

    with pytest.raises(ConnectorNotConfigured):
        connector.register()
    with pytest.raises(ConnectorNotConfigured):
        connector.extract("items")
    assert connector._http_client is None  # no network machinery was ever built

    # the registry template resolves all ${VAR:-} to empty and stays disabled
    source = next(s for s in load_source_configs() if s.source_id == "netsuite_template")
    assert source.enabled is False
    assert source.settings["subsidiaries"] == ""
    assert source.settings["accounting_book_id"] == ""
    template = _connector(tmp_path, NetsuiteConnector, settings=source.settings)
    assert isinstance(template, NetsuiteConnector)
    assert template.validate_config()  # honestly unconfigurable, exactly like the fixture


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
