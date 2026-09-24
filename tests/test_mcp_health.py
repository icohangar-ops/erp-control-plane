"""Protocol-level health probes with schema-drift alarms (wren-mcp + qdrant).

These tests drive the probe surface the way the wire does: JSON-RPC
initialize, notifications/initialized, tools/list — against a programmable
transport that can be unreachable, slow, unauthorized, lying about its
protocol, or honest-but-drifted. The reason-code classification and the
schema-drift hashing are the contract: a silent tool-surface change must
surface as a schema_drift alarm, never as a green check.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from api import index as api_index
from api.genbi import routes as genbi_routes
from api.genbi.mcp_health import (
    AUTH,
    BASELINE_MISSING,
    OK,
    PROTOCOL_ERROR,
    SCHEMA_DRIFT,
    TIMEOUT,
    UNREACHABLE,
    HttpxMcpTransport,
    ProtocolHealthProber,
    canonical_tool_schema_hash,
)

MCP_URL = "http://wren-mcp.test:8907/mcp"
QDRANT_URL = "http://qdrant.test:6333"


def initialize_result() -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "Wren Engine", "version": "0.1.0"},
        },
    }


def tools_result() -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "tools": [
                {
                    "name": "query",
                    "description": "Run a governed query",
                    "inputSchema": {"type": "object", "properties": {"sql": {"type": "string"}}},
                },
                {
                    "name": "deploy",
                    "description": "Deploy an MDL",
                    "inputSchema": {"type": "object", "properties": {"mdl": {"type": "object"}}},
                },
            ]
        },
    }


class FakeMcpTransport:
    """Scripted MCP server: method -> (status, body), or raise a fault."""

    def __init__(self) -> None:
        self.script: dict[str, Any] = {
            "initialize": (200, initialize_result()),
            "notifications/initialized": (200, None),
            "tools/list": (200, tools_result()),
        }
        self.calls: list[str] = []

    def post_json(self, url: str, body: dict[str, Any], timeout: float) -> tuple[int, Any]:
        method = body["method"]
        self.calls.append(method)
        status, payload = self.script[method]
        if isinstance(payload, Exception):
            raise payload
        return status, payload

    def rewrite(self, method: str, status: int, body: Any) -> None:
        self.script[method] = (status, body)


class FakeQdrantTransport:
    def __init__(self, status: int = 200, body: Any = None) -> None:
        self.status = status
        self.body = (
            body
            if body is not None
            else {"result": {"collections": [{"name": "acquisition_data_rooms"}]}}
        )

    def get_json(self, url: str, timeout: float) -> tuple[int, Any]:
        return self.status, self.body


class FaultyQdrantTransport:
    """Raises the given httpx fault on every request."""

    def __init__(self, fault: Exception) -> None:
        self.fault = fault

    def get_json(self, url: str, timeout: float) -> tuple[int, Any]:
        raise self.fault


def make_prober(
    tmp_path: Path,
    mcp: FakeMcpTransport | None = None,
    qdrant: Any = None,
) -> ProtocolHealthProber:
    return ProtocolHealthProber(
        mcp_url=MCP_URL,
        qdrant_url=QDRANT_URL,
        baseline_path=tmp_path / "mcp_health_baseline.json",
        mcp_transport=mcp or FakeMcpTransport(),
        qdrant_transport=qdrant or FakeQdrantTransport(),
        timeout=1.0,
    )


# --- the MCP handshake -----------------------------------------------------------


def test_handshake_without_baseline_reports_baseline_missing(tmp_path: Path) -> None:
    result = make_prober(tmp_path).probe_wren_mcp()
    assert result.reason_code == BASELINE_MISSING
    assert result.tool_names == ["query", "deploy"]
    assert result.schema_hash is not None


def test_handshake_performs_initialize_then_tools_list(tmp_path: Path) -> None:
    mcp = FakeMcpTransport()
    make_prober(tmp_path, mcp).probe_wren_mcp()
    assert mcp.calls[0] == "initialize"
    assert "tools/list" in mcp.calls


def test_unreachable_when_connect_fails(tmp_path: Path) -> None:
    mcp = FakeMcpTransport()
    mcp.rewrite("initialize", 0, httpx.ConnectError("refused"))
    result = make_prober(tmp_path, mcp).probe_wren_mcp()
    assert result.reason_code == UNREACHABLE


def test_timeout_is_its_own_reason_code(tmp_path: Path) -> None:
    mcp = FakeMcpTransport()
    mcp.rewrite("initialize", 0, httpx.ReadTimeout("too slow"))
    result = make_prober(tmp_path, mcp).probe_wren_mcp()
    assert result.reason_code == TIMEOUT


def test_auth_rejection_is_its_own_reason_code(tmp_path: Path) -> None:
    for status in (401, 403):
        mcp = FakeMcpTransport()
        mcp.rewrite("initialize", status, {"detail": "nope"})
        result = make_prober(tmp_path, mcp).probe_wren_mcp()
        assert result.reason_code == AUTH, status


def test_server_error_is_protocol_error(tmp_path: Path) -> None:
    mcp = FakeMcpTransport()
    mcp.rewrite("initialize", 500, "oops")
    result = make_prober(tmp_path, mcp).probe_wren_mcp()
    assert result.reason_code == PROTOCOL_ERROR


@pytest.mark.parametrize(
    ("broken_result", "why"),
    [
        (
            {"jsonrpc": "2.0", "id": 1, "error": {"code": -32601, "message": "nope"}},
            "rpc error",
        ),
        ("not json-rpc at all", "non-JSON-RPC body"),
        (
            {"jsonrpc": "2.0", "id": 1, "result": {"protocolVersion": "2024-11-05"}},
            "no serverInfo",
        ),
    ],
)
def test_broken_initialize_is_protocol_error(tmp_path: Path, broken_result: Any, why: str) -> None:
    mcp = FakeMcpTransport()
    mcp.rewrite("initialize", 200, broken_result)
    result = make_prober(tmp_path, mcp).probe_wren_mcp()
    assert result.reason_code == PROTOCOL_ERROR, why


def test_malformed_tools_array_is_protocol_error(tmp_path: Path) -> None:
    mcp = FakeMcpTransport()
    mcp.rewrite("tools/list", 200, {"jsonrpc": "2.0", "id": 1, "result": {"tools": "all of them"}})
    result = make_prober(tmp_path, mcp).probe_wren_mcp()
    assert result.reason_code == PROTOCOL_ERROR


# --- schema-drift hashing -----------------------------------------------------------


def test_schema_hash_is_key_order_insensitive_and_drifts_on_content() -> None:
    tools = tools_result()["result"]["tools"]
    reordered = [{k: v for k, v in sorted(tool.items(), reverse=True)} for tool in tools]
    assert canonical_tool_schema_hash(tools) == canonical_tool_schema_hash(reordered)
    renamed = [{**tools[0], "name": "query_renamed"}, *tools[1:]]
    assert canonical_tool_schema_hash(tools) != canonical_tool_schema_hash(renamed)
    redescribed = [{**tools[0], "description": "Now with different wording"}, *tools[1:]]
    assert canonical_tool_schema_hash(tools) != canonical_tool_schema_hash(redescribed)


def test_pinned_baseline_then_drift_is_alarmed(tmp_path: Path) -> None:
    prober = make_prober(tmp_path)
    prober.pin_baselines([prober.probe_wren_mcp()])
    assert prober.probe_wren_mcp().reason_code == OK

    drifted_tools = {
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "tools": [
                *tools_result()["result"]["tools"],
                {"name": "exfiltrate", "description": "new tool appeared", "inputSchema": {}},
            ]
        },
    }
    mcp = FakeMcpTransport()
    mcp.rewrite("tools/list", 200, drifted_tools)
    drifted = make_prober(tmp_path, mcp=mcp)
    result = drifted.probe_wren_mcp()
    assert result.reason_code == SCHEMA_DRIFT
    assert result.is_alarm


# --- the qdrant probe ----------------------------------------------------------------


def test_qdrant_ok_reports_collections(tmp_path: Path) -> None:
    result = make_prober(tmp_path).probe_qdrant()
    assert result.reason_code == OK
    assert result.tool_names == ["acquisition_data_rooms"]


def test_qdrant_reason_codes(tmp_path: Path) -> None:
    assert (
        make_prober(tmp_path, qdrant=FaultyQdrantTransport(httpx.ConnectError("refused")))
        .probe_qdrant()
        .reason_code
        == UNREACHABLE
    )
    assert (
        make_prober(tmp_path, qdrant=FaultyQdrantTransport(httpx.ReadTimeout("too slow")))
        .probe_qdrant()
        .reason_code
        == TIMEOUT
    )
    assert (
        make_prober(tmp_path, qdrant=FakeQdrantTransport(403, {})).probe_qdrant().reason_code
        == AUTH
    )
    liar = FakeQdrantTransport(200, {"not": "qdrant"})
    assert make_prober(tmp_path, qdrant=liar).probe_qdrant().reason_code == PROTOCOL_ERROR


# --- probe_all / endpoint -------------------------------------------------------------


def test_probe_all_alarms_on_drifted_target_only(tmp_path: Path) -> None:
    prober = make_prober(tmp_path)
    prober.pin_baselines([prober.probe_wren_mcp()])
    payload = prober.probe_all()
    assert payload["schema"] == "genbi-protocol-health/v1"
    assert [t["reason_code"] for t in payload["targets"]] == [OK, OK]
    assert payload["alarms"] == []

    mcp = FakeMcpTransport()
    mcp.rewrite("tools/list", 200, {"jsonrpc": "2.0", "id": 1, "result": {"tools": []}})
    payload = make_prober(tmp_path, mcp=mcp).probe_all()
    assert any(a["reason_code"] == SCHEMA_DRIFT for a in payload["alarms"])


def test_protocol_health_endpoint_returns_target_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prober = make_prober(tmp_path)
    monkeypatch.setattr(genbi_routes, "build_prober", lambda: prober)
    client = TestClient(api_index.app)
    response = client.get("/api/v1/genbi/health/protocols")
    assert response.status_code == 200
    body = response.json()
    assert {t["target"] for t in body["targets"]} == {"wren-mcp", "qdrant"}
    # No baseline pinned in a fresh environment: surfaced, and honest about it.
    mcp_target = next(t for t in body["targets"] if t["target"] == "wren-mcp")
    assert mcp_target["reason_code"] == BASELINE_MISSING


def test_real_httpx_transport_decodes_json_and_flags_non_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The real transport's contract: (status, decoded) with None for non-JSON
    bodies — the classification path must never crash on a lying server."""

    def fake_post(url: str, **_kwargs: Any) -> httpx.Response:
        if url.endswith("json"):
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": {}})
        return httpx.Response(200, text="<html>not json</html>")

    monkeypatch.setattr(httpx, "post", fake_post)
    transport = HttpxMcpTransport()
    status, decoded = transport.post_json("http://t.test/json", {}, 1.0)
    assert status == 200 and decoded == {"jsonrpc": "2.0", "id": 1, "result": {}}
    status, decoded = transport.post_json("http://t.test/html", {}, 1.0)
    assert status == 200 and decoded is None
