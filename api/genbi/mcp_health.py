"""Protocol-level health probes with schema-drift alarms (codesentinel
mcp-health reference, [SecOps] P3).

A TCP check lies. A service whose port is open but whose MCP handshake no
longer speaks our protocol version, or whose tool surface changed under a
pinned deploy, is not healthy — it is a different service wearing the same
port. This module probes the WrenAI/Qdrant GenBI surface at the protocol
level instead:

- **wren-mcp** — a real MCP handshake over Streamable HTTP: JSON-RPC
  ``initialize`` (validate ``protocolVersion``, ``serverInfo``,
  ``capabilities``), then ``notifications/initialized``, then ``tools/list``.
  The returned tool schemas are canonical-JSON hashed and compared against a
  pinned baseline; any difference is a ``schema_drift`` alarm.
- **qdrant** — the store's own HTTP API (``GET /collections``), not just a
  socket connect.

Failure classification (reason codes, exhaustive on purpose):

- ``ok``                — handshake + schema check passed
- ``unreachable``       — connection refused / DNS / route failure
- ``timeout``           — the probe deadline elapsed
- ``auth``              — the endpoint rejected the probe's credentials
- ``protocol_error``    — reached the endpoint but it is not speaking the
  expected protocol (bad JSON-RPC, missing serverInfo, malformed tools)
- ``schema_drift``      — handshake works but the tool schemas differ from
  the pinned baseline (ALARM: a silent vendored-code or server upgrade)
- ``baseline_missing``  — no baseline pinned yet for this target; probe
  connectivity is proven, drift is not yet checkable (ALARM: pin one)

Results are surfaced at the dedicated ``GET /api/v1/genbi/health/protocols``
endpoint (always HTTP 200 — this is a diagnostic surface; monitors alert on
the ``alarms`` array, and folding it into the data-layer ``/health`` would
couple liveness semantics across two failure domains).

Baselines live under the gitignored runtime-state tree: the vendored MCP
server legitimately changes across WrenAI upgrades, so pinning is a
deliberate ops action (``pin_baselines`` / the build_prober CLI), not a
committed file that rots.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

import httpx

# --- scheme constants --------------------------------------------------------

HEALTH_SCHEMA = "genbi-protocol-health/v1"
BASELINE_SCHEMA = "genbi-mcp-schema-baseline/v1"

MCP_PROTOCOL_VERSION = "2024-11-05"
MCP_CLIENT_INFO = {"name": "cubiczan-erp-control-plane", "version": "0.1.0"}

# Reason codes — exhaustive on purpose so dashboards can rely on stable values.
OK = "ok"
UNREACHABLE = "unreachable"
TIMEOUT = "timeout"
AUTH = "auth"
PROTOCOL_ERROR = "protocol_error"
SCHEMA_DRIFT = "schema_drift"
BASELINE_MISSING = "baseline_missing"

#: Reason codes that constitute an alarm (anything not OK, except the
#: informational baseline-missing state — which is still surfaced, but is an
#: unpinned target, not a drifting one).
ALARM_CODES = frozenset({UNREACHABLE, TIMEOUT, AUTH, PROTOCOL_ERROR, SCHEMA_DRIFT})


class BaselineWriteError(Exception):
    """The schema baseline could not be pinned."""


# --- transports (injected; real probes use HTTP) -------------------------------


class McpProbeTransport(Protocol):
    """POST a JSON-RPC body, return (http_status, decoded-json-or-None).

    HTTP-level faults are raised as httpx errors and classified by the
    prober; everything else is reported as a (status, body) pair.
    """

    def post_json(self, url: str, body: dict[str, Any], timeout: float) -> tuple[int, Any]: ...


class HttpxMcpTransport:
    """Streamable-HTTP JSON-RPC transport for MCP probes."""

    def post_json(self, url: str, body: dict[str, Any], timeout: float) -> tuple[int, Any]:
        response = httpx.post(
            url,
            json=body,
            headers={
                "Accept": "application/json, text/event-stream",
                "Content-Type": "application/json",
            },
            timeout=timeout,
        )
        try:
            decoded: Any = response.json()
        except ValueError:
            decoded = None
        return response.status_code, decoded


class QdrantProbeTransport(Protocol):
    """GET a Qdrant management endpoint, return (status, decoded-json-or-None)."""

    def get_json(self, url: str, timeout: float) -> tuple[int, Any]: ...


class HttpxQdrantProbeTransport:
    """HTTP transport for Qdrant protocol probes."""

    def __init__(self, api_key: str | None = None) -> None:
        self._headers = {"api-key": api_key} if api_key else {}

    def get_json(self, url: str, timeout: float) -> tuple[int, Any]:
        response = httpx.get(url, headers=self._headers, timeout=timeout)
        try:
            decoded: Any = response.json()
        except ValueError:
            decoded = None
        return response.status_code, decoded


# --- results -------------------------------------------------------------------


@dataclass(frozen=True)
class ProbeResult:
    """One target's protocol-level health, classified and hash-stamped."""

    target: str
    kind: str  # "mcp" | "qdrant"
    reason_code: str
    detail: str
    latency_ms: int
    schema_hash: str | None = None
    baseline_schema_hash: str | None = None
    tool_names: list[str] | None = None
    checked_at: str = field(default_factory=lambda: dt.datetime.now(dt.UTC).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return {
            "target": self.target,
            "kind": self.kind,
            "reason_code": self.reason_code,
            "detail": self.detail,
            "latency_ms": self.latency_ms,
            "schema_hash": self.schema_hash,
            "baseline_schema_hash": self.baseline_schema_hash,
            "tool_names": self.tool_names,
            "checked_at": self.checked_at,
        }

    @property
    def is_alarm(self) -> bool:
        return self.reason_code in ALARM_CODES


def canonical_tool_schema_hash(tools: list[dict[str, Any]]) -> str:
    """SHA-256 over the canonical JSON of the tools' schema surface.

    Canonicalization (sorted keys, compact separators) matches the receipts'
    and CHP ledger's convention, so the same hashing story backs every sealed
    surface in the control plane. Descriptions are included: a changed tool
    description is exactly the kind of silent drift this alarm exists for.
    """
    surface = [
        {
            "name": tool.get("name"),
            "description": tool.get("description"),
            "inputSchema": tool.get("inputSchema"),
        }
        for tool in tools
    ]
    canonical = json.dumps(surface, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# --- the prober -------------------------------------------------------------------


class ProtocolHealthProber:
    """Protocol-level probes for the WrenAI/Qdrant GenBI surface."""

    def __init__(
        self,
        *,
        mcp_url: str,
        qdrant_url: str,
        baseline_path: Path,
        mcp_transport: McpProbeTransport | None = None,
        qdrant_transport: QdrantProbeTransport | None = None,
        timeout: float = 5.0,
    ) -> None:
        self.mcp_url = mcp_url
        self.qdrant_url = qdrant_url.rstrip("/")
        self.baseline_path = Path(baseline_path)
        self._mcp_transport = mcp_transport or HttpxMcpTransport()
        self._qdrant_transport = qdrant_transport or HttpxQdrantProbeTransport()
        self.timeout = timeout

    # ------------------------------------------------------------------
    # MCP handshake
    # ------------------------------------------------------------------

    def probe_wren_mcp(self) -> ProbeResult:
        """initialize + tools/list against the wren-mcp server, then
        schema-drift the returned tool surface against the pinned baseline."""
        return self._mcp_handshake()

    def _mcp_handshake(self) -> ProbeResult:
        started = dt.datetime.now(dt.UTC)
        try:
            status, body = self._mcp_transport.post_json(
                self.mcp_url,
                _rpc(
                    "initialize",
                    {
                        "protocolVersion": MCP_PROTOCOL_VERSION,
                        "capabilities": {},
                        "clientInfo": MCP_CLIENT_INFO,
                    },
                ),
                self.timeout,
            )
        except httpx.ConnectError as exc:
            return self._mcp_result(started, UNREACHABLE, f"connect failed: {exc}")
        except httpx.TimeoutException as exc:
            return self._mcp_result(started, TIMEOUT, f"initialize timed out: {exc}")
        except httpx.HTTPError as exc:
            return self._mcp_result(started, UNREACHABLE, f"transport failure: {exc}")

        classified = _classify_http_status(status)
        if classified is not None:
            return self._mcp_result(started, classified, f"initialize returned HTTP {status}")

        init_result, error_detail = _jsonrpc_result(body, "initialize")
        if init_result is None:
            return self._mcp_result(started, PROTOCOL_ERROR, error_detail or "initialize failed")
        server_info = init_result.get("serverInfo")
        if not isinstance(server_info, dict) or not server_info.get("name"):
            return self._mcp_result(
                started, PROTOCOL_ERROR, "initialize result is missing serverInfo.name"
            )

        # The initialized notification gets no response; send it and move on.
        # A server that rejects it will fail on tools/list below, loudly.
        with contextlib.suppress(httpx.HTTPError):
            self._mcp_transport.post_json(
                self.mcp_url, _rpc("notifications/initialized", {}), self.timeout
            )

        try:
            status, body = self._mcp_transport.post_json(
                self.mcp_url, _rpc("tools/list", {}), self.timeout
            )
        except httpx.TimeoutException as exc:
            return self._mcp_result(started, TIMEOUT, f"tools/list timed out: {exc}")
        except httpx.HTTPError as exc:
            return self._mcp_result(started, PROTOCOL_ERROR, f"tools/list transport failure: {exc}")

        classified = _classify_http_status(status)
        if classified is not None:
            return self._mcp_result(started, classified, f"tools/list returned HTTP {status}")
        tools, error_detail = _jsonrpc_result(body, "tools/list")
        if tools is None:
            return self._mcp_result(started, PROTOCOL_ERROR, error_detail or "tools/list failed")
        tool_list = tools.get("tools") if isinstance(tools, dict) else None
        if not isinstance(tool_list, list) or not all(
            isinstance(t, dict) and isinstance(t.get("name"), str) for t in tool_list
        ):
            return self._mcp_result(
                started, PROTOCOL_ERROR, "tools/list result has a malformed tools array"
            )

        schema_hash = canonical_tool_schema_hash(tool_list)
        tool_names = [t["name"] for t in tool_list]
        return self._drift_check(started, schema_hash, tool_names)

    def _drift_check(
        self, started: dt.datetime, schema_hash: str, tool_names: list[str]
    ) -> ProbeResult:
        baseline_hash = self._baseline_hash()
        if baseline_hash is None:
            return self._mcp_result(
                started,
                BASELINE_MISSING,
                "handshake OK; no schema baseline pinned — pin one to arm drift alarms",
                schema_hash=schema_hash,
                tool_names=tool_names,
            )
        if baseline_hash != schema_hash:
            return self._mcp_result(
                started,
                SCHEMA_DRIFT,
                "tool schemas differ from the pinned baseline — a vendored-code or "
                "server upgrade landed without re-pinning",
                schema_hash=schema_hash,
                tool_names=tool_names,
            )
        return self._mcp_result(
            started,
            OK,
            "handshake OK; tool schemas match the pinned baseline",
            schema_hash=schema_hash,
            tool_names=tool_names,
        )

    def _baseline_hash(self) -> str | None:
        try:
            baseline = json.loads(self.baseline_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            return None  # no baseline / unreadable baseline -> not armed
        entry = (
            baseline.get("targets", {}).get("wren-mcp", {}) if isinstance(baseline, dict) else {}
        )
        hash_value = entry.get("schema_hash") if isinstance(entry, dict) else None
        return hash_value if isinstance(hash_value, str) else None

    def pin_baselines(self, results: list[ProbeResult] | None = None) -> dict[str, Any]:
        """Pin the current tool-schema hash(es) as the drift baseline.

        An ops action after a deliberate WrenAI/MCP upgrade — never after a
        probe that merely succeeded while something else looked off.
        """
        results = results if results is not None else [self.probe_wren_mcp()]
        targets: dict[str, Any] = {}
        if self.baseline_path.exists():
            try:
                existing = json.loads(self.baseline_path.read_text(encoding="utf-8"))
                targets = existing.get("targets", {}) if isinstance(existing, dict) else {}
            except (OSError, json.JSONDecodeError) as exc:
                raise BaselineWriteError(f"existing baseline unreadable: {exc}") from exc
        for result in results:
            if result.schema_hash is not None:
                targets[result.target] = {
                    "schema_hash": result.schema_hash,
                    "tool_names": result.tool_names,
                    "pinned_at": dt.datetime.now(dt.UTC).isoformat(),
                }
        document = {"schema": BASELINE_SCHEMA, "targets": targets}
        try:
            self.baseline_path.parent.mkdir(parents=True, exist_ok=True)
            self.baseline_path.write_text(
                json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
            )
        except OSError as exc:
            raise BaselineWriteError(f"baseline not writable: {exc}") from exc
        return document

    # ------------------------------------------------------------------
    # Qdrant probe
    # ------------------------------------------------------------------

    def probe_qdrant(self) -> ProbeResult:
        """Protocol-level Qdrant check: its own HTTP API answers correctly."""
        started = dt.datetime.now(dt.UTC)
        try:
            status, body = self._qdrant_transport.get_json(
                f"{self.qdrant_url}/collections", self.timeout
            )
        except httpx.ConnectError as exc:
            return _result(started, "qdrant", "qdrant", UNREACHABLE, f"connect failed: {exc}")
        except httpx.TimeoutException as exc:
            return _result(started, "qdrant", "qdrant", TIMEOUT, f"request timed out: {exc}")
        except httpx.HTTPError as exc:
            return _result(started, "qdrant", "qdrant", UNREACHABLE, f"transport failure: {exc}")

        classified = _classify_http_status(status)
        if classified is not None:
            return _result(
                started, "qdrant", "qdrant", classified, f"/collections returned HTTP {status}"
            )
        if not isinstance(body, dict) or "result" not in body:
            return _result(
                started,
                "qdrant",
                "qdrant",
                PROTOCOL_ERROR,
                "response is not Qdrant collections JSON",
            )
        collections = body.get("result", {}).get("collections", [])
        names = [
            c.get("name")
            for c in collections
            if isinstance(c, dict) and isinstance(c.get("name"), str)
        ]
        return _result(
            started,
            "qdrant",
            "qdrant",
            OK,
            f"API OK; {len(names)} collection(s) visible",
            tool_names=names,
        )

    # ------------------------------------------------------------------

    def probe_all(self) -> dict[str, Any]:
        """Probe every GenBI protocol target; the health endpoint's payload."""
        results = [self.probe_wren_mcp(), self.probe_qdrant()]
        return {
            "schema": HEALTH_SCHEMA,
            "checked_at": dt.datetime.now(dt.UTC).isoformat(),
            "targets": [r.to_dict() for r in results],
            "alarms": [
                {"target": r.target, "reason_code": r.reason_code, "detail": r.detail}
                for r in results
                if r.is_alarm
            ],
        }

    # ------------------------------------------------------------------

    def _mcp_result(
        self,
        started: dt.datetime,
        reason_code: str,
        detail: str,
        *,
        schema_hash: str | None = None,
        tool_names: list[str] | None = None,
    ) -> ProbeResult:
        return _result(
            started,
            "wren-mcp",
            "mcp",
            reason_code,
            detail,
            schema_hash=schema_hash,
            tool_names=tool_names,
        )


# --- helpers --------------------------------------------------------------------


def _rpc(method: str, params: dict[str, Any]) -> dict[str, Any]:
    body: dict[str, Any] = {"jsonrpc": "2.0", "id": 1, "method": method}
    if params:
        body["params"] = params
    return body


def _jsonrpc_result(body: Any, call: str) -> tuple[Any, str | None]:
    """Extract result from a JSON-RPC response, or a protocol-error detail."""
    if not isinstance(body, dict):
        return None, f"{call} returned non-JSON-RPC body"
    if body.get("jsonrpc") != "2.0" or ("id" not in body):
        return None, f"{call} response is not JSON-RPC 2.0 with an id"
    if "error" in body:
        return None, f"{call} JSON-RPC error: {body['error']}"
    if "result" not in body:
        return None, f"{call} response has neither result nor error"
    return body["result"], None


def _classify_http_status(status: int) -> str | None:
    """Map an HTTP status to a reason code, or None when 2xx."""
    if 200 <= status < 300:
        return None
    if status in (401, 403):
        return AUTH
    return PROTOCOL_ERROR


def _result(
    started: dt.datetime,
    target: str,
    kind: str,
    reason_code: str,
    detail: str,
    *,
    schema_hash: str | None = None,
    tool_names: list[str] | None = None,
) -> ProbeResult:
    return ProbeResult(
        target=target,
        kind=kind,
        reason_code=reason_code,
        detail=detail,
        latency_ms=_elapsed_ms(started),
        schema_hash=schema_hash,
        tool_names=tool_names,
    )


def _elapsed_ms(started: dt.datetime) -> int:
    return max(0, int((dt.datetime.now(dt.UTC) - started).total_seconds() * 1000))
