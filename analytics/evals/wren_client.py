"""Minimal synchronous client for the WrenAI wren-ai-service ask flow.

Contract verified against the wren-ai-service 0.29.0 source (Canner/WrenAI,
src/web/v1/routers/ask.py + src/web/v1/services/ask.py):

    POST /v1/asks        {"query": "...", "request_from": "api"} -> {"query_id": "..."}
    GET  /v1/asks/{id}/result -> {status, rephrased_question, retrieved_tables,
                                  response: [{sql, type, viewId}], error, ...}

status is one of understanding|searching|planning|generating|correcting|
finished|failed|stopped. The runner polls /result until a terminal state.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import httpx

TERMINAL_STATUSES = {"finished", "failed", "stopped"}


class WrenAIError(RuntimeError):
    """Raised for transport failures or non-accepted ask submissions."""


@dataclass
class AskOutcome:
    question: str
    status: str
    query_id: str | None = None
    sql: str | None = None
    sql_type: str | None = None
    rephrased_question: str | None = None
    retrieved_tables: list[str] = field(default_factory=list)
    reasoning: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    latency_s: float = 0.0


class WrenAIClient:
    def __init__(
        self,
        base_url: str,
        mdl_hash: str | None = None,
        timeout_s: float = 300.0,
        poll_interval_s: float = 2.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._mdl_hash = mdl_hash
        self._timeout_s = timeout_s
        self._poll_interval_s = poll_interval_s

    def ask(self, question: str) -> AskOutcome:
        payload: dict = {"query": question, "request_from": "api"}
        if self._mdl_hash:
            payload["mdl_hash"] = self._mdl_hash

        with httpx.Client(timeout=self._timeout_s) as client:
            response = client.post(f"{self._base_url}/v1/asks", json=payload)
            if response.status_code != 201:
                raise WrenAIError(
                    f"POST /v1/asks returned {response.status_code}: {response.text[:300]}"
                )
            query_id = response.json().get("query_id")
            if not query_id:
                raise WrenAIError(f"POST /v1/asks returned no query_id: {response.text[:300]}")

            import time

            started = time.monotonic()
            result = self._poll_result(client, query_id)
            result["latency_s"] = round(time.monotonic() - started, 2)
            return self._to_outcome(question, query_id, result)

    def _poll_result(self, client: httpx.Client, query_id: str) -> dict:
        import time

        deadline = time.monotonic() + self._timeout_s
        while True:
            response = client.get(f"{self._base_url}/v1/asks/{query_id}/result")
            if response.status_code != 200:
                raise WrenAIError(
                    f"GET /v1/asks/{query_id}/result returned "
                    f"{response.status_code}: {response.text[:300]}"
                )
            result = response.json()
            if result.get("status") in TERMINAL_STATUSES:
                return result
            if time.monotonic() > deadline:
                raise WrenAIError(
                    f"ask {query_id} did not reach a terminal state within {self._timeout_s}s"
                )
            time.sleep(self._poll_interval_s)

    @staticmethod
    def _to_outcome(question: str, query_id: str, result: dict) -> AskOutcome:
        responses = result.get("response") or []
        first = responses[0] if responses else {}
        error = result.get("error") or {}
        return AskOutcome(
            question=question,
            status=result.get("status", "unknown"),
            query_id=query_id,
            sql=first.get("sql"),
            sql_type=first.get("type"),
            rephrased_question=result.get("rephrased_question"),
            retrieved_tables=result.get("retrieved_tables") or [],
            reasoning=result.get("sql_generation_reasoning"),
            error_code=error.get("code"),
            error_message=error.get("message"),
            latency_s=result.get("latency_s", 0.0),
        )
