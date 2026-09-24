"""HTTP surface of the GenBI loop: routes, error mapping, and the coverage queue.

The promote endpoint builds its service from the environment; tests monkeypatch
``build_service`` to inject a Superset client backed by the fake transport from
``test_genbi_promotion``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from test_genbi_promotion import (
    ANSWER_DATE,
    QUESTION,
    SQL,
    VIZ,
    FakeSuperset,
    make_service,
)

from api import index as api_index
from api.genbi import routes as genbi_routes
from api.genbi.receipts import PROMOTION_TOOL, promotion_args, promotion_policy_version

PROMOTE_URL = "/api/v1/genbi/answers/promote"
PROMOTE_PAYLOAD: dict[str, Any] = {
    "question": QUESTION,
    "sql": SQL,
    "viz": {
        "viz_type": "echarts_timeseries_bar",
        "x_axis": "branch_code",
        "metrics": [{"column": "revenue", "aggregate": "SUM", "label": "Revenue"}],
    },
    "answer_date": ANSWER_DATE.isoformat(),
}


@pytest.fixture()
def fake() -> FakeSuperset:
    return FakeSuperset()


@pytest.fixture()
def env(tmp_path: Path, analytics_file: Path) -> dict[str, str]:
    """Same shape as test_genbi_promotion.env, with the seeded analytics DB."""
    return {
        "GENBI_SUPERSET_URL": "http://superset.test",
        "GENBI_SUPERSET_USER": "admin",
        "GENBI_SUPERSET_PASSWORD": "pw",
        "GENBI_SUPERSET_READONLY_DATABASE_ID": "1",
        "GENBI_ANALYTICS_DUCKDB_PATH": str(analytics_file),
        "GENBI_AUDIT_PATH": str(tmp_path / "audit.jsonl"),
        "GENBI_COVERAGE_PATH": str(tmp_path / "coverage.jsonl"),
        "GENBI_CHP_DECISIONS_PATH": str(tmp_path / "chp_decisions.jsonl"),
        "GENBI_APPROVAL_RECEIPTS_PATH": str(tmp_path / "approval_receipts.jsonl"),
    }


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch, env: dict[str, str], fake: FakeSuperset) -> TestClient:
    """The demo app with a GenBI service wired to the fake Superset."""
    service = make_service(env, fake)
    monkeypatch.setattr(genbi_routes, "build_service", lambda: service)
    return TestClient(api_index.app)


def test_promote_endpoint_round_trips(client: TestClient, fake: FakeSuperset) -> None:
    response = client.post(PROMOTE_URL, json=PROMOTE_PAYLOAD)
    assert response.status_code == 200
    body = response.json()
    assert body["chart_created"] is True
    assert body["slug"].startswith("genbi-")
    assert body["dashboard_url"].startswith("http://superset.test/superset/dashboard/")
    assert len(fake.charts) == 1

    # Idempotent re-post: same slug, nothing new created.
    again = client.post(PROMOTE_URL, json=PROMOTE_PAYLOAD).json()
    assert again["chart_created"] is False
    assert again["chart_id"] == body["chart_id"]


def test_promote_endpoint_backing_schema_alias(client: TestClient, fake: FakeSuperset) -> None:
    payload = {
        **PROMOTE_PAYLOAD,
        "backing": {"schema": "main_marts", "table": "kpi_headline"},
    }
    response = client.post(PROMOTE_URL, json=payload)
    assert response.status_code == 200
    dataset = next(iter(fake.datasets.values()))
    assert dataset["table_name"] == "kpi_headline"
    assert dataset["schema"] == "main_marts"
    assert dataset["sql"] is None, "physical backing must not carry the answer SQL"


def test_guardrail_rejection_maps_to_422(client: TestClient, fake: FakeSuperset) -> None:
    response = client.post(PROMOTE_URL, json={**PROMOTE_PAYLOAD, "sql": "drop table x"})
    assert response.status_code == 422
    assert response.json()["detail"], "the caller must get the guardrail reason"
    assert not fake.charts


def test_validation_error_maps_to_422(client: TestClient) -> None:
    response = client.post(PROMOTE_URL, json={"question": "q", "sql": "select 1"})
    assert response.status_code == 422  # missing viz


def test_audit_endpoint_lists_the_trail(client: TestClient) -> None:
    client.post(PROMOTE_URL, json=PROMOTE_PAYLOAD)
    records = client.get("/api/v1/genbi/audit").json()
    assert [r["outcome"] for r in records] == ["promoted", "executed"]
    assert records[1]["question"] == QUESTION


def test_coverage_request_endpoints_dedupe(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GENBI_COVERAGE_PATH", str(tmp_path / "coverage.jsonl"))
    with TestClient(api_index.app) as client:
        first = client.post(
            "/api/v1/genbi/coverage-requests",
            json={"question": "What is our OTIF by region?"},
        )
        client.post(
            "/api/v1/genbi/coverage-requests",
            json={"question": "what is our otif by region?"},
        )
        listing = client.get("/api/v1/genbi/coverage-requests").json()
    assert first.status_code == 201
    assert len(listing) == 1
    assert listing[0]["request_count"] == 2


def test_root_endpoint_advertises_genbi_routes(client: TestClient) -> None:
    endpoints = client.get("/", headers={"accept": "application/json"}).json()["endpoints"]
    assert PROMOTE_URL in endpoints
    assert "/api/v1/genbi/coverage-requests" in endpoints
    assert "/api/v1/genbi/contracts" in endpoints
    assert "/api/v1/genbi/approval-receipts" in endpoints


def _valid_receipt(env: dict[str, str], fake: FakeSuperset, actor: str) -> dict[str, Any]:
    """Sign against a locally built service; the ledger paths and key come from env."""
    service = make_service(env, fake)
    return service.receipts.sign(
        tool=PROMOTION_TOOL,
        actor=actor,
        args=promotion_args(
            question=QUESTION, sql=SQL, answer_date=ANSWER_DATE, backing=None, viz=VIZ
        ),
        policy_version=promotion_policy_version(service.settings),
    )


def test_confirmed_promotion_records_a_locked_decision(
    client: TestClient, env: dict[str, str], fake: FakeSuperset
) -> None:
    receipt = _valid_receipt(env, fake, actor="sam@cubiczan.com")
    response = client.post(
        PROMOTE_URL,
        json={**PROMOTE_PAYLOAD, "confirmed_by": "sam@cubiczan.com", "approval_receipt": receipt},
    )
    assert response.status_code == 200
    chp = response.json()["chp"]
    assert chp["session_status"] == "LOCKED"
    assert chp["confirmed_by"] == "sam@cubiczan.com"
    assert chp["approval_receipt"]["receipt_id"] == receipt["receipt_id"]
    assert chp["foundation_score"] == 70  # general question: guardrails + bounded result

    listing = client.get("/api/v1/genbi/decisions").json()
    assert len(listing) == 1
    record = listing[0]
    assert record["envelope_valid"] is True
    assert record["decision_id"].startswith("promote-")
    detail = client.get(f"/api/v1/genbi/decisions/{record['decision_id']}").json()
    assert detail["question"] == QUESTION
    assert client.get("/api/v1/genbi/decisions/promote-missing").status_code == 404


def test_chp_rejection_maps_to_422_and_audits(client: TestClient, fake: FakeSuperset) -> None:
    response = client.post(PROMOTE_URL, json={**PROMOTE_PAYLOAD, "question": "asdf jkl"})
    assert response.status_code == 422
    assert "CHP" in response.json()["detail"]
    assert not fake.charts

    audit = client.get("/api/v1/genbi/audit").json()
    assert audit[0]["outcome"] == "chp_rejected"
    assert client.get("/api/v1/genbi/decisions").json() == []


def test_confirmed_promotion_without_a_receipt_maps_to_422(
    client: TestClient, fake: FakeSuperset
) -> None:
    response = client.post(
        PROMOTE_URL, json={**PROMOTE_PAYLOAD, "confirmed_by": "sam@cubiczan.com"}
    )
    assert response.status_code == 422
    assert "human_lock_bears_receipt" in response.json()["detail"]
    assert not fake.charts  # the human lock was never reached, let alone satisfied


def test_tampered_receipt_maps_to_422_and_audits(
    client: TestClient, env: dict[str, str], fake: FakeSuperset
) -> None:
    receipt = _valid_receipt(env, fake, actor="sam@cubiczan.com")
    tampered = {**receipt, "actor": "someone.else"}
    response = client.post(
        PROMOTE_URL,
        json={**PROMOTE_PAYLOAD, "confirmed_by": "sam@cubiczan.com", "approval_receipt": tampered},
    )
    assert response.status_code == 422
    assert "receipt" in response.json()["detail"].lower()
    assert not fake.charts
    audit = client.get("/api/v1/genbi/audit").json()
    assert audit[0]["outcome"] == "receipt_rejected"


def test_contracts_endpoint_lists_enforced_rules(client: TestClient) -> None:
    rules = client.get("/api/v1/genbi/contracts").json()
    assert len(rules) > 0
    assert all(rule["enforced"] == "true" for rule in rules)


def test_approval_receipts_endpoint_lists_the_ledger(
    client: TestClient, env: dict[str, str], fake: FakeSuperset
) -> None:
    assert client.get("/api/v1/genbi/approval-receipts").json() == []
    receipt = _valid_receipt(env, fake, actor="sam@cubiczan.com")
    client.post(
        PROMOTE_URL,
        json={**PROMOTE_PAYLOAD, "confirmed_by": "sam@cubiczan.com", "approval_receipt": receipt},
    )
    events = client.get("/api/v1/genbi/approval-receipts").json()
    assert [event["event"] for event in events] == ["redeemed", "issued"]
    assert events[0]["receipt_id"] == receipt["receipt_id"]
