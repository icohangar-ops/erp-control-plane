"""Typed claim lifecycle: state machine, four-eyes, human lock, lock-gated export.

Covers the engine (control_plane/claims.py) deterministically — every `now` is
injected — and the minimal HTTP surface (api/claims/routes.py) through
TestClient against a temp ledger.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from api import index as api_index
from control_plane.claims import (
    DEFAULT_LEDGER_PATH,
    TRANSITIONS,
    ClaimAction,
    ClaimEngine,
    ClaimKind,
    ClaimLifecycleError,
    ClaimNotLocked,
    ClaimStatus,
    claim_id_for,
    resolve_ledger_path,
)

NOW = datetime(2026, 9, 20, 12, 0, 0, tzinfo=UTC)
LATER = NOW + timedelta(hours=2)
MUCH_LATER = NOW + timedelta(days=1)
HUMAN = "Shyam Desigan"


@pytest.fixture()
def engine(tmp_path: Path) -> ClaimEngine:
    return ClaimEngine(ledger_path=tmp_path / "claims_ledger.jsonl")


def _open_claim(engine: ClaimEngine, **overrides: object):
    params: dict[str, object] = {
        "kind": ClaimKind.KPI_METRIC,
        "subject": "gmroi",
        "statement": "GMROI for the August window is 1.73",
        "asserted_value": "1.73",
        "opened_by": "analyst",
        "now": NOW,
        "unit": "ratio",
        "window_start": "2026-08-01",
        "window_end": "2026-08-31",
        "evidence": {"metric": "gmroi_q3", "ledger": "kpi_ledger.jsonl"},
    }
    params.update(overrides)
    return engine.open(**params)  # type: ignore[arg-type]


def _advance_to_approved(engine: ClaimEngine, claim_id: str) -> None:
    engine.apply(claim_id, ClaimAction.SUBMIT, actor="analyst", now=LATER)
    engine.apply(claim_id, ClaimAction.START_REVIEW, actor="controller", now=LATER)
    engine.apply(claim_id, ClaimAction.APPROVE, actor="controller", now=MUCH_LATER)


# --- the state machine as data ------------------------------------------------


def test_transition_table_is_total_and_terminal_states_are_closed() -> None:
    live = {
        ClaimStatus.DRAFT,
        ClaimStatus.SUBMITTED,
        ClaimStatus.UNDER_REVIEW,
        ClaimStatus.APPROVED,
    }
    for status in ClaimStatus:
        outgoing = {action for (s, action) in TRANSITIONS if s is status}
        if status in live:
            assert outgoing, f"{status.value} should have at least one outgoing transition"
        else:
            assert not outgoing, f"terminal state {status.value} must have no outgoing transitions"


# --- opening claims -----------------------------------------------------------


def test_open_assigns_deterministic_content_id(engine: ClaimEngine) -> None:
    claim = _open_claim(engine)
    assert claim.status is ClaimStatus.DRAFT
    assert claim.claim_id == claim_id_for(ClaimKind.KPI_METRIC, "gmroi", "2026-08-01", "analyst")
    # Identity is (kind, subject, window, opener): statement/value changes do not
    # change the id — the same identity is refused rather than overwritten.
    twin_engine = ClaimEngine(ledger_path=engine.ledger_path)
    twin = twin_engine.open(
        kind=ClaimKind.KPI_METRIC,
        subject="gmroi",
        statement="different words, same identity",
        asserted_value="1.90",
        opened_by="analyst",
        now=LATER,
        window_start="2026-08-01",
    )
    assert twin.claim_id == claim.claim_id


def test_duplicate_open_is_refused(engine: ClaimEngine) -> None:
    _open_claim(engine)
    with pytest.raises(ClaimLifecycleError, match="already exists"):
        _open_claim(engine, asserted_value="1.90")


def test_empty_statement_refused(engine: ClaimEngine) -> None:
    with pytest.raises(ClaimLifecycleError, match="must state something"):
        _open_claim(engine, statement="   ")


# --- the happy path and refusals ---------------------------------------------


def test_happy_path_reaches_locked(engine: ClaimEngine) -> None:
    claim = _open_claim(engine)
    _advance_to_approved(engine, claim.claim_id)
    locked = engine.apply(
        claim.claim_id,
        ClaimAction.LOCK,
        actor="controller",
        now=MUCH_LATER,
        confirmed_by=HUMAN,
    )
    assert locked.status is ClaimStatus.LOCKED
    assert locked.locked_by == HUMAN


def test_invalid_transition_is_refused_not_silenced(engine: ClaimEngine) -> None:
    claim = _open_claim(engine)
    with pytest.raises(ClaimLifecycleError, match="not allowed from draft"):
        engine.apply(claim.claim_id, ClaimAction.APPROVE, actor="controller", now=LATER)
    # and the claim did not move
    assert engine.claims[claim.claim_id].status is ClaimStatus.DRAFT


def test_terminal_states_have_no_exits(engine: ClaimEngine) -> None:
    claim = _open_claim(engine)
    _advance_to_approved(engine, claim.claim_id)
    engine.apply(
        claim.claim_id, ClaimAction.LOCK, actor="controller", now=MUCH_LATER, confirmed_by=HUMAN
    )
    with pytest.raises(ClaimLifecycleError, match="not allowed from locked"):
        engine.apply(claim.claim_id, ClaimAction.WITHDRAW, actor="analyst", now=MUCH_LATER)


def test_reject_and_withdraw_paths(engine: ClaimEngine) -> None:
    rejected = _open_claim(engine)
    engine.apply(rejected.claim_id, ClaimAction.SUBMIT, actor="analyst", now=LATER)
    engine.apply(rejected.claim_id, ClaimAction.START_REVIEW, actor="controller", now=LATER)
    engine.apply(rejected.claim_id, ClaimAction.REJECT, actor="controller", now=MUCH_LATER)
    assert engine.claims[rejected.claim_id].status is ClaimStatus.REJECTED

    withdrawn = _open_claim(engine, subject="spend/po-411")
    engine.apply(withdrawn.claim_id, ClaimAction.SUBMIT, actor="analyst", now=LATER)
    engine.apply(withdrawn.claim_id, ClaimAction.WITHDRAW, actor="analyst", now=MUCH_LATER)
    assert engine.claims[withdrawn.claim_id].status is ClaimStatus.WITHDRAWN


def test_unknown_claim_refused(engine: ClaimEngine) -> None:
    with pytest.raises(ClaimLifecycleError, match="unknown claim"):
        engine.apply("deadbeef", ClaimAction.SUBMIT, actor="analyst", now=NOW)


# --- four-eyes and the human lock --------------------------------------------


def test_submitter_cannot_approve_own_claim(engine: ClaimEngine) -> None:
    claim = _open_claim(engine)
    engine.apply(claim.claim_id, ClaimAction.SUBMIT, actor="analyst", now=LATER)
    engine.apply(claim.claim_id, ClaimAction.START_REVIEW, actor="analyst", now=LATER)
    with pytest.raises(ClaimLifecycleError, match="four-eyes"):
        engine.apply(claim.claim_id, ClaimAction.APPROVE, actor="analyst", now=MUCH_LATER)


def test_submitter_cannot_lock_own_claim(engine: ClaimEngine) -> None:
    claim = _open_claim(engine)
    _advance_to_approved(engine, claim.claim_id)
    with pytest.raises(ClaimLifecycleError, match="four-eyes"):
        engine.apply(
            claim.claim_id, ClaimAction.LOCK, actor="analyst", now=MUCH_LATER, confirmed_by=HUMAN
        )


def test_whitespace_actor_is_refused_on_every_transition(engine: ClaimEngine) -> None:
    # A single space passes the HTTP field's length-1 validation; the engine
    # must refuse it everywhere, or a blank actor skips four-eyes and locks.
    claim = _open_claim(engine)
    with pytest.raises(ClaimLifecycleError, match="named actor"):
        engine.apply(claim.claim_id, ClaimAction.SUBMIT, actor=" ", now=LATER)
    engine.apply(claim.claim_id, ClaimAction.SUBMIT, actor="analyst", now=LATER)
    with pytest.raises(ClaimLifecycleError, match="named actor"):
        engine.apply(claim.claim_id, ClaimAction.START_REVIEW, actor="  ", now=LATER)
    engine.apply(claim.claim_id, ClaimAction.START_REVIEW, actor="controller", now=LATER)
    with pytest.raises(ClaimLifecycleError, match="named actor"):
        engine.apply(claim.claim_id, ClaimAction.APPROVE, actor="   ", now=MUCH_LATER)
    engine.apply(claim.claim_id, ClaimAction.APPROVE, actor="controller", now=MUCH_LATER)
    with pytest.raises(ClaimLifecycleError, match="named actor"):
        engine.apply(
            claim.claim_id, ClaimAction.LOCK, actor="\t", now=MUCH_LATER, confirmed_by=HUMAN
        )


def test_lock_requires_named_human_confirmer(engine: ClaimEngine) -> None:
    claim = _open_claim(engine)
    _advance_to_approved(engine, claim.claim_id)
    with pytest.raises(ClaimLifecycleError, match="named human confirmer"):
        engine.apply(claim.claim_id, ClaimAction.LOCK, actor="controller", now=MUCH_LATER)
    with pytest.raises(ClaimLifecycleError, match="must not be the acting reviewer"):
        engine.apply(
            claim.claim_id,
            ClaimAction.LOCK,
            actor="controller",
            now=MUCH_LATER,
            confirmed_by="controller",
        )


# --- lock-gated export --------------------------------------------------------


def test_export_refuses_unlocked_claims(engine: ClaimEngine) -> None:
    claim = _open_claim(engine)
    with pytest.raises(ClaimNotLocked):
        engine.export([claim.claim_id])


def test_export_refuses_missing_and_mixed_sets(engine: ClaimEngine) -> None:
    locked = _open_claim(engine, subject="close/2026-08")
    _advance_to_approved(engine, locked.claim_id)
    engine.apply(
        locked.claim_id, ClaimAction.LOCK, actor="controller", now=MUCH_LATER, confirmed_by=HUMAN
    )
    draft = _open_claim(engine, subject="spend/po-411")
    with pytest.raises(ClaimLifecycleError, match="no claims requested"):
        engine.export([])
    with pytest.raises(ClaimNotLocked, match="missing"):
        engine.export(["nope"])
    with pytest.raises(ClaimNotLocked, match="not LOCKED"):
        engine.export([locked.claim_id, draft.claim_id])  # all-or-nothing


def test_export_is_lock_gated_and_byte_stable(engine: ClaimEngine) -> None:
    a = _open_claim(engine, subject="close/2026-08")
    _advance_to_approved(engine, a.claim_id)
    engine.apply(
        a.claim_id, ClaimAction.LOCK, actor="controller", now=MUCH_LATER, confirmed_by=HUMAN
    )
    b = _open_claim(
        engine, kind=ClaimKind.SPEND_DECISION, subject="spend/po-411", opened_by="buyer"
    )
    engine.apply(b.claim_id, ClaimAction.SUBMIT, actor="buyer", now=LATER)
    engine.apply(b.claim_id, ClaimAction.START_REVIEW, actor="controller", now=LATER)
    engine.apply(b.claim_id, ClaimAction.APPROVE, actor="controller", now=MUCH_LATER)
    engine.apply(
        b.claim_id, ClaimAction.LOCK, actor="controller", now=MUCH_LATER, confirmed_by=HUMAN
    )

    first = engine.export([a.claim_id, b.claim_id])
    assert [c["claim_id"] for c in first["package"]["claims"]] == [a.claim_id, b.claim_id]
    assert first["package"]["history"][a.claim_id][-1]["confirmed_by"] == HUMAN
    second = engine.export([a.claim_id, b.claim_id])
    assert first["bytes"] == second["bytes"]
    assert first["content_sha256"] == second["content_sha256"]


# --- ledger persistence --------------------------------------------------------


def test_ledger_replay_rebuilds_state_and_history(engine: ClaimEngine) -> None:
    claim = _open_claim(engine)
    _advance_to_approved(engine, claim.claim_id)
    engine.apply(
        claim.claim_id, ClaimAction.LOCK, actor="controller", now=MUCH_LATER, confirmed_by=HUMAN
    )

    reloaded = ClaimEngine.load(engine.ledger_path)
    stored = reloaded.claims[claim.claim_id]
    assert stored.status is ClaimStatus.LOCKED
    assert stored.locked_by == HUMAN
    replayed_actions = [t.action for t in reloaded.history[claim.claim_id]]
    assert replayed_actions == [
        ClaimAction.SUBMIT,
        ClaimAction.START_REVIEW,
        ClaimAction.APPROVE,
        ClaimAction.LOCK,
    ]
    # A reloaded engine continues the lifecycle coherently.
    with pytest.raises(ClaimLifecycleError, match="not allowed from locked"):
        reloaded.apply(claim.claim_id, ClaimAction.APPROVE, actor="controller", now=MUCH_LATER)


def test_load_missing_ledger_is_empty_engine(tmp_path: Path) -> None:
    engine = ClaimEngine.load(tmp_path / "absent.jsonl")
    assert engine.claims == {}


def test_resolve_ledger_path_env_override(tmp_path: Path) -> None:
    assert resolve_ledger_path({}) == DEFAULT_LEDGER_PATH
    assert (
        resolve_ledger_path({"CLAIMS_LEDGER_PATH": str(tmp_path / "x.jsonl")})
        == tmp_path / "x.jsonl"
    )


# --- the HTTP surface (minimal review-UI wiring) -------------------------------


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("CLAIMS_LEDGER_PATH", str(tmp_path / "api_claims.jsonl"))
    return TestClient(api_index.app)


def _post_claim(client: TestClient, **overrides: object) -> dict:
    payload: dict[str, object] = {
        "kind": "kpi_metric",
        "subject": "gmroi",
        "statement": "GMROI for the August window is 1.73",
        "asserted_value": "1.73",
        "opened_by": "analyst",
        "unit": "ratio",
        "window_start": "2026-08-01",
        "window_end": "2026-08-31",
        "evidence": {"metric": "gmroi_q3"},
    }
    payload.update(overrides)
    response = client.post("/api/v1/claims", json=payload)
    assert response.status_code == 201, response.text
    return response.json()["claim"]


def test_api_open_list_detail_flow(client: TestClient) -> None:
    claim = _post_claim(client)
    listing = client.get("/api/v1/claims")
    assert listing.status_code == 200
    assert [c["claim_id"] for c in listing.json()["claims"]] == [claim["claim_id"]]
    detail = client.get(f"/api/v1/claims/{claim['claim_id']}")
    assert detail.status_code == 200
    assert detail.json()["claim"]["status"] == "draft"
    assert client.get("/api/v1/claims/nope").status_code == 404


def test_api_transition_happy_path_and_four_eyes(client: TestClient) -> None:
    claim = _post_claim(client)
    url = f"/api/v1/claims/{claim['claim_id']}/transitions"
    submitted = client.post(url, json={"action": "submit", "actor": "analyst"})
    assert submitted.json()["claim"]["status"] == "submitted"
    reviewing = client.post(url, json={"action": "start_review", "actor": "controller"})
    assert reviewing.json()["claim"]["status"] == "under_review"
    four_eyes = client.post(url, json={"action": "approve", "actor": "analyst"})
    assert four_eyes.status_code == 422
    assert "four-eyes" in four_eyes.json()["detail"]
    approved = client.post(url, json={"action": "approve", "actor": "controller"})
    assert approved.json()["claim"]["status"] == "approved"
    missing_confirmer = client.post(url, json={"action": "lock", "actor": "controller"})
    assert missing_confirmer.status_code == 422
    locked = client.post(url, json={"action": "lock", "actor": "controller", "confirmed_by": HUMAN})
    assert locked.json()["claim"]["status"] == "locked"


def test_api_export_is_lock_gated(client: TestClient) -> None:
    claim = _post_claim(client)
    gated = client.post("/api/v1/claims/export", json={"claim_ids": [claim["claim_id"]]})
    assert gated.status_code == 409  # not locked yet
    url = f"/api/v1/claims/{claim['claim_id']}/transitions"
    client.post(url, json={"action": "submit", "actor": "analyst"})
    client.post(url, json={"action": "start_review", "actor": "controller"})
    client.post(url, json={"action": "approve", "actor": "controller"})
    client.post(url, json={"action": "lock", "actor": "controller", "confirmed_by": HUMAN})
    exported = client.post("/api/v1/claims/export", json={"claim_ids": [claim["claim_id"]]})
    assert exported.status_code == 200
    body = exported.json()
    assert body["claims"][0]["status"] == "locked"
    assert len(body["content_sha256"]) == 64
