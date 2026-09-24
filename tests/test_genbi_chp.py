"""CHP-hardened promotion gate: R0 refusal, deterministic foundation scoring, human lock, and the decision ledger.

Covers the consensus-hardening-protocol integration (``api/genbi/chp.py``):

- the promotion-shaped R0 gate refuses ill-posed requests before the engine;
- the deterministic adversary scores guardrails + bounded result + golden
  parity (finance-domain answers gate at CHP's finance floor of 100 through
  parity evidence, and a parity mismatch is fatal);
- every hardened case opens ``PROVISIONAL_LOCK`` and a named confirmer locks
  it through CHP third-party validation;
- every promotion seals a CHP payload envelope into the append-only decision
  ledger, whose reads re-validate envelope integrity.

Golden-set parity is exercised against a small temporary golden file whose
expected value matches the ``dealer_revenue`` conftest fixture (120 + 90 + 60
= 270), so parity here is self-grounding rather than pinned to the shipped
golden set.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from chp import Verdict
from chp.models import SessionStatus
from test_genbi_promotion import ANSWER_DATE, QUESTION, SQL, VIZ, FakeSuperset, make_service

from api.genbi.chp import ChpPromotionGate, ChpRejection
from api.genbi.config import GenbiSettings
from api.genbi.guardrails import ExecutionResult
from api.genbi.promote import PromotionService
from api.genbi.receipts import PROMOTION_TOOL, promotion_args, promotion_policy_version

GOLDEN_QUESTION = "What is our total revenue?"
GOLDEN_SQL = "select sum(revenue) as revenue from dealer_revenue"
TOTAL_REVENUE = 270.0
CONFIRMER = "sam@cubiczan.com"


def execution(rows: list[tuple], columns: tuple[str, ...] = ("revenue",)) -> ExecutionResult:
    return ExecutionResult(
        columns=list(columns),
        rows=rows,
        row_count=len(rows),
        latency_ms=3,
        duckdb_uri="duckdb:///analytics.duckdb?access_mode=READ_ONLY&read_only=1",
    )


def scalar_case(expected: float = TOTAL_REVENUE) -> dict:
    return {
        "id": "total_revenue",
        "question": GOLDEN_QUESTION,
        "metric": "revenue",
        "unit": "usd",
        "expected": expected,
        "tolerance": 0.005,
    }


def base_env(
    tmp_path: Path, cases: list[dict] | None = None, require_lock: bool = False
) -> dict[str, str]:
    golden_path = tmp_path / "golden.yaml"
    golden_path.write_text(
        yaml.safe_dump(
            {
                "schema_version": 1,
                "generated_from": "test",
                "window": {"start": "2026-01-06", "end": "2026-08-25", "days": 232},
                "cases": list(cases or []),
            }
        )
    )
    env = {
        "GENBI_SUPERSET_URL": "http://superset.test",
        "GENBI_SUPERSET_USER": "admin",
        "GENBI_SUPERSET_PASSWORD": "pw",
        "GENBI_SUPERSET_READONLY_DATABASE_ID": "1",
        "GENBI_ANALYTICS_DUCKDB_PATH": str(tmp_path / "analytics.duckdb"),
        "GENBI_AUDIT_PATH": str(tmp_path / "audit.jsonl"),
        "GENBI_COVERAGE_PATH": str(tmp_path / "coverage.jsonl"),
        "GENBI_CHP_DECISIONS_PATH": str(tmp_path / "chp_decisions.jsonl"),
        "GENBI_APPROVAL_RECEIPTS_PATH": str(tmp_path / "approval_receipts.jsonl"),
        "GENBI_GOLDEN_PATH": str(golden_path),
    }
    if require_lock:
        env["GENBI_CHP_REQUIRE_HUMAN_LOCK"] = "1"
    return env


def make_gate(env: dict[str, str]) -> ChpPromotionGate:
    return ChpPromotionGate(GenbiSettings.from_env(env))


def empty_executor(sql: str, **kwargs: object) -> ExecutionResult:
    return execution([])


def promotion_receipt(service: PromotionService) -> dict[str, Any]:
    """A valid receipt for the canonical promotion request, bound to CONFIRMER."""
    return service.receipts.sign(
        tool=PROMOTION_TOOL,
        actor=CONFIRMER,
        args=promotion_args(
            question=QUESTION, sql=SQL, answer_date=ANSWER_DATE, backing=None, viz=VIZ
        ),
        policy_version=promotion_policy_version(service.settings),
    )


# ----------------------------------------------------------------------- R0


def test_r0_refuses_a_non_analytical_question_before_execution(tmp_path: Path) -> None:
    gate = make_gate(base_env(tmp_path))
    with pytest.raises(ChpRejection) as excinfo:
        gate.open_r0("hello there", "select 1", None)
    assert excinfo.value.evaluation.results["Worth_it"] == "FATAL"


def test_r0_refuses_an_empty_request(tmp_path: Path) -> None:
    gate = make_gate(base_env(tmp_path))
    with pytest.raises(ChpRejection) as excinfo:
        gate.open_r0("What is revenue by branch?", "   ", None)
    assert excinfo.value.evaluation.results["Solvable"] == "FATAL"


def test_r0_accepts_analytical_and_golden_questions(tmp_path: Path) -> None:
    gate = make_gate(base_env(tmp_path, [scalar_case()]))
    gate.open_r0(QUESTION, "select 1", None)
    gate.open_r0(GOLDEN_QUESTION, GOLDEN_SQL, None)


# ---------------------------------------------------------------- foundation


def test_golden_parity_scores_a_full_finance_foundation(tmp_path: Path) -> None:
    gate = make_gate(base_env(tmp_path, [scalar_case()]))
    assessment = gate.assess_foundation(GOLDEN_QUESTION, execution([(TOTAL_REVENUE,)]))
    assert assessment.domain == "finance"
    assert assessment.score == 100
    assert assessment.parity is not None and assessment.parity.within_tolerance is True


def test_golden_parity_mismatch_is_fatal(tmp_path: Path) -> None:
    gate = make_gate(base_env(tmp_path, [scalar_case(expected=999.0)]))
    with pytest.raises(ChpRejection, match="MISMATCH"):
        gate.harden(
            question=GOLDEN_QUESTION,
            sql=GOLDEN_SQL,
            execution=execution([(TOTAL_REVENUE,)]),
            answer_date=ANSWER_DATE,
        )


def test_general_answer_passes_without_parity(tmp_path: Path) -> None:
    gate = make_gate(base_env(tmp_path))
    assessment = gate.assess_foundation(
        QUESTION,
        execution([("BLDG", 120.0), ("ELEC", 90.0), ("TOOL", 60.0)], ("branch_code", "revenue")),
    )
    assert assessment.domain == "general"
    assert assessment.score == 70  # guardrails 40 + bounded result 30; no parity evidence


def test_zero_rows_cannot_self_certify(tmp_path: Path) -> None:
    gate = make_gate(base_env(tmp_path))
    assessment = gate.assess_foundation(QUESTION, execution([]))
    assert assessment.score == 40


def test_hardened_case_opens_provisional_and_locks_with_a_confirmer(tmp_path: Path) -> None:
    gate = make_gate(base_env(tmp_path))
    decision = gate.harden(
        question=QUESTION,
        sql=SQL,
        execution=execution(
            [("BLDG", 120.0), ("ELEC", 90.0), ("TOOL", 60.0)], ("branch_code", "revenue")
        ),
        answer_date=ANSWER_DATE,
    )
    assert decision.case.status == SessionStatus.PROVISIONAL_LOCK
    assert gate.lock(decision, CONFIRMER) == SessionStatus.LOCKED


# -------------------------------------------------------------------- ledger


def hardened(gate: ChpPromotionGate):
    decision = gate.harden(
        question=QUESTION,
        sql=SQL,
        execution=execution(
            [("BLDG", 120.0), ("ELEC", 90.0), ("TOOL", 60.0)], ("branch_code", "revenue")
        ),
        answer_date=ANSWER_DATE,
    )
    return gate.record(
        decision,
        question=QUESTION,
        sql=SQL,
        slug="genbi-test",
        artifacts={"chart_id": 1, "dataset_id": 2, "dashboard_id": 3, "slug": "genbi-test"},
        confirmed_by=None,
    )


def test_decision_record_seals_an_envelope(tmp_path: Path) -> None:
    gate = make_gate(base_env(tmp_path))
    record = hardened(gate)

    listing = gate.records.list()
    assert len(listing) == 1
    assert listing[0]["envelope_valid"] is True
    assert listing[0]["decision_id"] == record["decision_id"]
    assert gate.records.get(record["decision_id"])["question"] == QUESTION
    assert gate.records.get("promote-missing") is None


def test_tampered_ledger_envelopes_read_as_invalid(tmp_path: Path) -> None:
    gate = make_gate(base_env(tmp_path))
    hardened(gate)

    path = gate.records.path
    lines = path.read_text(encoding="utf-8").splitlines()
    entry = json.loads(lines[0])
    # tamper with the sealed payload body: inflate the foundation score
    entry["body"] = entry["body"].replace('"foundation_score": 70', '"foundation_score": 100')
    lines[0] = json.dumps(entry)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    record = gate.records.list()[0]
    assert record["integrity_valid"] is False
    assert record["envelope_valid"] is True  # the CHP envelope checks structure only


# ------------------------------------------------------- service integration


@pytest.fixture()
def chp_env(tmp_path: Path, analytics_file: Path) -> dict[str, str]:
    env = base_env(tmp_path)
    env["GENBI_ANALYTICS_DUCKDB_PATH"] = str(analytics_file)
    return env


def test_promotion_is_provisional_without_a_confirmer(chp_env: dict[str, str]) -> None:
    service = make_service(chp_env, FakeSuperset())
    result = service.promote(QUESTION, SQL, VIZ, answer_date=ANSWER_DATE)
    assert result["chp"]["session_status"] == SessionStatus.PROVISIONAL_LOCK.value
    assert result["chp"]["foundation_score"] == 70
    assert result["chp"]["confirmed_by"] is None


def test_a_named_confirmer_locks_the_decision(chp_env: dict[str, str]) -> None:
    service = make_service(chp_env, FakeSuperset())
    result = service.promote(
        QUESTION,
        SQL,
        VIZ,
        answer_date=ANSWER_DATE,
        confirmed_by=CONFIRMER,
        approval_receipt=promotion_receipt(service),
    )
    assert result["chp"]["session_status"] == SessionStatus.LOCKED.value
    record = service.gate.records.list()[0]
    assert record["confirmed_by"] == CONFIRMER
    assert record["artifacts"]["slug"] == result["slug"]


def test_reframe_promotion_requires_a_confirmer(chp_env: dict[str, str]) -> None:
    service = make_service(chp_env, FakeSuperset(), executor=empty_executor)
    with pytest.raises(ChpRejection, match="cannot self-certify"):
        service.promote(QUESTION, SQL, VIZ, answer_date=ANSWER_DATE)
    assert service.gate.records.list() == []
    assert service.audit.list()[0]["outcome"] == "chp_rejected"


def test_require_human_lock_refuses_unconfirmed_promotions(chp_env: dict[str, str]) -> None:
    chp_env["GENBI_CHP_REQUIRE_HUMAN_LOCK"] = "1"
    service = make_service(chp_env, FakeSuperset())
    with pytest.raises(ChpRejection, match="human lock"):
        service.promote(QUESTION, SQL, VIZ, answer_date=ANSWER_DATE)
    assert service.gate.records.list() == []

    result = make_service(chp_env, FakeSuperset()).promote(
        QUESTION,
        SQL,
        VIZ,
        answer_date=ANSWER_DATE,
        confirmed_by=CONFIRMER,
        approval_receipt=promotion_receipt(service),
    )
    assert result["chp"]["session_status"] == SessionStatus.LOCKED.value


def test_golden_parity_promotion_self_certifies_at_the_finance_floor(
    tmp_path: Path, analytics_file: Path
) -> None:
    env = base_env(tmp_path, [scalar_case()])
    env["GENBI_ANALYTICS_DUCKDB_PATH"] = str(analytics_file)
    service = make_service(env, FakeSuperset())
    result = service.promote(GOLDEN_QUESTION, GOLDEN_SQL, VIZ, answer_date=ANSWER_DATE)
    assert result["chp"]["foundation_score"] == 100
    assert result["chp"]["r0_verdict"] == Verdict.PASS.value


def test_parity_mismatch_refuses_the_promotion(tmp_path: Path, analytics_file: Path) -> None:
    env = base_env(tmp_path, [scalar_case(expected=999.0)])
    env["GENBI_ANALYTICS_DUCKDB_PATH"] = str(analytics_file)
    service = make_service(env, FakeSuperset())
    with pytest.raises(ChpRejection, match="MISMATCH"):
        service.promote(GOLDEN_QUESTION, GOLDEN_SQL, VIZ, answer_date=ANSWER_DATE)
    assert service.gate.records.list() == []
    outcomes = [entry["outcome"] for entry in service.audit.list()]
    assert "promoted" not in outcomes
