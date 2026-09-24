"""Audit-trail and coverage-queue store behavior: persistence, ordering, dedup."""

from __future__ import annotations

import threading
from pathlib import Path

from api.genbi import audit as audit_module
from api.genbi.audit import AuditEntry, AuditTrail
from api.genbi.coverage import CoverageQueue


def test_audit_append_then_list_newest_first(tmp_path: Path) -> None:
    trail = AuditTrail(tmp_path / "audit.jsonl")
    trail.append(
        AuditEntry(
            question="first question",
            question_hash="h1",
            sql="select 1",
            latency_ms=12,
            outcome=audit_module.EXECUTED,
            rows_returned=3,
        )
    )
    trail.append(
        AuditEntry(
            question="second question",
            question_hash="h2",
            sql="select 2",
            latency_ms=5,
            outcome=audit_module.PROMOTED,
            stage=audit_module.STAGE_PERSIST,
            artifacts={"chart_id": 7},
        )
    )
    records = trail.list()
    assert [r["question"] for r in records] == ["second question", "first question"]
    assert records[0]["outcome"] == "promoted"
    assert records[0]["artifacts"] == {"chart_id": 7}
    assert records[1]["outcome"] == "executed"
    assert records[1]["rows_returned"] == 3


def test_audit_survives_reopen(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    AuditTrail(path).append(
        AuditEntry(
            question="q",
            question_hash="h",
            sql="select 1",
            latency_ms=1,
            outcome=audit_module.EXECUTED,
        )
    )
    records = AuditTrail(path).list()
    assert len(records) == 1
    assert records[0]["sql"] == "select 1"


def test_audit_is_thread_safe(tmp_path: Path) -> None:
    trail = AuditTrail(tmp_path / "audit.jsonl")
    per_thread, threads = 25, 8

    def worker(n: int) -> None:
        for i in range(per_thread):
            trail.append(
                AuditEntry(
                    question=f"q-{n}-{i}",
                    question_hash=f"h-{n}-{i}",
                    sql="select 1",
                    latency_ms=1,
                    outcome=audit_module.EXECUTED,
                )
            )

    runners = [threading.Thread(target=worker, args=(n,)) for n in range(threads)]
    for runner in runners:
        runner.start()
    for runner in runners:
        runner.join()
    assert len(trail.list(limit=threads * per_thread)) == threads * per_thread


def test_coverage_records_and_dedupes(tmp_path: Path) -> None:
    queue = CoverageQueue(tmp_path / "coverage.jsonl")
    first = queue.record("What is our fill rate by vendor?", "not modeled yet")
    assert first["request_count"] == 1
    queue.record("what is our fill rate by vendor?", "asked again")  # same normalized question
    queue.record("Show me open POs", "not modeled yet")

    records = queue.list()
    assert len(records) == 2, "duplicate questions must dedupe into one record"
    by_question = {r["question"]: r for r in records}
    assert by_question["What is our fill rate by vendor?"]["request_count"] == 2
    assert by_question["Show me open POs"]["request_count"] == 1
    assert records[0]["request_count"] == 2, "most-requested question lists first"


def test_coverage_survives_reopen(tmp_path: Path) -> None:
    path = tmp_path / "coverage.jsonl"
    CoverageQueue(path).record("Question one")
    CoverageQueue(path).record("question one")
    records = CoverageQueue(path).list()
    assert len(records) == 1
    assert records[0]["request_count"] == 2
