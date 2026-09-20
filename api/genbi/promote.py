"""The promotion loop: NL answer -> CHP hardening -> guardrailed execution -> governed Superset artifact.

Implements spec §4.2 as the control-plane caller of the v0.1 idempotent REST
automation. Order of operations is the governance story:

1. **CHP R0 gate** (fail closed, before the engine): the promotion request must
   be solvable, scoped, valid, and worth_it — or it is refused with nothing
   executed and the refusal audited (``chp_rejected``).
2. **Guardrails** (fail closed): the answer SQL executes against the
   canonical READ_ONLY DuckDB URI — SELECT-only, single statement, statement
   timeout, row cap — and every execution is audited (question -> SQL ->
   latency -> outcome) before the caller learns the result.
3. **CHP foundation pass**: the deterministic adversary scores the answer —
   guardrails, bounded result, golden parity against the dbt-pinned
   ``analytics/evals/golden_qa.yaml``. A finance-domain answer that cannot
   self-certify (score below CHP's finance floor) requires a named human
   confirmer; a golden parity mismatch is refused outright. Every promotion
   opens as a CHP ``PROVISIONAL_LOCK`` case; ``confirmed_by`` locks it.
4. **Governed persistence** (under the per-question mutex): dataset on the
   pre-registered READ_ONLY database (physical mart backing or virtual dataset
   over the validated SQL), chart by deterministic slug, dashboard position_json
   merged per the runbook rules with the "Ask → Save" markdown header. The
   sealed CHP decision record lands in the decision ledger.

Spec risk #10 (concurrent saves double-create) is answered by deterministic
slugs + create-or-update-by-name + a per-question-hash mutex held across the
find/create window. The mutex is process-local; multi-process deployments get
the same guarantee from the deterministic identity as long as the API runs
single-process (the demo topology) — document a store-backed lock when that
changes.
"""

from __future__ import annotations

import datetime as dt
import threading
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any

from chp import Verdict

from api.genbi import audit as audit_module
from api.genbi.audit import AuditTrail
from api.genbi.chp import ChpPromotionGate, ChpRejection
from api.genbi.config import GenbiSettings, NotConfigured
from api.genbi.coverage import CoverageQueue
from api.genbi.guardrails import (
    ExecutionResult,
    GuardrailError,
    NotReadOnlyUri,
    execute_readonly,
)
from api.genbi.layout import append_chart_row, ensure_root, fresh_layout
from api.genbi.slugs import dataset_table_name, genbi_row_id, question_hash, slug_for_question
from api.genbi.superset import SupersetClient, SupersetError
from api.genbi.viz import BackingSpec, VizSpec, chart_params

_SLUG_LOCKS: dict[str, threading.Lock] = {}
_SLUG_LOCKS_GUARD = threading.Lock()


@contextmanager
def _question_mutex(question_hash_value: str):
    """Serialize promotes of the same question within this process."""
    with _SLUG_LOCKS_GUARD:
        lock = _SLUG_LOCKS.setdefault(question_hash_value, threading.Lock())
    with lock:
        yield


class PromotionService:
    """One promotion request, end to end."""

    def __init__(
        self,
        settings: GenbiSettings,
        superset: SupersetClient,
        audit: AuditTrail,
        coverage: CoverageQueue,
        executor: Callable[..., ExecutionResult] = execute_readonly,
        gate: ChpPromotionGate | None = None,
    ) -> None:
        self.settings = settings
        self.superset = superset
        self.audit = audit
        self.coverage = coverage
        self.executor = executor
        self.gate = gate if gate is not None else ChpPromotionGate(settings)

    # ------------------------------------------------------------ public API
    def promote(
        self,
        question: str,
        sql: str,
        viz: VizSpec,
        answer_date: dt.date | None = None,
        backing: BackingSpec | None = None,
        confirmed_by: str | None = None,
    ) -> dict[str, Any]:
        """CHP-gate, guardrail, execute, audit, then persist the answer as a chart."""
        if self.settings.superset_readonly_database_id is None:
            raise NotConfigured(
                "GENBI_SUPERSET_READONLY_DATABASE_ID is not set — register the READ_ONLY "
                "DuckDB database in Superset and configure its id before promoting answers."
            )
        question_hash_value = question_hash(question)
        answer_date = answer_date or dt.datetime.now(dt.UTC).date()

        # CHP R0 — before the engine: an ill-posed request costs nothing.
        self._chp_guarded(
            question, question_hash_value, sql, lambda: self.gate.open_r0(question, sql, backing)
        )

        execution = self._execute(question, question_hash_value, sql)

        # CHP foundation pass — the deterministic adversary scores the answer.
        decision = self._chp_guarded(
            question,
            question_hash_value,
            sql,
            lambda: self.gate.harden(
                question=question, sql=sql, execution=execution, answer_date=answer_date
            ),
        )
        if decision.report.foundation_verdict != Verdict.PASS and not confirmed_by:
            reason = (
                f"CHP foundation: {decision.report.foundation_verdict.value}"
                f" (score {decision.case.foundation_score}, {decision.assessment.domain} domain)"
                " — the promotion cannot self-certify; retry with a named confirmer"
                " (confirmed_by)."
            )
            self._audit_chp(question, question_hash_value, sql, reason)
            raise ChpRejection(reason)
        if self.settings.chp_require_human_lock and not confirmed_by:
            reason = (
                "CHP human lock: GENBI_CHP_REQUIRE_HUMAN_LOCK is set — every promotion"
                " needs a named confirmer (confirmed_by)."
            )
            self._audit_chp(question, question_hash_value, sql, reason)
            raise ChpRejection(reason)
        if confirmed_by:
            self.gate.lock(decision, confirmed_by)

        with _question_mutex(question_hash_value):
            result = self._persist(
                question, question_hash_value, sql, viz, answer_date, backing, execution
            )

        record = self.gate.record(
            decision,
            question=question,
            sql=sql,
            slug=result["slug"],
            artifacts={
                "slug": result["slug"],
                "dataset_id": result["dataset_id"],
                "chart_id": result["chart_id"],
                "dashboard_id": result["dashboard_id"],
            },
            confirmed_by=confirmed_by,
        )
        result["chp"] = {
            "decision_id": record["decision_id"],
            "session_status": record["session_status"],
            "r0_verdict": record["r0_verdict"],
            "foundation_verdict": record["foundation_verdict"],
            "foundation_score": record["foundation_score"],
            "confirmed_by": confirmed_by,
        }
        return result

    # ------------------------------------------------------------------- CHP
    def _chp_guarded(self, question: str, question_hash_value: str, sql: str, step):
        """Run a CHP stage; audit and re-raise any rejection before the caller sees it."""
        try:
            return step()
        except ChpRejection as exc:
            self._audit_chp(question, question_hash_value, sql, exc.reason)
            raise

    def _audit_chp(self, question: str, question_hash_value: str, sql: str, detail: str) -> None:
        self.audit.append(
            audit_module.AuditEntry(
                question=question,
                question_hash=question_hash_value,
                sql=sql,
                latency_ms=0,
                outcome=audit_module.CHP_REJECTED,
                stage=audit_module.STAGE_CHP,
                detail=detail,
            )
        )

    # ------------------------------------------------------------ guardrails
    def _execute(self, question: str, question_hash_value: str, sql: str) -> ExecutionResult:
        """Run the guardrailed execution and audit every outcome (spec §1)."""
        try:
            result = self.executor(
                sql,
                uri=self.settings.duckdb_uri,
                timeout_seconds=self.settings.statement_timeout_seconds,
                row_cap=self.settings.row_cap,
            )
        except GuardrailError as exc:
            self.audit.append(
                audit_module.AuditEntry(
                    question=question,
                    question_hash=question_hash_value,
                    sql=sql,
                    latency_ms=0,
                    outcome=audit_module.GUARDRAIL_REJECTED,
                    detail=str(exc),
                )
            )
            raise
        self.audit.append(
            audit_module.AuditEntry(
                question=question,
                question_hash=question_hash_value,
                sql=sql,
                latency_ms=result.latency_ms,
                outcome=audit_module.EXECUTED,
                rows_returned=result.row_count,
            )
        )
        return result

    # ------------------------------------------------------------ persistence
    def _persist(
        self,
        question: str,
        question_hash_value: str,
        sql: str,
        viz: VizSpec,
        answer_date: dt.date | None,
        backing: BackingSpec | None,
        execution: ExecutionResult,
    ) -> dict[str, Any]:
        database_id = self.settings.superset_readonly_database_id
        # Governance floor (spec §4.2.6): the target database must BE the
        # read-only analytics connection, verified server-side, before any write.
        database = self.superset.get_database(database_id)
        sqlalchemy_uri = str(database.get("sqlalchemy_uri", ""))
        if "access_mode=READ_ONLY" not in sqlalchemy_uri:
            raise NotReadOnlyUri(
                f"Superset database {database_id} is not the READ_ONLY analytics connection "
                f"({sqlalchemy_uri or 'no uri'}) — refusing to persist against it."
            )

        answer_date = answer_date or dt.datetime.now(dt.UTC).date()
        slug = slug_for_question(question, answer_date)
        table_name = dataset_table_name(question)
        if backing and backing.table:
            # Physical mode: the answer resolves to a governed mart table/view.
            dataset_id, dataset_created = self.superset.ensure_dataset(
                database_id,
                backing.table,
                schema=backing.table_schema or self.settings.marts_schema,
            )
        else:
            # Virtual mode: a governed dataset over the validated answer SQL.
            dataset_id, dataset_created = self.superset.ensure_dataset(
                database_id, table_name, sql=sql
            )

        params = chart_params(viz, dataset_id, self.settings.row_cap)
        try:
            chart_id, chart_uuid, chart_created = self.superset.ensure_chart(
                slug,
                viz_type=viz.viz_type,
                datasource_id=dataset_id,
                params=params,
                description=question,
            )
        except SupersetError as exc:
            self._audit_persist(
                question, question_hash_value, sql, audit_module.SUPERSET_ERROR, exc
            )
            raise

        dashboard_id, _dashboard_created = self.superset.ensure_dashboard(
            self.settings.dashboard_slug, self.settings.dashboard_title
        )
        layout = self.superset.get_positions(dashboard_id) or fresh_layout()
        ensure_root(layout)
        append_chart_row(
            layout,
            question=question,
            chart_id=chart_id,
            chart_uuid=chart_uuid,
            width=viz.width,
            height=viz.height,
        )
        try:
            self.superset.put_positions(dashboard_id, layout)
        except SupersetError as exc:
            self._audit_persist(
                question, question_hash_value, sql, audit_module.SUPERSET_ERROR, exc
            )
            raise

        outcome = audit_module.PROMOTED if chart_created else audit_module.UPDATED
        self._audit_persist(
            question,
            question_hash_value,
            sql,
            outcome,
            artifacts={
                "slug": slug,
                "dataset_id": dataset_id,
                "chart_id": chart_id,
                "dashboard_id": dashboard_id,
                "grid_row_id": genbi_row_id(question),
            },
        )
        return {
            "question": question,
            "question_hash": question_hash_value,
            "slug": slug,
            "dataset_id": dataset_id,
            "dataset_table": backing.table if backing and backing.table else table_name,
            "dataset_created": dataset_created,
            "chart_id": chart_id,
            "chart_created": chart_created,
            "dashboard_id": dashboard_id,
            "dashboard_slug": self.settings.dashboard_slug,
            "dashboard_url": f"{self.superset.base_url}/superset/dashboard/{self.settings.dashboard_slug}/",
            "rows_returned": execution.row_count,
            "latency_ms": execution.latency_ms,
        }

    def _audit_persist(
        self,
        question: str,
        question_hash_value: str,
        sql: str,
        outcome: str,
        artifacts: dict[str, object] | Exception | None = None,
    ) -> None:
        detail = ""
        artifact_payload: dict[str, object] = {}
        if isinstance(artifacts, Exception):
            detail = str(artifacts)
        elif isinstance(artifacts, dict):
            artifact_payload = artifacts
        self.audit.append(
            audit_module.AuditEntry(
                question=question,
                question_hash=question_hash_value,
                sql=sql,
                latency_ms=0,
                outcome=outcome,
                stage=audit_module.STAGE_PERSIST,
                detail=detail,
                artifacts=artifact_payload,
            )
        )
