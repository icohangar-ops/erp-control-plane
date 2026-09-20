"""Typed claim lifecycle: governance as a data model (servicesell-finbridge reference).

A **claim** is a governed statement about ERP data — a KPI value ("GMROI for
the window is 1.73"), a spend decision, or a close-period attestation. Before
this module the repo governed *answers* (CHP decision cases, receipts) but had
no typed object for a claim someone must review, approve, lock, and later
export as part of a close package. The lifecycle is the governance:

- **The state machine is data** — ``TRANSITIONS`` maps
  ``(status, action) -> status``. ``apply()`` is the only way a claim moves;
  an unknown transition is a refused lifecycle error, never a silent no-op
  (fail-closed, like the connector contract).
- **Four-eyes** — the actor who submits a claim can neither approve nor lock
  it. Approval and locking are someone else's decision.
- **Human lock** — locking requires a named ``confirmed_by`` human, mirroring
  the CHP human-lock posture (api/genbi/chp.py) already in force on the
  promotion surface.
- **Lock-gated export** — ``export()`` refuses any claim that is not
  ``LOCKED``. Export is the moment a claim leaves the governed boundary
  (a close package, a board report); unlocked claims have no business
  crossing it. Export bytes are canonical JSON plus a content SHA-256 —
  byte-stable, so the same locked claim exports identically every time.

Persistence follows the CHP decision-ledger precedent: an append-only JSONL
ledger under the gitignored state tree (not the control-plane store, which
records ingestion facts, not governance). Ledger records are ``OPEN`` and
``TRANSITION`` events; reloading replays them to rebuild current state.
The module is stdlib-only and deterministic: no wall clock — ``now`` is always
injected; ids are content hashes; ledger replay is order-stable.

The review UI wiring is deliberately minimal (``api/claims/routes.py``): a
JSON surface (list / detail / transition / export) and nothing else. The
review experience is the hard part of a claims product; this module owns the
lifecycle mechanics, and the API stays thin until a real reviewer exists.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

CLAIMS_SCHEMA = "cubiczan-erp/claim-lifecycle/v1"

#: Default ledger location (under the gitignored state tree, beside the CHP
#: decision ledger). Overridable via ``CLAIMS_LEDGER_PATH``.
DEFAULT_LEDGER_PATH = Path("./data/state/claims_ledger.jsonl")


class ClaimLifecycleError(Exception):
    """A claim transition was refused — the lifecycle is fail-closed."""


class ClaimNotLocked(ClaimLifecycleError):
    """Export refused: the claim has not reached LOCKED."""


class ClaimKind(StrEnum):
    """What the claim is about — the governed statements this repo makes."""

    KPI_METRIC = "kpi_metric"  # "GMROI for the window is 1.73"
    SPEND_DECISION = "spend_decision"  # a committed/vendor spend call
    CLOSE_PERIOD = "close_period"  # "August close is complete"


class ClaimStatus(StrEnum):
    """Lifecycle states. LOCKED is the export gate; the rest are progression."""

    DRAFT = "draft"
    SUBMITTED = "submitted"
    UNDER_REVIEW = "under_review"
    APPROVED = "approved"
    LOCKED = "locked"
    REJECTED = "rejected"
    WITHDRAWN = "withdrawn"


class ClaimAction(StrEnum):
    """The transitions a reviewer (or author) can request."""

    SUBMIT = "submit"
    START_REVIEW = "start_review"
    APPROVE = "approve"
    REJECT = "reject"
    LOCK = "lock"
    WITHDRAW = "withdraw"


#: The state machine as data — the whole lifecycle in one table. Terminal
#: statuses (LOCKED, REJECTED, WITHDRAWN) have no outgoing transitions.
TRANSITIONS: dict[tuple[ClaimStatus, ClaimAction], ClaimStatus] = {
    (ClaimStatus.DRAFT, ClaimAction.SUBMIT): ClaimStatus.SUBMITTED,
    (ClaimStatus.DRAFT, ClaimAction.WITHDRAW): ClaimStatus.WITHDRAWN,
    (ClaimStatus.SUBMITTED, ClaimAction.START_REVIEW): ClaimStatus.UNDER_REVIEW,
    (ClaimStatus.SUBMITTED, ClaimAction.WITHDRAW): ClaimStatus.WITHDRAWN,
    (ClaimStatus.UNDER_REVIEW, ClaimAction.APPROVE): ClaimStatus.APPROVED,
    (ClaimStatus.UNDER_REVIEW, ClaimAction.REJECT): ClaimStatus.REJECTED,
    (ClaimStatus.UNDER_REVIEW, ClaimAction.WITHDRAW): ClaimStatus.WITHDRAWN,
    (ClaimStatus.APPROVED, ClaimAction.LOCK): ClaimStatus.LOCKED,
}


@dataclass(frozen=True)
class Claim:
    """One governed statement, its lifecycle state, and its evidence."""

    claim_id: str
    kind: ClaimKind
    subject: str  # e.g. "gmroi", "po/vendor/BUILD-RITE", "2026-08"
    statement: str
    asserted_value: str  # normalized text (numbers as their canonical string)
    unit: str | None
    window_start: str | None  # ISO date
    window_end: str | None  # ISO date
    evidence: dict[str, str]  # named references (ledgers, metric ids, reports)
    opened_by: str
    opened_at: datetime
    status: ClaimStatus
    updated_at: datetime
    locked_by: str | None = None  # the confirmed_by human, set at LOCK

    def to_dict(self) -> dict[str, Any]:
        return {
            "claim_id": self.claim_id,
            "kind": self.kind.value,
            "subject": self.subject,
            "statement": self.statement,
            "asserted_value": self.asserted_value,
            "unit": self.unit,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "evidence": dict(self.evidence),
            "opened_by": self.opened_by,
            "opened_at": self.opened_at.isoformat(),
            "status": self.status.value,
            "updated_at": self.updated_at.isoformat(),
            "locked_by": self.locked_by,
        }


def _canonical_json(payload: dict[str, Any]) -> str:
    """Canonical form: sorted keys, compact separators, stable unicode handling."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def claim_id_for(kind: ClaimKind, subject: str, window_start: str | None, opened_by: str) -> str:
    """Deterministic id: first 16 hex of SHA-256 over the canonical claim identity."""
    canonical = _canonical_json(
        {
            "kind": kind.value,
            "subject": subject,
            "window_start": window_start or "",
            "opened_by": opened_by,
        }
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def _advance(
    claim: Claim, *, status: ClaimStatus, updated_at: datetime, locked_by: str | None
) -> Claim:
    """The one way a claim's state advances — used by both live transitions and
    ledger replay, so a reloaded engine lands on exactly the live state."""
    return Claim(
        claim_id=claim.claim_id,
        kind=claim.kind,
        subject=claim.subject,
        statement=claim.statement,
        asserted_value=claim.asserted_value,
        unit=claim.unit,
        window_start=claim.window_start,
        window_end=claim.window_end,
        evidence=claim.evidence,
        opened_by=claim.opened_by,
        opened_at=claim.opened_at,
        status=status,
        updated_at=updated_at,
        locked_by=locked_by,
    )


@dataclass(frozen=True)
class ClaimTransition:
    """One applied lifecycle event, appended to the ledger."""

    claim_id: str
    action: ClaimAction
    from_status: ClaimStatus
    to_status: ClaimStatus
    actor: str
    at: datetime
    note: str | None = None
    confirmed_by: str | None = None  # named human at LOCK

    def to_dict(self) -> dict[str, Any]:
        return {
            "record": "TRANSITION",
            "claim_id": self.claim_id,
            "action": self.action.value,
            "from_status": self.from_status.value,
            "to_status": self.to_status.value,
            "actor": self.actor,
            "at": self.at.isoformat(),
            "note": self.note,
            "confirmed_by": self.confirmed_by,
        }


@dataclass
class ClaimEngine:
    """The lifecycle machine over a JSONL ledger. All times injected — deterministic."""

    ledger_path: Path = DEFAULT_LEDGER_PATH
    claims: dict[str, Claim] = field(default_factory=dict)
    history: dict[str, list[ClaimTransition]] = field(default_factory=dict)

    # ------------------------------------------------------------- lifecycle
    def open(
        self,
        *,
        kind: ClaimKind,
        subject: str,
        statement: str,
        asserted_value: str,
        opened_by: str,
        now: datetime,
        unit: str | None = None,
        window_start: str | None = None,
        window_end: str | None = None,
        evidence: dict[str, str] | None = None,
    ) -> Claim:
        """Open a claim in DRAFT. Claim ids are content-derived: opening the
        same identity twice is refused — a revision is a new claim, not an
        overwrite."""
        claim_id = claim_id_for(kind, subject, window_start, opened_by)
        if claim_id in self.claims:
            raise ClaimLifecycleError(
                f"claim {claim_id} ({kind.value}/{subject}) already exists; "
                "a revision is a new claim, not an overwrite"
            )
        if not statement.strip():
            raise ClaimLifecycleError("a claim must state something — empty statement refused")
        claim = Claim(
            claim_id=claim_id,
            kind=kind,
            subject=subject,
            statement=statement,
            asserted_value=asserted_value,
            unit=unit,
            window_start=window_start,
            window_end=window_end,
            evidence=dict(evidence or {}),
            opened_by=opened_by,
            opened_at=now,
            status=ClaimStatus.DRAFT,
            updated_at=now,
        )
        self._append({"record": "OPEN", "claim": claim.to_dict()})
        self.claims[claim_id] = claim
        self.history[claim_id] = []
        return claim

    def apply(
        self,
        claim_id: str,
        action: ClaimAction,
        *,
        actor: str,
        now: datetime,
        note: str | None = None,
        confirmed_by: str | None = None,
    ) -> Claim:
        """The only way a claim moves. Validates the transition table, four-eyes,
        and the human-lock requirement; appends the event to the ledger."""
        claim = self.claims.get(claim_id)
        if claim is None:
            raise ClaimLifecycleError(f"unknown claim {claim_id}")
        to_status = TRANSITIONS.get((claim.status, action))
        if to_status is None:
            raise ClaimLifecycleError(
                f"claim {claim_id}: {action.value} is not allowed from "
                f"{claim.status.value} — the transition table refuses it"
            )
        if not actor.strip():
            raise ClaimLifecycleError("a transition needs a named actor")
        # Four-eyes: the submitter is never the approver or the locker.
        if (
            action in (ClaimAction.APPROVE, ClaimAction.LOCK)
            and actor.strip().lower() == claim.opened_by.strip().lower()
        ):
            raise ClaimLifecycleError(
                f"claim {claim_id}: {action.value} by the submitter "
                f"({claim.opened_by}) refused — four-eyes requires a second person"
            )
        # Human lock: locking names a human confirmer, mirroring the CHP posture.
        if action is ClaimAction.LOCK:
            if not confirmed_by or not confirmed_by.strip():
                raise ClaimLifecycleError(
                    f"claim {claim_id}: locking requires a named human confirmer "
                    "(confirmed_by) — no anonymous locks"
                )
            if confirmed_by.strip().lower() == actor.strip().lower():
                raise ClaimLifecycleError(
                    f"claim {claim_id}: the confirmer must not be the acting reviewer"
                )
        transition = ClaimTransition(
            claim_id=claim_id,
            action=action,
            from_status=claim.status,
            to_status=to_status,
            actor=actor,
            at=now,
            note=note,
            confirmed_by=confirmed_by if action is ClaimAction.LOCK else None,
        )
        self._append(transition.to_dict())
        self.claims[claim_id] = _advance(
            claim,
            status=to_status,
            updated_at=now,
            locked_by=confirmed_by if action is ClaimAction.LOCK else claim.locked_by,
        )
        self.history[claim_id].append(transition)
        return self.claims[claim_id]

    # ----------------------------------------------------------------- export
    def export(self, claim_ids: list[str]) -> dict[str, Any]:
        """Lock-gated export: canonical JSON for each claim plus a content hash.

        Refuses the whole export if ANY claim is not LOCKED — a close package
        is all-locked or nothing, never a mix someone can quietly unpick.
        Export bytes are byte-stable: identical locked claims produce identical
        canonical JSON and identical hashes.
        """
        if not claim_ids:
            raise ClaimLifecycleError("export refused: no claims requested")
        unlocked = [
            cid
            for cid in claim_ids
            if (claim := self.claims.get(cid)) is None or claim.status is not ClaimStatus.LOCKED
        ]
        if unlocked:
            raise ClaimNotLocked(
                "export refused — claims not LOCKED: "
                + ", ".join(
                    f"{cid}({self.claims[cid].status.value})"
                    if cid in self.claims
                    else f"{cid}(missing)"
                    for cid in unlocked
                )
            )
        package = {
            "schema": CLAIMS_SCHEMA,
            "claims": [self.claims[cid].to_dict() for cid in claim_ids],
            "history": {cid: [t.to_dict() for t in self.history.get(cid, [])] for cid in claim_ids},
        }
        canonical = _canonical_json(package)
        return {
            "package": package,
            "content_sha256": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
            "bytes": canonical.encode("utf-8"),
        }

    # -------------------------------------------------------------- persistence
    def _append(self, record: dict[str, Any]) -> None:
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        with self.ledger_path.open("a", encoding="utf-8") as ledger:
            ledger.write(_canonical_json(record) + "\n")

    @classmethod
    def load(cls, ledger_path: Path) -> ClaimEngine:
        """Replay the ledger to rebuild state. Order-stable, deterministic."""
        engine = cls(ledger_path=ledger_path)
        if not ledger_path.exists():
            return engine
        with ledger_path.open(encoding="utf-8") as ledger:
            for line in ledger:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                if record.get("record") == "OPEN":
                    raw = record["claim"]
                    claim = Claim(
                        claim_id=raw["claim_id"],
                        kind=ClaimKind(raw["kind"]),
                        subject=raw["subject"],
                        statement=raw["statement"],
                        asserted_value=raw["asserted_value"],
                        unit=raw["unit"],
                        window_start=raw["window_start"],
                        window_end=raw["window_end"],
                        evidence=dict(raw["evidence"]),
                        opened_by=raw["opened_by"],
                        opened_at=datetime.fromisoformat(raw["opened_at"]),
                        status=ClaimStatus(raw["status"]),
                        updated_at=datetime.fromisoformat(raw["updated_at"]),
                        locked_by=raw["locked_by"],
                    )
                    engine.claims[claim.claim_id] = claim
                    engine.history.setdefault(claim.claim_id, [])
                elif record.get("record") == "TRANSITION":
                    transition = ClaimTransition(
                        claim_id=record["claim_id"],
                        action=ClaimAction(record["action"]),
                        from_status=ClaimStatus(record["from_status"]),
                        to_status=ClaimStatus(record["to_status"]),
                        actor=record["actor"],
                        at=datetime.fromisoformat(record["at"]),
                        note=record.get("note"),
                        confirmed_by=record.get("confirmed_by"),
                    )
                    engine.history.setdefault(transition.claim_id, []).append(transition)
                    # Replay must advance the claim itself, not just its history —
                    # otherwise a reloaded engine resurrects every claim as DRAFT.
                    current = engine.claims.get(transition.claim_id)
                    if current is not None:
                        engine.claims[transition.claim_id] = _advance(
                            current,
                            status=transition.to_status,
                            updated_at=transition.at,
                            locked_by=(
                                transition.confirmed_by
                                if transition.action is ClaimAction.LOCK
                                else current.locked_by
                            ),
                        )
        return engine


def resolve_ledger_path(env: dict[str, str] | None = None) -> Path:
    """CLAIMS_LEDGER_PATH or the state-tree default (12-factor, like the CHP ledger)."""
    env = dict(os.environ if env is None else env)
    return Path(env["CLAIMS_LEDGER_PATH"]) if env.get("CLAIMS_LEDGER_PATH") else DEFAULT_LEDGER_PATH
