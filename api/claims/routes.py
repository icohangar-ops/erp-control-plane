"""Minimal claims review surface (the lock-gated lifecycle's API wiring).

Deliberately thin: list, detail, open, transition, export. The claim
lifecycle itself (state machine, four-eyes, human lock, lock-gated export)
lives in ``control_plane/claims.py``; this router only carries HTTP — no HTML,
no JavaScript, no workflow logic. The review experience is the hard part of a
claims product and is intentionally left for when a real reviewer exists;
until then this JSON surface keeps the lifecycle exercisable end to end.

The engine is loaded per request (JSONL ledger replay is cheap at demo
scale), so the surface stays stateless and safe behind multiple workers.
"""

from __future__ import annotations

from datetime import UTC, datetime
from http import HTTPStatus

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from control_plane.claims import (
    CLAIMS_SCHEMA,
    ClaimAction,
    ClaimEngine,
    ClaimKind,
    ClaimLifecycleError,
    ClaimNotLocked,
    ClaimStatus,
    resolve_ledger_path,
)

router = APIRouter(prefix="/api/v1/claims", tags=["claims"])


class OpenClaimRequest(BaseModel):
    kind: ClaimKind
    subject: str = Field(min_length=1)
    statement: str = Field(min_length=1)
    asserted_value: str
    opened_by: str = Field(min_length=1)
    unit: str | None = None
    window_start: str | None = None
    window_end: str | None = None
    evidence: dict[str, str] = Field(default_factory=dict)


class TransitionRequest(BaseModel):
    action: ClaimAction
    actor: str = Field(min_length=1)
    note: str | None = None
    confirmed_by: str | None = None


class ExportRequest(BaseModel):
    claim_ids: list[str] = Field(min_length=1)


def _engine() -> ClaimEngine:
    return ClaimEngine.load(resolve_ledger_path())


def _refuse(exc: ClaimLifecycleError) -> HTTPException:
    status = (
        HTTPStatus.CONFLICT if isinstance(exc, ClaimNotLocked) else HTTPStatus.UNPROCESSABLE_ENTITY
    )
    return HTTPException(status, detail=str(exc))


@router.get("")
def list_claims(status: ClaimStatus | None = None, kind: ClaimKind | None = None) -> dict:
    """The claims queue — a reviewer's worklist, newest first."""
    claims = [
        claim.to_dict()
        for claim in _engine().claims.values()
        if (status is None or claim.status is status) and (kind is None or claim.kind is kind)
    ]
    claims.sort(key=lambda c: (c["updated_at"], c["claim_id"]), reverse=True)
    return {"schema": CLAIMS_SCHEMA, "claims": claims}


@router.get("/{claim_id}")
def claim_detail(claim_id: str) -> dict:
    engine = _engine()
    claim = engine.claims.get(claim_id)
    if claim is None:
        raise HTTPException(HTTPStatus.NOT_FOUND, detail=f"no claim {claim_id}")
    return {
        "claim": claim.to_dict(),
        "history": [t.to_dict() for t in engine.history.get(claim_id, [])],
    }


@router.post("", status_code=HTTPStatus.CREATED)
def open_claim(request: OpenClaimRequest) -> dict:
    try:
        claim = _engine().open(
            kind=request.kind,
            subject=request.subject,
            statement=request.statement,
            asserted_value=request.asserted_value,
            opened_by=request.opened_by,
            now=datetime.now(UTC),
            unit=request.unit,
            window_start=request.window_start,
            window_end=request.window_end,
            evidence=request.evidence,
        )
    except ClaimLifecycleError as exc:
        raise _refuse(exc) from exc
    return {"claim": claim.to_dict()}


@router.post("/{claim_id}/transitions")
def apply_transition(claim_id: str, request: TransitionRequest) -> dict:
    try:
        claim = _engine().apply(
            claim_id,
            request.action,
            actor=request.actor,
            now=datetime.now(UTC),
            note=request.note,
            confirmed_by=request.confirmed_by,
        )
    except ClaimLifecycleError as exc:
        raise _refuse(exc) from exc
    return {"claim": claim.to_dict()}


@router.post("/export")
def export_claims(request: ExportRequest) -> dict:
    try:
        result = _engine().export(request.claim_ids)
    except ClaimLifecycleError as exc:
        raise _refuse(exc) from exc
    return {
        "schema": result["package"]["schema"],
        "claims": result["package"]["claims"],
        "history": result["package"]["history"],
        "content_sha256": result["content_sha256"],
    }
