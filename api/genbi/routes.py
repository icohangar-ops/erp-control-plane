"""FastAPI routes for the GenBI answer-promotion loop (spec §4.2).

- ``POST /api/v1/genbi/answers/promote``    — NL answer -> governed Superset chart
- ``POST /api/v1/genbi/coverage-requests``  — record a "not modeled yet" (spec §1)
- ``GET  /api/v1/genbi/coverage-requests``  — the listable coverage queue
- ``GET  /api/v1/genbi/audit``              — question -> SQL -> latency -> outcome

Services are built per request from the environment (Vercel-friendly); tests
override the Superset transport via httpx and point the state paths at tmp dirs.
"""

from __future__ import annotations

import datetime as dt
from http import HTTPStatus
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from api.genbi.audit import AuditTrail
from api.genbi.config import GenbiSettings, NotConfigured
from api.genbi.coverage import CoverageQueue
from api.genbi.guardrails import GuardrailError, NotReadOnlyUri
from api.genbi.promote import PromotionService
from api.genbi.superset import SupersetClient, SupersetError
from api.genbi.viz import BackingSpec, VizSpec

router = APIRouter(prefix="/api/v1/genbi", tags=["genbi"])


class PromoteRequest(BaseModel):
    """An NL answer to persist: the question, its governed SQL, and the chart."""

    question: str = Field(min_length=1)
    sql: str = Field(min_length=1)
    viz: VizSpec
    answer_date: dt.date | None = Field(
        default=None, description="Defaults to today (UTC); part of the deterministic slug."
    )
    backing: BackingSpec | None = Field(
        default=None,
        description="Physical governed backing (schema+table); omit for a virtual dataset over the answer SQL.",
    )


class CoverageRequestIn(BaseModel):
    question: str = Field(min_length=1)
    reason: str = "not modeled yet"


def build_service(env: dict[str, str] | None = None) -> PromotionService:
    """Assemble the promotion service from environment settings."""
    settings = GenbiSettings.from_env(env)
    return PromotionService(
        settings=settings,
        superset=SupersetClient(
            settings.superset_url,
            settings.superset_user,
            settings.superset_password,
        ),
        audit=AuditTrail(settings.audit_path),
        coverage=CoverageQueue(settings.coverage_path),
    )


@router.post("/answers/promote")
def promote_answer(request: PromoteRequest) -> dict[str, Any]:
    """Persist an NL answer as a governed dataset + chart on the Ask → Save dashboard."""
    service = build_service()
    try:
        return service.promote(
            request.question,
            request.sql,
            request.viz,
            answer_date=request.answer_date,
            backing=request.backing,
        )
    except NotConfigured as exc:
        raise HTTPException(HTTPStatus.SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    except NotReadOnlyUri as exc:
        # A governance misconfiguration, not caller error — the loop refuses to run.
        raise HTTPException(HTTPStatus.SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    except GuardrailError as exc:
        raise HTTPException(HTTPStatus.UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    except SupersetError as exc:
        raise HTTPException(HTTPStatus.BAD_GATEWAY, detail=str(exc)) from exc


@router.post("/coverage-requests", status_code=201)
def create_coverage_request(request: CoverageRequestIn) -> dict[str, Any]:
    """Feed a "not modeled yet" response into the persisted coverage queue (spec §1)."""
    return build_service().coverage.record(request.question, request.reason)


@router.get("/coverage-requests")
def list_coverage_requests() -> list[dict[str, Any]]:
    """The deduplicated coverage queue, most-requested first (recency breaks ties)."""
    return build_service().coverage.list()


@router.get("/audit")
def list_audit(limit: int = 100) -> list[dict[str, Any]]:
    """Newest-first audit trail: question -> SQL -> latency -> outcome."""
    return build_service().audit.list(limit)
