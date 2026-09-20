"""Honest provenance badges: LIVE -> CACHE -> MOCK three-tier resolution.

Every Superset / analytics KPI surface carries a provenance badge answering
one question: *which tier actually served this number?*

Resolution order (the tiered pattern):

1. **LIVE** — the value was computed at request time by the governed pipeline
   (guardrailed SQL against the read-only warehouse / dbt marts).
2. **CACHE** — the value is a previously computed real KPI served from cache
   (including a stale fallback when live computation fails — still a real
   number, honestly labeled with its age).
3. **MOCK** — a placeholder or synthetic value (fixture, generator, sample
   data), or a value whose provenance cannot be established.

Fail-closed honesty: a badge resolves to **MOCK whenever there is no positive
evidence of a real tier** — unknown provenance must never read as real — and
any mock evidence trumps a real-tier claim, because a number that is partly
mock is not a real KPI. ``MOCK_KPI_MARKER`` renders that state explicitly, and
:func:`assert_real` is the tripwire render paths call before presenting a
number as a KPI: a mock number must never render as a real KPI.

The module is pure (no I/O, no dependencies) and lives in the pip-installed
``control_plane`` package so the demo API, the GenBI layout, and the Superset
runbook script all share one source of badge truth.
"""

from __future__ import annotations

from dataclasses import dataclass

TIER_LIVE = "LIVE"
TIER_CACHE = "CACHE"
TIER_MOCK = "MOCK"

LIVE_KPI_MARKER = "[LIVE]"
CACHE_KPI_MARKER = "[CACHE]"
MOCK_KPI_MARKER = "[MOCK — NOT A REAL KPI]"

_REAL_TIERS = frozenset({TIER_LIVE, TIER_CACHE})
_MARKERS = {TIER_LIVE: LIVE_KPI_MARKER, TIER_CACHE: CACHE_KPI_MARKER, TIER_MOCK: MOCK_KPI_MARKER}


class MockKpiRenderError(Exception):
    """A MOCK-tier value was about to be presented as a real KPI."""


@dataclass(frozen=True)
class ProvenanceBadge:
    """The honest provenance of one KPI value (or KPI set)."""

    tier: str
    detail: str = ""

    @property
    def is_real(self) -> bool:
        """LIVE and CACHE are real numbers; MOCK is not."""
        return self.tier in _REAL_TIERS

    @property
    def marker(self) -> str:
        """The render-time marker; the MOCK marker is self-incriminating."""
        try:
            return _MARKERS[self.tier]
        except KeyError as exc:
            raise MockKpiRenderError(f"unknown provenance tier: {self.tier!r}") from exc


def resolve_badge(
    *,
    live: bool | None = None,
    cached: bool | None = None,
    mock: bool | None = None,
    detail: str = "",
) -> ProvenanceBadge:
    """Resolve which tier served the value; fail closed to MOCK.

    ``None`` means the evidence is unknown, which is never evidence of a real
    tier. Explicit ``mock=True`` beats any real-tier claim.
    """
    if mock or not (live or cached):
        fallback_detail = detail or "no positive evidence of a real tier — fail closed to MOCK"
        return ProvenanceBadge(TIER_MOCK, fallback_detail)
    if mock is None and live is None and cached is None:
        # Unreachable given the guard above, kept as an explicit honesty floor.
        return ProvenanceBadge(TIER_MOCK, "provenance unknown — fail closed to MOCK")
    if mock is None and live is None and cached:
        return ProvenanceBadge(TIER_CACHE, detail or "served from cache")
    if mock is None and cached is None and live:
        return ProvenanceBadge(TIER_LIVE, detail or "computed at request time")
    return ProvenanceBadge(TIER_MOCK, detail or "provenance unknown — fail closed to MOCK")


def assert_real(badge: ProvenanceBadge, *, context: str) -> ProvenanceBadge:
    """Tripwire: refuse to present a MOCK-tier value as a real KPI.

    Render paths call this right before emitting numbers; a caller that
    somehow resolved a mock value into a KPI slot fails loudly here instead of
    quietly publishing it.
    """
    if not badge.is_real:
        raise MockKpiRenderError(
            f"{context}: refusing to render a {badge.tier} value as a real KPI"
            f" ({badge.detail or 'no detail'})"
        )
    return badge
