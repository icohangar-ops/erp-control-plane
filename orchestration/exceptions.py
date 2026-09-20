"""The exception agent: triages reconciliation signals off the stigmergy board.

This is the second of the two coordinated agents. The connector-reconciliation
agent (``orchestration/assets.py::delete_reconciliation``) posts
``RECONCILIATION_RUN`` / ``TOMBSTONE_BATCH`` / ``KEY_SCAN_MISSING`` signals;
this module reads them and decides, per subject, what happens next. The two
agents never call each other and share no result dicts — the board is the
only channel between them (control_plane/stigmergy.py).

Dispositions are the exception agent's own vocabulary:

- ``APPLY`` — a reconciliation run tombstoned a normal share of keys; the
  tombstones stand as hard deletes.
- ``REVIEW`` — a mass-tombstone run: the tombstoned share is at or above
  ``alert_ratio``. A truncated SFTP key-inventory drop looks exactly like a
  mass hard-delete; the exception agent holds it for human review instead of
  letting it stand silently.
- ``SKIP_LOUD`` — no key-inventory scan exists; the reconciliation was
  skipped and must stay visible (the control plane's "skipped loudly, never
  silently" posture, now typed).

Every triaged subject gets an ``EXCEPTION_DISPOSITION`` signal posted back to
the board — the loop closes through the board, so the next reconciliation run
can read prior dispositions without importing anything from this module's
callers. Pure functions throughout; ``now`` is always injected (deterministic).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from control_plane.stigmergy import SignalKind, StigmergyBoard

#: Share of warehouse keys tombstoned at or above which a run is held for
#: review instead of applied. The learner (control_plane/learner.py) owns the
#: tuning of this threshold; this constant is its starting value.
DEFAULT_ALERT_RATIO = 0.10

#: A flagged run whose tombstones are (mostly) real deletes was a false alarm;
#: one at or above this false-delete share counts as a caught glitch.
REAL_DELETE_FALSE_RATE = 0.5

DISPOSITION_APPLY = "APPLY"
DISPOSITION_REVIEW = "REVIEW"
DISPOSITION_SKIP_LOUD = "SKIP_LOUD"


@dataclass(frozen=True)
class Disposition:
    """The exception agent's verdict for one triaged subject."""

    subject: str
    disposition: str  # APPLY | REVIEW | SKIP_LOUD
    reason: str
    detail: dict[str, object]


def triage(
    board: StigmergyBoard,
    *,
    now: datetime,
    alert_ratio: float = DEFAULT_ALERT_RATIO,
) -> list[Disposition]:
    """Read fresh reconciliation signals, decide dispositions, post them back.

    Reads ``RECONCILIATION_RUN`` and ``KEY_SCAN_MISSING`` signals (the run
    signals already carry the tombstone ratio), posts one
    ``EXCEPTION_DISPOSITION`` signal per subject, and returns the dispositions
    in deterministic board-read order.
    """
    dispositions: list[Disposition] = []

    for signal in board.read(now, kinds=(SignalKind.KEY_SCAN_MISSING,)):
        dispositions.append(
            Disposition(
                subject=signal.subject,
                disposition=DISPOSITION_SKIP_LOUD,
                reason="no key-inventory scan for this source/entity",
                detail={"detail": signal.payload.get("detail", "")},
            )
        )

    for signal in board.read(now, kinds=(SignalKind.RECONCILIATION_RUN,)):
        ratio = float(signal.payload.get("tombstone_ratio", 0.0))
        tombstoned = int(signal.payload.get("tombstoned", 0))
        if ratio >= alert_ratio:
            dispositions.append(
                Disposition(
                    subject=signal.subject,
                    disposition=DISPOSITION_REVIEW,
                    reason=(
                        f"mass-tombstone run: {tombstoned} key(s) = {ratio:.1%} of warehouse "
                        f"keys >= alert ratio {alert_ratio:.1%} — suspected feed glitch, "
                        "held for review"
                    ),
                    detail={
                        "tombstoned": tombstoned,
                        "tombstone_ratio": ratio,
                        "alert_ratio": alert_ratio,
                    },
                )
            )
        elif tombstoned > 0:
            dispositions.append(
                Disposition(
                    subject=signal.subject,
                    disposition=DISPOSITION_APPLY,
                    reason=(
                        f"{tombstoned} key(s) = {ratio:.1%} of warehouse keys below alert "
                        f"ratio {alert_ratio:.1%} — tombstones stand as hard deletes"
                    ),
                    detail={
                        "tombstoned": tombstoned,
                        "tombstone_ratio": ratio,
                        "alert_ratio": alert_ratio,
                    },
                )
            )
        # ratio 0.0 / no tombstones: a clean run is not an exception — no signal.

    for disposition in dispositions:
        board.post(
            SignalKind.EXCEPTION_DISPOSITION,
            disposition.subject,
            emitted_at=now,
            payload={
                "disposition": disposition.disposition,
                "reason": disposition.reason,
                **disposition.detail,
            },
        )
    return dispositions


def review_fidelity(
    dispositions: list[Disposition],
    *,
    false_delete_rate: float,
) -> str:
    """Score a REVIEW disposition against the observed outcome for the subject.

    Given the false-delete share later observed for a flagged run (share of
    tombstoned keys that reappear in the next run's source inventory —
    ``control_plane/learner.py`` derives it from persisted runs), return
    ``CORRECT`` (the hold caught a real glitch) or ``FALSE_ALARM`` (the
    tombstones were genuine deletes). This is the judgment the learner's
    bounded updates act on; kept here so the semantics live with the agent.
    """
    flagged = any(d.disposition == DISPOSITION_REVIEW for d in dispositions)
    if flagged and false_delete_rate >= REAL_DELETE_FALSE_RATE:
        return "CORRECT"
    if flagged:
        return "FALSE_ALARM"
    return "UNFLAGGED"
