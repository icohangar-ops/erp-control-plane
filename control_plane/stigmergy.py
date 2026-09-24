"""Stigmergy board for agent-to-agent coordination (cubiczan-swarm-pack reference).

Stigmergy: agents coordinate by writing signals into a shared environment
instead of calling each other directly. Here the **connector-reconciliation
agent** (``orchestration/assets.py::delete_reconciliation``) and the
**exception agent** (``orchestration/exceptions.py::triage``) exchange typed
signals on one board, replacing the bespoke result plumbing that used to
carry reconciliation outcomes between them (ad-hoc dicts and parallel skip
lists folded into asset metadata).

Two properties matter more than the mechanism:

- **Typed signals with per-type half-lives.** Every signal decays: its
  salience halves once per kind-specific half-life (a reconciliation-run
  signal is per-run noise and fades in hours; a missing key scan is rare and
  stays relevant longer; an exception disposition lives longest). Consumers
  decide with *fresh* signals only — stale signals lose influence smoothly
  instead of switching off at an arbitrary TTL.
- **Determinism.** The module never reads the wall clock: every
  time-dependent method takes ``now`` explicitly. Signal ids are hashes of
  canonical content (posting the same signal twice is a no-op), reads are
  totally ordered, and sweeps drop signals below a fixed salience floor.
  Two boards fed the same events in the same order are identical.

The board is in-memory with a JSON snapshot form (``save_board`` /
``load_board``) so two Dagster assets in one job exchange through the
snapshot under the gitignored state tree — the same posture as the CHP
decision ledger. Persistence in the control-plane store is deliberately out
of scope: the store records reconciliation *facts*; the board carries
*coordination signals* between agents.
"""

from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any

BOARD_SCHEMA = "cubiczan-erp/stigmergy-board/v1"

#: Default snapshot location (under the gitignored state tree). Overridable
#: per call so tests use tmp_path and the Dagster assets can point at the
#: deployment's data root.
DEFAULT_BOARD_PATH = Path("./data/state/stigmergy_board.json")

#: Signals whose salience falls below the floor are swept. 2**-10 ≈ 0.001,
#: i.e. a signal survives ~10 half-lives of its kind before removal.
SALIENCE_FLOOR = 2.0**-10


class SignalKind(StrEnum):
    """The signal vocabulary the two agents exchange — closed on purpose."""

    #: One anti-join reconciliation run for a (source, entity) — per-run noise.
    RECONCILIATION_RUN = "reconciliation_run"
    #: A run tombstoned keys — the exception agent's triage input.
    TOMBSTONE_BATCH = "tombstone_batch"
    #: A source/entity was skipped: no key-inventory scan exists.
    KEY_SCAN_MISSING = "key_scan_missing"
    #: The exception agent's verdict on one exception (closes the loop).
    EXCEPTION_DISPOSITION = "exception_disposition"


#: Per-type half-lives in seconds. Run-level noise decays fastest; skip and
#: disposition signals persist because they change what a human must look at.
KIND_HALF_LIVES: dict[SignalKind, float] = {
    SignalKind.RECONCILIATION_RUN: 12 * 3600.0,
    SignalKind.TOMBSTONE_BATCH: 6 * 3600.0,
    SignalKind.KEY_SCAN_MISSING: 24 * 3600.0,
    SignalKind.EXCEPTION_DISPOSITION: 48 * 3600.0,
}


@dataclass(frozen=True)
class Signal:
    """One typed, decaying coordination signal on the board.

    ``signal_id`` is derived from the canonical content (kind, subject,
    payload, half-life) — the same event posted twice is the same signal, so
    ``post`` is idempotent and replays are stable. ``emitted_at`` is the one
    mutable-by-repost field: reposting the same content refreshes it.
    """

    signal_id: str
    kind: SignalKind
    subject: str  # e.g. "csvsftp_ridgeline/invoice_lines"
    payload: dict[str, Any]
    half_life_seconds: float
    emitted_at: datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "kind": self.kind.value,
            "subject": self.subject,
            "payload": self.payload,
            "half_life_seconds": self.half_life_seconds,
            "emitted_at": self.emitted_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> Signal:
        return cls(
            signal_id=str(raw["signal_id"]),
            kind=SignalKind(str(raw["kind"])),
            subject=str(raw["subject"]),
            payload=dict(raw["payload"]),
            half_life_seconds=float(raw["half_life_seconds"]),
            emitted_at=datetime.fromisoformat(str(raw["emitted_at"])),
        )


def _signal_id(kind: SignalKind, subject: str, payload: dict[str, Any]) -> str:
    """Deterministic id: sha256 over canonical JSON of the signal content."""
    canonical = json.dumps(
        {"kind": kind.value, "subject": subject, "payload": payload},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def salience(signal: Signal, now: datetime) -> float:
    """Exponential decay: 0.5 ** (age / half_life). Fresh = 1.0, never negative."""
    age_seconds = max(0.0, (now - signal.emitted_at).total_seconds())
    return 0.5 ** (age_seconds / signal.half_life_seconds)


@dataclass
class StigmergyBoard:
    """Shared signal board — post, read, decay, sweep. Deterministic by construction."""

    signals: dict[str, Signal] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def post(
        self,
        kind: SignalKind,
        subject: str,
        *,
        emitted_at: datetime,
        payload: dict[str, Any] | None = None,
    ) -> Signal:
        """Post (or refresh) a signal. Idempotent per content — same content,
        same id, refreshed emission time."""
        body = dict(payload or {})
        signal = Signal(
            signal_id=_signal_id(kind, subject, body),
            kind=kind,
            subject=subject,
            payload=body,
            half_life_seconds=KIND_HALF_LIVES[kind],
            emitted_at=emitted_at,
        )
        with self._lock:
            self.signals[signal.signal_id] = signal
        return signal

    def read(
        self,
        now: datetime,
        *,
        kinds: tuple[SignalKind, ...] | None = None,
        min_salience: float = 0.0,
    ) -> list[Signal]:
        """Alive signals (salience >= min_salience), deterministically ordered.

        Order: kind, then subject, then signal_id — a total order, so two
        readers see the same sequence and downstream code is reproducible.
        """
        wanted = set(kinds) if kinds is not None else set(SignalKind)
        with self._lock:
            alive = [
                s
                for s in self.signals.values()
                if s.kind in wanted and salience(s, now) >= min_salience
            ]
        return sorted(alive, key=lambda s: (s.kind.value, s.subject, s.signal_id))

    def sweep(self, now: datetime) -> int:
        """Drop signals below the salience floor. Returns the number swept."""
        with self._lock:
            dead = [sid for sid, s in self.signals.items() if salience(s, now) < SALIENCE_FLOOR]
            for sid in dead:
                del self.signals[sid]
        return len(dead)

    def snapshot(self, now: datetime) -> dict[str, Any]:
        """JSON-serializable board state (swept at ``now`` first — no zombie signals)."""
        self.sweep(now)
        with self._lock:
            signals = sorted(
                self.signals.values(), key=lambda s: (s.kind.value, s.subject, s.signal_id)
            )
        return {
            "schema": BOARD_SCHEMA,
            "signals": [s.to_dict() for s in signals],
        }


def save_board(board: StigmergyBoard, path: Path, now: datetime) -> Path:
    """Persist the board snapshot (canonical JSON) so the next agent — often a
    separate Dagster asset — reads exactly what this one wrote."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(board.snapshot(now), sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    return path


def load_board(path: Path) -> StigmergyBoard:
    """Load a board snapshot. A missing file is an EMPTY board, not an error —
    the first run of a job has no predecessor signals."""
    if not path.exists():
        return StigmergyBoard()
    raw = json.loads(path.read_text(encoding="utf-8"))
    schema = str(raw.get("schema", ""))
    if schema != BOARD_SCHEMA:
        raise ValueError(f"board snapshot {path} has schema {schema!r}, expected {BOARD_SCHEMA!r}")
    board = StigmergyBoard()
    for item in raw.get("signals", []):
        signal = Signal.from_dict(item)
        board.signals[signal.signal_id] = signal
    return board


def tombstone_ratio(tombstoned: int, warehouse_keys: int) -> float:
    """Share of the warehouse key set a reconciliation run tombstoned.

    Zero warehouse keys -> 0.0: an empty warehouse is never a glitch signal
    (mirrors the extract-side zero-row rule in connectors/base.py).
    """
    if warehouse_keys <= 0:
        return 0.0
    return tombstoned / warehouse_keys


def horizon_for(kind: SignalKind) -> timedelta:
    """The kind's ~10-half-life survival horizon (informational; sweep enforces)."""
    return timedelta(seconds=KIND_HALF_LIVES[kind] * 10)
