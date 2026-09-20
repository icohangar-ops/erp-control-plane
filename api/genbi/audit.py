"""Append-only audit trail: question -> SQL -> latency -> outcome (spec §1).

One JSON line per event under the gitignored runtime-state directory. The
mandatory entry is the execution record — every query the control plane runs
(including guardrail refusals) is logged before the caller sees the result.
Persistence events (chart created/updated) are logged with the same shape.
Production deployments point ``GENBI_AUDIT_PATH`` at durable storage.
"""

from __future__ import annotations

import datetime as dt
import json
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path

# Outcome vocabulary — exhaustive on purpose so dashboards over the audit log
# can rely on stable values.
EXECUTED = "executed"
GUARDRAIL_REJECTED = "guardrail_rejected"
CHP_REJECTED = "chp_rejected"
PROMOTED = "promoted"
UPDATED = "updated"
SUPERSET_ERROR = "superset_error"

STAGE_EXECUTE = "execute"
STAGE_CHP = "chp"
STAGE_PERSIST = "persist"


@dataclass
class AuditEntry:
    """One audit event: what was asked, what ran, how long it took, what happened."""

    question: str
    question_hash: str
    sql: str
    latency_ms: int
    outcome: str
    stage: str = STAGE_EXECUTE
    detail: str = ""
    rows_returned: int | None = None
    artifacts: dict[str, object] = field(default_factory=dict)
    entry_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = field(default_factory=lambda: dt.datetime.now(dt.UTC).isoformat())

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.entry_id,
            "timestamp": self.timestamp,
            "stage": self.stage,
            "question": self.question,
            "question_hash": self.question_hash,
            "sql": self.sql,
            "latency_ms": self.latency_ms,
            "outcome": self.outcome,
            "detail": self.detail,
            "rows_returned": self.rows_returned,
            "artifacts": self.artifacts,
        }


class AuditTrail:
    """Thread-safe JSONL append + newest-first listing."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()

    def append(self, entry: AuditEntry) -> None:
        line = json.dumps(entry.to_dict(), ensure_ascii=False)
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")

    def list(self, limit: int = 100) -> list[dict[str, object]]:
        """Newest-first audit entries (up to ``limit``); empty when never written."""
        with self._lock:
            if not self.path.exists():
                return []
            lines = self.path.read_text(encoding="utf-8").splitlines()
        entries = []
        for line in reversed(lines):
            if not line.strip():
                continue
            entries.append(json.loads(line))
            if len(entries) >= limit:
                break
        return entries
