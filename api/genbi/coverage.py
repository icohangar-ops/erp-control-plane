"""Persisted, listable coverage-request queue (spec §1, §4.1.6).

When the NL layer answers "not modeled yet", the question lands here instead of
vanishing: the queue is mined weekly and coverage ships on a published cadence
(the §1 mitigation for refusal fatigue). One record per distinct question —
repeat requests bump ``request_count`` rather than duplicating rows.
"""

from __future__ import annotations

import datetime as dt
import json
import threading
from pathlib import Path

from api.genbi.slugs import normalize_question, question_hash


def _now_iso() -> str:
    return dt.datetime.now(dt.UTC).isoformat()


class CoverageQueue:
    """Thread-safe JSONL store, one deduplicated record per distinct question."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()

    def _read_records(self) -> list[dict[str, object]]:
        if not self.path.exists():
            return []
        return [
            json.loads(line)
            for line in self.path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

    def _write_records(self, records: list[dict[str, object]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records)
        self.path.write_text(payload, encoding="utf-8")

    def record(self, question: str, reason: str = "not modeled yet") -> dict[str, object]:
        """Register a coverage request; repeat questions increment a counter."""
        normalized = normalize_question(question)
        question_hash_value = question_hash(question)
        with self._lock:
            records = self._read_records()
            for existing in records:
                if existing.get("question_hash") == question_hash_value:
                    existing["request_count"] = int(existing.get("request_count", 1)) + 1
                    existing["last_requested_at"] = _now_iso()
                    if reason and reason != existing.get("reason"):
                        existing["reasons"] = sorted(
                            {*existing.get("reasons", [existing.get("reason")]), reason}
                        )
                    self._write_records(records)
                    return existing
            fresh = {
                "question_hash": question_hash_value,
                "question": question,
                "normalized_question": normalized,
                "reason": reason,
                "reasons": [reason] if reason else [],
                "request_count": 1,
                "first_requested_at": _now_iso(),
                "last_requested_at": _now_iso(),
            }
            records.append(fresh)
            self._write_records(records)
            return fresh

    def list(self) -> list[dict[str, object]]:
        """All coverage requests, most-requested first (recency breaks ties).

        The count is the product signal the queue exists for — it ranks what
        coverage ships next; recency only orders questions asked equally often.
        """
        with self._lock:
            records = self._read_records()
        return sorted(
            records,
            key=lambda record: (
                int(record.get("request_count", 1)),
                str(record.get("last_requested_at")),
            ),
            reverse=True,
        )
