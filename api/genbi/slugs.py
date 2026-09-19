"""Deterministic identity for promoted answers (spec §4.2.4, risk #10).

Same question in, same identifiers out — the persistence loop is
create-or-update by slug, never blind insert. The date component comes from
the answer's promotion date (UTC), so same-day re-runs update in place.
"""

from __future__ import annotations

import datetime as dt
import hashlib


def normalize_question(question: str) -> str:
    """Lowercase and collapse whitespace so trivially different phrasings collide."""
    return " ".join(question.lower().split())


def question_hash(question: str) -> str:
    """Stable 12-hex identity of the question text."""
    return hashlib.sha256(normalize_question(question).encode("utf-8")).hexdigest()[:12]


def slug_for_question(question: str, answer_date: dt.date) -> str:
    """Chart/dashboard slug: ``genbi-<question-hash>-<YYYYMMDD>`` (spec §4.2.4)."""
    return f"genbi-{question_hash(question)}-{answer_date:%Y%m%d}"


def dataset_table_name(question: str) -> str:
    """Superset ``table_name`` for the governed dataset backing the answer.

    Superset table names are identifier-like: alphanumerics + underscore.
    """
    return f"genbi_q_{question_hash(question)}"


def genbi_row_id(question: str) -> str:
    """Dashboard grid row id for the question — stable across re-promotions."""
    return f"ROW-GENBI-{question_hash(question)}"


def genbi_column_id(question: str) -> str:
    """Dashboard grid column wrapper id for the question."""
    return f"COLUMN-GENBI-{question_hash(question)}"
