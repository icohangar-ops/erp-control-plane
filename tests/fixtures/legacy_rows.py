"""Canonical fixture rows generated from the shared entity schemas.

Rows are derived from ``CANONICAL_ENTITY_COLUMNS`` so a schema change breaks
the fixture factory at test-collection time instead of silently drifting.
Values are rendered per canonical kind — real ODBC/JDBC drivers hand back
typed objects (Decimal, int), not strings, so the fixture does too.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from connectors.legacy.schemas import CANONICAL_ENTITY_COLUMNS, NATURAL_KEY_FIELDS

_SAMPLE_VALUES: dict[str, str] = {
    "string": "ACME",
    "decimal": "10.5",
    "integer": "2",
    "date": "2026-08-01",
    "timestamp": "2026-08-01T10:00:00",
}


def _typed(kind: str, value: str) -> Any:
    """Render like a real driver (and the CSV casts): typed, not stringly."""
    if kind == "decimal":
        return Decimal(value)
    if kind == "integer":
        return int(value)
    if kind == "date":
        return date.fromisoformat(value)
    if kind == "timestamp":
        return datetime.fromisoformat(value)
    return value


def canonical_row(entity: str, index: int = 0, **overrides: Any) -> dict[str, Any]:
    """One canonical row for ``entity`` — key fields suffixed by ``index``."""
    row: dict[str, Any] = {}
    for name, kind in CANONICAL_ENTITY_COLUMNS[entity]:
        value: Any = _SAMPLE_VALUES[kind]
        if name in NATURAL_KEY_FIELDS[entity]:
            if name == "line_no":
                value = index
            else:
                value = f"{name.upper().replace('_NO', '').replace('_CODE', '')}-{index:04d}"
        row[name] = value
    row.update(overrides)
    return row


def result_set(
    entity: str, rows: list[dict[str, Any]], columns: tuple[tuple[str, str], ...] | None = None
) -> tuple[list[str], list[tuple]]:
    """Render canonical rows as (description columns, DB-API rows) for a fake cursor.

    ``columns`` is the EntitySource map (canonical, source) — ``None`` means
    identity. Values are typed per canonical kind, as a real driver returns.
    """
    pairs = columns or [(name, name) for name, _ in CANONICAL_ENTITY_COLUMNS[entity]]
    kinds = dict(CANONICAL_ENTITY_COLUMNS[entity])
    return (
        [source for _, source in pairs],
        [
            tuple(_typed(kinds[canonical], str(row[canonical])) for canonical, _ in pairs)
            for row in rows
        ],
    )
