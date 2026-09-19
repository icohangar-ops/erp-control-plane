"""A fake DB-API 2.0 connection: queues result sets, records executed SQL.

Each ``execute`` consumes the next (columns, rows) pair from the queue; the
``description`` tuple mimics what pyodbc/JDBC-bridge cursors expose. Tests
assert against ``executed`` to verify watermark predicates, bound parameters,
and soft-delete filters without any live driver.
"""

from __future__ import annotations

from typing import Any


class FakeDbApiCursor:
    def __init__(self, connection: FakeDbApiConnection) -> None:
        self._connection = connection
        self.description: list[tuple] | None = None
        self._results: list[tuple] = []

    def execute(self, operation: str, parameters: Any = None) -> None:
        self._connection.executed.append((operation, parameters))
        columns, rows = self._connection.next_result_sets.pop(0)
        self.description = [(name, None, None, None, None, None, None) for name in columns]
        self._results = rows

    def fetchall(self) -> list[tuple]:
        return self._results

    def close(self) -> None:
        pass


class FakeDbApiConnection:
    """Queue-based fake: result set i is returned by the i-th execute call."""

    def __init__(self, result_sets: list[tuple[list[str], list[tuple]]]) -> None:
        self.next_result_sets = list(result_sets)
        self.executed: list[tuple[str, Any]] = []

    def cursor(self) -> FakeDbApiCursor:
        return FakeDbApiCursor(self)

    def close(self) -> None:
        pass
