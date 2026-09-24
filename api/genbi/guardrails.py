"""Guardrails for executed GenBI queries (spec §1, §4.1.4).

The control plane never trusts the NL layer: before an answer is persisted its
SQL is executed here, against the canonical READ_ONLY DuckDB URI, under four
enforcements:

1. SELECT-only — token-level validation after comment stripping; write/engine
   statements (INSERT, COPY, ATTACH, PRAGMA, SET, ...) are refused outright.
2. Single statement — interior semicolons are rejected (comment-smuggling safe).
3. Statement timeout — the query runs in a worker thread; on deadline the
   connection is ``interrupt()``-ed and the call fails.
4. Row cap — at most ``row_cap`` rows are fetched; exceeding the cap is an error,
   not a silent truncation.

Governance floor: ``ensure_read_only_uri`` fails closed unless the URI is a
DuckDB URI carrying ``access_mode=READ_ONLY`` (the constant lives in
``genbi/connection.py`` — the single source per the §2.3 same-options rule), and
the engine connection is additionally opened ``read_only=True`` so a smuggled
write cannot touch the file even if validation were bypassed.
"""

from __future__ import annotations

import datetime as dt
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import cast

import duckdb

from genbi.connection import URI_QUERY

# Statements that must never run from the NL path. Token-exact matching, so a
# column named ``description`` or ``created_at`` is not a false positive.
FORBIDDEN_TOKENS = frozenset(
    {
        "attach",
        "alter",
        "call",
        "checkpoint",
        "copy",
        "create",
        "delete",
        "detach",
        "drop",
        "export",
        "import",
        "insert",
        "install",
        "load",
        "merge",
        "pragma",
        "reset",
        "revoke",
        "grant",
        "set",
        "truncate",
        "update",
        "use",
        "vacuum",
    }
)

# DuckDB table functions can read arbitrary local files or remote resources
# even when the database connection itself is read-only. The NL surface is
# restricted to prebuilt relations; file/table functions are never allowed.
_FORBIDDEN_TABLE_FUNCTION = re.compile(
    r"\b(read_csv|read_csv_auto|read_parquet|parquet_scan|read_json|"
    r"read_json_auto|read_blob|glob|sqlite_scan|postgres_scan|mysql_scan|"
    r"httpfs|delta_scan|iceberg_scan)\s*\(",
    re.IGNORECASE,
)

_SELECT_PREFIX = re.compile(r"^(select|with)\b", re.IGNORECASE)
_TOKEN = re.compile(r"[a-zA-Z_]+")
_GRACE_SECONDS = 5.0  # wait for the worker to observe interrupt() before giving up


class GuardrailError(Exception):
    """Base class: a query violated a GenBI guardrail (or failed under them)."""


class NotReadOnlyUri(GuardrailError):
    """The configured DuckDB URI is not READ_ONLY — the NL path refuses to run."""


class NotSelectOnly(GuardrailError):
    """The statement is not a SELECT/WITH query or uses a forbidden keyword."""


class MultipleStatements(GuardrailError):
    """More than one SQL statement was submitted."""


class RowCapExceeded(GuardrailError):
    """The query returned more than the allowed number of rows."""

    def __init__(self, cap: int) -> None:
        super().__init__(f"query exceeds the GenBI row cap of {cap} rows")
        self.cap = cap


class StatementTimeout(GuardrailError):
    """The query exceeded the configured statement timeout."""

    def __init__(self, timeout_seconds: float) -> None:
        super().__init__(f"query exceeded the GenBI statement timeout of {timeout_seconds:g}s")
        self.timeout_seconds = timeout_seconds


class QueryFailed(GuardrailError):
    """The query failed at the engine (syntax, missing relation, read-only violation)."""


def strip_comments(sql: str) -> str:
    """Remove ``--`` line comments and ``/* */`` block comments."""
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", " ", sql)


def validate_select_only(sql: str) -> str:
    """Reject anything that is not a single SELECT/WITH statement.

    Returns the comment-stripped statement text. Raises NotSelectOnly or
    MultipleStatements.
    """
    stripped = strip_comments(sql).strip()
    if not stripped:
        raise NotSelectOnly("empty statement")
    if ";" in stripped.rstrip(";"):
        raise MultipleStatements("multiple SQL statements are not allowed")
    if not _SELECT_PREFIX.match(stripped):
        raise NotSelectOnly("only SELECT statements are allowed from the NL path")
    if _FORBIDDEN_TABLE_FUNCTION.search(stripped):
        raise NotSelectOnly("file and external table functions are not allowed")
    tokens = {match.group(0).lower() for match in _TOKEN.finditer(stripped)}
    forbidden = tokens & FORBIDDEN_TOKENS
    if forbidden:
        raise NotSelectOnly(f"forbidden keyword(s) in query: {', '.join(sorted(forbidden))}")
    return stripped


def ensure_read_only_uri(uri: str) -> str:
    """Fail closed unless ``uri`` is the canonical READ_ONLY DuckDB URI form.

    Returns the filesystem path extracted from the URI (for ``duckdb.connect``).
    """
    if not uri.startswith("duckdb:///"):
        raise NotReadOnlyUri(f"GenBI must query DuckDB, got: {uri.split('?')[0]}")
    path, _, query = uri[len("duckdb:///") :].partition("?")
    if URI_QUERY not in query.replace(" ", ""):
        raise NotReadOnlyUri("GenBI DuckDB connections must use access_mode=READ_ONLY")
    return path


@dataclass(frozen=True)
class ExecutionResult:
    """Outcome of a guardrailed execution against the read-only analytics file."""

    columns: list[str]
    rows: list[tuple]
    row_count: int
    latency_ms: int
    duckdb_uri: str


def execute_readonly(
    sql: str,
    *,
    uri: str,
    timeout_seconds: float,
    row_cap: int,
    connect: Callable[..., duckdb.DuckDBPyConnection] = duckdb.connect,
) -> ExecutionResult:
    """Execute ``sql`` under the guardrails and return its bounded result.

    ``connect`` is injectable so tests can exercise the timeout watchdog without
    a long-running real query. The engine connection is always opened
    ``read_only=True`` regardless of what the URI query string claims.
    """
    path = ensure_read_only_uri(uri)
    validate_select_only(sql)

    started = dt.datetime.now(dt.UTC)
    box: dict[str, object] = {}

    def worker() -> None:
        try:
            con = connect(path, read_only=True)
        except Exception as exc:
            box["error"] = exc
            return
        try:
            # Kept in the box so the watchdog thread can interrupt() on deadline.
            box["connection"] = con
            cur = con.execute(sql)
            columns = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchmany(row_cap + 1)
            box["columns"] = columns
            box["rows"] = rows
        except Exception as exc:
            box["error"] = exc

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    thread.join(timeout_seconds)
    if thread.is_alive():
        connection = box.get("connection")
        if isinstance(connection, duckdb.DuckDBPyConnection):
            connection.interrupt()
            thread.join(_GRACE_SECONDS)
        raise StatementTimeout(timeout_seconds)
    latency_ms = int((dt.datetime.now(dt.UTC) - started).total_seconds() * 1000)
    if "error" in box:
        raise QueryFailed(f"query failed under guardrails: {box['error']}") from cast(
            Exception, box["error"]
        )
    rows = cast(list, box.get("rows", []))
    if len(rows) > row_cap:
        raise RowCapExceeded(row_cap)
    return ExecutionResult(
        columns=cast("list[str]", box.get("columns", [])),
        rows=rows,
        row_count=len(rows),
        latency_ms=latency_ms,
        duckdb_uri=uri,
    )
