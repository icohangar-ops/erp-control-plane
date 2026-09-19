"""Guardrail tests: the NL path cannot write, multi-statement, overrun, or hang."""

from __future__ import annotations

import time
from pathlib import Path

import duckdb
import pytest

from api.genbi.guardrails import (
    ExecutionResult,
    MultipleStatements,
    NotReadOnlyUri,
    NotSelectOnly,
    QueryFailed,
    RowCapExceeded,
    StatementTimeout,
    execute_readonly,
    strip_comments,
    validate_select_only,
)
from genbi.connection import read_only_duckdb_uri


@pytest.fixture()
def analytics_file(tmp_path: Path) -> Path:
    path = tmp_path / "analytics.duckdb"
    con = duckdb.connect(str(path))
    con.execute(
        "create table dealer_revenue as select * from (values "
        "('BLDG', 120.0), ('ELEC', 90.0), ('TOOL', 60.0), ('PLMB', 40.0)) "
        "as t(branch_code, revenue)"
    )
    con.close()
    return path


@pytest.fixture()
def uri(analytics_file: Path) -> str:
    return read_only_duckdb_uri(analytics_file)


def test_select_passes(uri: str) -> None:
    result = execute_readonly(
        "select branch_code, sum(revenue) as revenue from dealer_revenue group by 1 order by 2 desc",
        uri=uri,
        timeout_seconds=10.0,
        row_cap=100,
    )
    assert isinstance(result, ExecutionResult)
    assert result.columns == ["branch_code", "revenue"]
    assert result.rows[0] == ("BLDG", 120.0)
    assert result.row_count == 4


def test_cte_passes(uri: str) -> None:
    result = execute_readonly(
        "with totals as (select sum(revenue) as total from dealer_revenue) "
        "select total from totals",
        uri=uri,
        timeout_seconds=10.0,
        row_cap=100,
    )
    assert result.rows[0][0] == 310.0


@pytest.mark.parametrize(
    "sql",
    [
        "insert into dealer_revenue values ('HACK', 1)",
        "update dealer_revenue set revenue = 0",
        "delete from dealer_revenue",
        "drop table dealer_revenue",
        "create table hacked as select 1",
        "alter table dealer_revenue add column x int",
        "copy dealer_revenue to 'out.csv'",
        "attach 'other.duckdb' as evil (READ_WRITE)",
        "pragma disable_verification",
        "set enable_progress_bar = true",
        "export database 'analytics.duckdb' to 'out'",
    ],
)
def test_write_and_engine_statements_rejected(uri: str, sql: str) -> None:
    with pytest.raises(NotSelectOnly):
        execute_readonly(sql, uri=uri, timeout_seconds=10.0, row_cap=100)


def test_forbidden_token_inside_column_name_allowed() -> None:
    # "created_at" and "description" contain forbidden substrings but are
    # single tokens — token-exact matching must not flag them.
    assert validate_select_only("select 'created_at' as description") == (
        "select 'created_at' as description"
    )


def test_multiple_statements_rejected(uri: str) -> None:
    with pytest.raises(MultipleStatements):
        validate_select_only("select 1; select 2")


def test_comment_smuggled_statement_rejected() -> None:
    # The smuggled statement hides behind a line comment; after stripping, the
    # interior semicolon (and the drop token) still trips validation.
    with pytest.raises((MultipleStatements, NotSelectOnly)):
        validate_select_only("select 1; -- select 2\ndrop table dealer_revenue")


def test_comment_forbidden_word_alone_is_not_flagged() -> None:
    # A forbidden word inside a comment must not fail a legitimate query.
    assert validate_select_only("select 1 -- drop table later") == "select 1"


def test_trailing_semicolon_allowed() -> None:
    assert validate_select_only("select 1;").startswith("select 1")


def test_empty_statement_rejected() -> None:
    with pytest.raises(NotSelectOnly):
        validate_select_only("/* nothing but a comment */")


def test_strip_comments_removes_both_forms() -> None:
    # Comments are replaced with whitespace (tokens stay separated); the
    # trailing "drop" is real statement text, so it survives.
    stripped = strip_comments("select /* block */ 1 -- line\ndrop")
    assert " ".join(stripped.split()) == "select 1 drop"


def test_non_duckdb_uri_rejected() -> None:
    with pytest.raises(NotReadOnlyUri):
        execute_readonly(
            "select 1", uri="postgresql://localhost/analytics", timeout_seconds=1, row_cap=10
        )


def test_read_write_uri_rejected(analytics_file: Path) -> None:
    with pytest.raises(NotReadOnlyUri):
        execute_readonly(
            "select 1",
            uri=f"duckdb:///{analytics_file}",
            timeout_seconds=10.0,
            row_cap=10,
        )


def test_row_cap_is_an_error_not_truncation(uri: str) -> None:
    with pytest.raises(RowCapExceeded):
        execute_readonly("select * from dealer_revenue", uri=uri, timeout_seconds=10.0, row_cap=2)


def test_row_cap_boundary_exactly_at_cap(uri: str) -> None:
    result = execute_readonly(
        "select * from dealer_revenue", uri=uri, timeout_seconds=10.0, row_cap=4
    )
    assert result.row_count == 4


def test_statement_timeout_interrupts_a_long_scan(uri: str) -> None:
    """A long-running scan is interrupted and the call fails fast.

    Uses an interruptible cross join rather than scalar sleep(5): DuckDB cannot
    break a scalar mid-call, only pipeline steps — the watchdog still raises
    StatementTimeout either way, but the interrupt path must be the fast one.
    """
    started = time.monotonic()
    with pytest.raises(StatementTimeout):
        execute_readonly(
            "select count(*) from range(1000000000) a, range(1000000000) b",
            uri=uri,
            timeout_seconds=0.3,
            row_cap=10,
        )
    assert time.monotonic() - started < 3.0, "timeout watchdog must not wait for the full query"


def test_statement_timeout_when_connect_hangs(tmp_path: Path) -> None:
    """If even the connection hangs, the watchdog still raises on deadline."""

    def hanging_connect(*args: object, **kwargs: object) -> duckdb.DuckDBPyConnection:
        time.sleep(30)
        raise AssertionError("never reached")  # pragma: no cover

    with pytest.raises(StatementTimeout):
        execute_readonly(
            "select 1",
            uri=read_only_duckdb_uri(tmp_path / "x.duckdb"),
            timeout_seconds=0.2,
            row_cap=10,
            connect=hanging_connect,  # type: ignore[arg-type]
        )


def test_engine_failure_surfaces_as_query_failed(uri: str) -> None:
    with pytest.raises(QueryFailed):
        execute_readonly("select * from no_such_table", uri=uri, timeout_seconds=10.0, row_cap=10)


def test_read_only_engine_refuses_writes_even_if_validation_bypassed(analytics_file: Path) -> None:
    """Deep defense: the connection itself is opened read_only, so a smuggled
    write fails at the engine even if the validator were bypassed."""
    con = duckdb.connect(str(analytics_file), read_only=True)
    with pytest.raises(duckdb.Error):
        con.execute("create table smuggled as select 1")
    con.close()
