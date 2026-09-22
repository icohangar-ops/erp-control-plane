"""Fixture-based tests for the BisTrack connector (spec art_mWnBjp7j).

The ODBC path is coded against its spec-verified surfaces (OrderHeader/
OrderLine, InvoiceHeader/InvoiceLine) but is UNEXERCISED against a live
BisTrack site. These tests prove the documented behaviors — per-document-type
numbering watermarks, keyset paging, UOM/branch carriage, quarantine, the §6
anti-join, and disabled-first gating — through the real extraction machinery
with an injected fake DB-API connection (the MockTransport analog for the
database surface). No ODBC driver and no live SQL Server is ever touched.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from connectors.base import (
    ConnectorError,
    ConnectorMaturity,
    ConnectorNotConfigured,
    ConnectorNotImplemented,
    DeleteSemantics,
    ExtractionMode,
)
from connectors.bistrack.connector import _DOCUMENT_ENTITIES, BisTrackConnector
from connectors.legacy.schemas import CANONICAL_ENTITY_COLUMNS
from connectors.registry import load_source_configs
from control_plane.config import ControlPlaneConfig
from control_plane.models import SourceConfig, SyncCheckpoint
from control_plane.store import SqliteControlPlaneStore
from tests.fixtures.fake_dbapi import FakeDbApiConnection
from tests.fixtures.legacy_rows import _typed, canonical_row

BT_SETTINGS = {
    "mode": "odbc",
    "odbc_dsn": "bistrack-fixture",
    "db_user": "svc_ctlplane_ro",
    "db_password": "fixture-password",
    "order_document_types": "ORDER",  # most tests exercise a single type scan
    "invoice_document_types": "",
}

SALES = "sales_order_lines"
INVOICES = "invoice_lines"


def _connector(
    tmp_path: Path, connection: FakeDbApiConnection, settings: dict[str, str] | None = None
) -> BisTrackConnector:
    """One connector instance wired to a tmp control plane and a fake DB-API connection."""
    config = ControlPlaneConfig(
        backend="sqlite",
        sqlite_path=tmp_path / "cp.db",
        control_plane_dsn=None,
        lake_root=tmp_path / "lake",
        analytics_duckdb_path=tmp_path / "analytics.duckdb",
        quarantine_root=tmp_path / "quarantine",
        environment="test",
    )
    store = SqliteControlPlaneStore(tmp_path / "cp.db")
    store.initialize()
    source = SourceConfig(
        source_id="bistrack_template",
        erp="bistrack",
        description="fixture source",
        settings={**BT_SETTINGS, **(settings or {})},
        enabled=False,
    )
    return BisTrackConnector(source, store, config, connection_factory=lambda _settings: connection)


def _doc_row(entity: str, index: int, doc_type: str, **overrides: object) -> dict[str, object]:
    row = canonical_row(entity, index, **overrides)
    row[_DOCUMENT_ENTITIES[entity].type_field] = doc_type
    return row


def _scan_connection(entity: str, *batches: list[dict[str, object]]) -> FakeDbApiConnection:
    """Queue one result set per expected page, in document-scan SELECT order.

    Values are typed per canonical kind via the shared fixture typer — a real
    ODBC driver hands back typed objects (int, Decimal, date), not strings.
    """
    spec = _DOCUMENT_ENTITIES[entity]
    names = [*spec.column_names, spec.type_field]
    kinds = dict(CANONICAL_ENTITY_COLUMNS[entity])
    pages = [
        (
            list(names),
            [
                (
                    *[
                        _typed(kinds.get(name, "string"), str(row[name]))
                        for name in spec.column_names
                    ],
                    row[spec.type_field],
                )
                for row in batch
            ],
        )
        for batch in batches
    ]
    return FakeDbApiConnection(pages)


def _incremental(connector: BisTrackConnector, entity: str):
    return connector.extract(entity, mode=ExtractionMode.INCREMENTAL)


def _stored_watermark(connector: BisTrackConnector, entity: str) -> dict[str, str]:
    raw = connector.store.get_watermark("bistrack_template", entity, "incremental")
    return {} if raw is None else json.loads(raw)


# ---------------------------------------------------------------------------
# Extraction plans (derived without a connection, spec §3.1/§3.2)
# ---------------------------------------------------------------------------


def test_odbc_extraction_plans_are_derived_from_the_spec(tmp_path: Path) -> None:
    connector = _connector(tmp_path, FakeDbApiConnection([]))
    assert connector.maturity is ConnectorMaturity.IMPLEMENTED

    plan = connector.dry_run()
    assert plan["source_id"] == "bistrack_template"
    assert plan["maturity"] == "implemented"
    assert not connector.validate_config()  # fixture settings resolve

    entities = {entry["entity"]: entry for entry in plan["entities"]}
    order = entities[SALES]
    assert "OrderLine l JOIN OrderHeader h" in order["surface"]
    assert "keyset pages of 500" in order["surface"]
    assert "per h.order_type value" in order["incremental_key"]
    # spec §3.2: the invoice surface maps the two public invoice tables
    assert "InvoiceHeader" in entities[INVOICES]["surface"]
    assert "InvoiceLine" in entities[INVOICES]["surface"]
    # spec §3.2 [D]: every other entity stays a plan until onboarding pins tables
    for entity in (
        "items",
        "customers",
        "vendors",
        "purchase_order_lines",
        "inventory_snapshots",
        "gl_entries",
    ):
        assert "discovery pack" in entities[entity]["surface"], entity


def test_smartview_mode_stays_a_documented_plan(tmp_path: Path) -> None:
    connector = _connector(
        tmp_path,
        FakeDbApiConnection([]),
        settings={
            "mode": "smartview",
            "smartview_base_url": "https://bt.fixture",
            "smartview_api_key": "fixture-key",
        },
    )
    assert connector.maturity is ConnectorMaturity.SKELETON

    plan = connector.dry_run()
    entities = {entry["entity"]: entry for entry in plan["entities"]}
    assert set(entities) == set(connector.entities())
    for entity, entry in entities.items():
        assert "separately licensed BisTrack API" in entry["surface"], entity
    # financial data exchange is a documented API group (spec §3.2 row 10)
    assert "financial data exchange" in entities["gl_entries"]["surface"]

    with pytest.raises(ConnectorNotImplemented, match="separately licensed"):
        connector.extract(SALES)
    with pytest.raises(ConnectorNotImplemented, match="separately licensed"):
        connector.source_key_inventory(SALES)


# ---------------------------------------------------------------------------
# Disabled-first gating (no credentials, no connection, no registration)
# ---------------------------------------------------------------------------


def test_disabled_first_without_credentials(tmp_path: Path) -> None:
    """Without credentials the adapter validates to an honest problem list,
    refuses registration and extraction before any connection attempt, and the
    registry template resolves to exactly that state."""
    attempted: list[tuple] = []
    connector = _connector(
        tmp_path,
        FakeDbApiConnection([]),
        settings={  # empty settings: no dsn, no login, no document types
            "mode": "odbc",
            "odbc_dsn": "",
            "db_user": "",
            "db_password": "",
            "order_document_types": "",
            "invoice_document_types": "",
        },
    )

    # stand in a factory that fails loudly if it is ever called
    def factory(settings):
        attempted.append(settings)
        raise AssertionError("connection attempted on an unconfigured source")

    connector._connection_factory = factory

    problems = connector.validate_config()
    assert problems
    assert "odbc_dsn" in problems[0]
    assert any("order_document_types" in problem for problem in problems)

    with pytest.raises(ConnectorNotConfigured):
        connector.register()
    with pytest.raises(ConnectorNotConfigured):
        connector.extract("items")
    with pytest.raises(ConnectorNotConfigured):
        connector.source_key_inventory(SALES)
    assert attempted == []  # no connection was ever attempted

    # the registry template resolves all ${VAR:-} to empty and stays disabled
    source = next(s for s in load_source_configs() if s.source_id == "bistrack_template")
    assert source.enabled is False
    assert source.settings["odbc_dsn"] == ""
    assert source.settings["order_document_types"] == ""
    template = _connector(tmp_path, FakeDbApiConnection([]), settings=source.settings)
    assert template.validate_config()  # honestly unconfigurable, exactly like the fixture


def test_invalid_mode_reports_a_problem(tmp_path: Path) -> None:
    connector = _connector(tmp_path, FakeDbApiConnection([]), settings={"mode": "pigeon"})
    problems = connector.validate_config()
    assert problems and "pigeon" in problems[0]


def test_smartview_without_credentials_reports_its_own_gaps(tmp_path: Path) -> None:
    connector = _connector(tmp_path, FakeDbApiConnection([]), settings={"mode": "smartview"})
    problems = connector.validate_config()
    assert problems and "smartview_base_url" in problems[0]


# ---------------------------------------------------------------------------
# Backfill ingestion: fixture rowsets -> typed Parquet with provenance
# ---------------------------------------------------------------------------


def test_backfill_ingests_fixture_rowsets_and_stamps_provenance(tmp_path: Path) -> None:
    rows = [_doc_row(SALES, 1, "ORDER"), _doc_row(SALES, 2, "ORDER")]
    conn = _scan_connection(SALES, rows)
    connector = _connector(tmp_path, conn)

    result = connector.extract(SALES)

    assert result.rows_extracted == 2
    assert result.mode == "backfill"
    # one keyset-paged scan for the configured document type
    sql, params = conn.executed[0]
    assert "FROM OrderLine l JOIN OrderHeader h ON h.order_no = l.order_no" in sql
    assert "h.order_type = ?" in sql  # spec §7.1: quotes are never ingested as orders
    assert params == ("ORDER",)
    assert "OFFSET 0 ROWS FETCH NEXT 500 ROWS ONLY" in sql

    table = pq.read_table(result.parquet_path)
    assert table.num_rows == 2
    assert {"source_system", "source_id", "loaded_at"}.issubset(set(table.column_names))
    assert set(table.column("source_system").to_pylist()) == {"bistrack"}
    # natural keys carry the document type as scope: per-type numbering
    # sequences repeat numbers across types (spec §7.1), so unscoped keys collide
    assert set(table.column("source_id").to_pylist()) == {
        "ORDER:ORDER-0001:1",
        "ORDER:ORDER-0002:2",
    }
    # branch partitioning (spec §7.4): branch_code rides every staged row
    assert set(table.column("branch_code").to_pylist()) == {"ACME"}


def test_uom_carries_source_units_without_normalization(tmp_path: Path) -> None:
    """spec pitfall §7.3: dealers buy in one UOM and sell in another — the
    source UOM rides every line and quantities/prices stage raw."""
    rows = [
        _doc_row(
            SALES,
            1,
            "ORDER",
            uom="MBF",
            ordered_qty=2,
            filled_qty=1,
            unit_price=Decimal("415.00"),
            unit_cost=Decimal("380.00"),
        ),
        _doc_row(
            SALES,
            2,
            "ORDER",
            uom="LF",
            ordered_qty=48,
            filled_qty=48,
            unit_price=Decimal("0.95"),
            unit_cost=Decimal("0.72"),
        ),
    ]
    conn = _scan_connection(SALES, rows)
    connector = _connector(tmp_path, conn)

    result = connector.extract(SALES)

    table = pq.read_table(result.parquet_path).sort_by("source_id")
    uoms = table.column("uom").to_pylist()
    assert uoms == ["MBF", "LF"]  # two different source UOMs, both carried
    assert table.column("ordered_qty").to_pylist() == [2, 48]  # raw, unconverted
    assert table.column("unit_price").to_pylist() == [Decimal("415.00"), Decimal("0.95")]
    assert table.column("unit_cost").to_pylist() == [Decimal("380.00"), Decimal("0.72")]


def test_unmapped_entity_raises_until_onboarding_pins_tables(tmp_path: Path) -> None:
    connector = _connector(tmp_path, FakeDbApiConnection([]))
    with pytest.raises(ConnectorNotImplemented, match="discovery pack"):
        connector.extract("items")


# ---------------------------------------------------------------------------
# Per-type numbering watermarks (spec §4.2 + §7.1)
# ---------------------------------------------------------------------------


def test_first_incremental_run_scans_full_history_per_type(tmp_path: Path) -> None:
    conn = _scan_connection(
        SALES,
        [_doc_row(SALES, 1, "ORDER"), _doc_row(SALES, 2, "ORDER")],
        [_doc_row(SALES, 1, "SPECIAL ORDER")],
    )
    connector = _connector(tmp_path, conn, settings={"order_document_types": "ORDER,SPECIAL ORDER"})

    result = _incremental(connector, SALES)

    # first incremental run: no stored checkpoint, so both type scans are full
    assert conn.executed[0][1] == ("ORDER",)
    assert conn.executed[1][1] == ("SPECIAL ORDER",)
    assert result.rows_extracted == 3
    assert json.loads(result.watermark_after or "{}") == {
        "ORDER": "ORDER-0002",
        "SPECIAL ORDER": "ORDER-0001",
    }
    assert _stored_watermark(connector, SALES) == json.loads(result.watermark_after)


def test_watermarks_advance_per_type_across_batches(tmp_path: Path) -> None:
    """A checkpoint stored per document type: each scan binds only its own type's
    maximum, and observing one type never advances the other."""
    connector = _connector(tmp_path, FakeDbApiConnection([]))
    connector.store.upsert_watermark(
        SyncCheckpoint(
            source_id="bistrack_template",
            entity=SALES,
            mode="incremental",
            watermark=json.dumps({"ORDER": "ORDER-0002", "SPECIAL ORDER": "ORDER-0001"}),
            updated_at=datetime.now(UTC),
        )
    )
    conn = _scan_connection(
        SALES,
        [_doc_row(SALES, 3, "ORDER")],  # ORDER advances to 0003
        [],  # SPECIAL ORDER: nothing new
    )
    connector = _connector(tmp_path, conn, settings={"order_document_types": "ORDER,SPECIAL ORDER"})

    result = _incremental(connector, SALES)

    # each scan binds its own stored maximum (type filter + per-type watermark)
    assert conn.executed[0][1] == ("ORDER", "ORDER-0002")
    assert conn.executed[1][1] == ("SPECIAL ORDER", "ORDER-0001")
    assert result.rows_extracted == 1
    assert json.loads(result.watermark_after or "{}") == {
        "ORDER": "ORDER-0003",
        "SPECIAL ORDER": "ORDER-0001",  # untouched type keeps its maximum
    }


def test_incremental_restart_at_checkpoint_is_a_no_op(tmp_path: Path) -> None:
    conn = _scan_connection(
        SALES,
        [_doc_row(SALES, 1, "ORDER"), _doc_row(SALES, 2, "ORDER")],
        [_doc_row(SALES, 1, "SPECIAL ORDER")],
    )
    first = _connector(tmp_path, conn, settings={"order_document_types": "ORDER,SPECIAL ORDER"})
    _incremental(first, SALES)
    checkpoint = _stored_watermark(first, SALES)

    # restart at the checkpoint: both scans return nothing, nothing regresses
    conn2 = _scan_connection(SALES, [], [])
    second = _connector(tmp_path, conn2, settings={"order_document_types": "ORDER,SPECIAL ORDER"})
    second.store.upsert_watermark(
        SyncCheckpoint(
            source_id="bistrack_template",
            entity=SALES,
            mode="incremental",
            watermark=json.dumps(checkpoint),
            updated_at=datetime.now(UTC),
        )
    )

    result = _incremental(second, SALES)

    assert result.rows_extracted == 0
    assert result.watermark_before == json.dumps(checkpoint)
    assert result.watermark_after == result.watermark_before
    assert _stored_watermark(second, SALES) == checkpoint


def test_document_numbers_compare_numerically(tmp_path: Path) -> None:
    """Unpadded per-type sequences: 1000 must beat 999 (a lexical max would
    strand everything above 999)."""
    conn = _scan_connection(
        SALES,
        [_doc_row(SALES, 1, "ORDER", order_no="999"), _doc_row(SALES, 2, "ORDER", order_no="1000")],
    )
    connector = _connector(tmp_path, conn)

    result = _incremental(connector, SALES)

    assert json.loads(result.watermark_after or "{}") == {"ORDER": "1000"}


def test_invoice_scans_run_unscoped_when_no_type_list_is_configured(tmp_path: Path) -> None:
    """Invoices may scan unscoped (key "*"): one site-global numbering sequence
    assumed [spec §4.2 I] — the safe per-type path stays available."""
    rows = [_doc_row(INVOICES, 1, "STANDARD INV"), _doc_row(INVOICES, 2, "STANDARD INV")]
    conn = _scan_connection(INVOICES, rows)
    connector = _connector(tmp_path, conn)

    result = _incremental(connector, INVOICES)

    sql, params = conn.executed[0]
    assert "FROM InvoiceLine l JOIN InvoiceHeader h ON h.invoice_no = l.invoice_no" in sql
    assert params is None  # no type filter, no watermark on the first run
    assert json.loads(result.watermark_after or "{}") == {"*": "INVOICE-0002"}
    table = pq.read_table(result.parquet_path)
    assert set(table.column("source_id").to_pylist()) == {
        "STANDARD INV:INVOICE-0001:1",
        "STANDARD INV:INVOICE-0002:2",
    }


# ---------------------------------------------------------------------------
# Keyset paging (spec §6.4)
# ---------------------------------------------------------------------------


def test_keyset_paging_advances_by_predicate_never_offset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(BisTrackConnector, "PAGE_SIZE", 2)
    rows = [_doc_row(SALES, 1, "ORDER"), _doc_row(SALES, 2, "ORDER"), _doc_row(SALES, 3, "ORDER")]
    conn = _scan_connection(SALES, rows[:2], rows[2:])
    connector = _connector(tmp_path, conn)

    result = _incremental(connector, SALES)

    assert result.rows_extracted == 3
    assert len(conn.executed) == 2  # two pages of two and one rows
    first_sql, first_params = conn.executed[0]
    assert "OFFSET 0 ROWS FETCH NEXT 2 ROWS ONLY" in first_sql
    assert first_params == ("ORDER",)  # type filter only on page one
    second_sql, second_params = conn.executed[1]
    # the cursor rides the predicate: strictly after page one's last (doc, line)
    assert "(h.order_no > ? OR (h.order_no = ? AND l.line_no > ?))" in second_sql
    assert second_params == ("ORDER", "ORDER-0002", "ORDER-0002", 2)
    assert "OFFSET 2" not in second_sql  # never a sliding OFFSET window


# ---------------------------------------------------------------------------
# Fail-closed behavior: quarantine, corrupt checkpoints, refused connections
# ---------------------------------------------------------------------------


def test_malformed_row_quarantines_and_fails_closed(tmp_path: Path) -> None:
    """A stale column map (short row) quarantines with a machine-readable reason
    and fails the run — never a silent drop or a partial promote."""
    good = _doc_row(SALES, 1, "ORDER")
    spec = _DOCUMENT_ENTITIES[SALES]
    names = [*spec.column_names, spec.type_field]
    short = (*[good[c] for c in spec.column_names][:-1], "ORDER")  # one column short
    conn = FakeDbApiConnection([(list(names), [short])])
    connector = _connector(tmp_path, conn)

    with pytest.raises(ConnectorError, match="column map is stale"):
        connector.extract(SALES)

    quarantined = connector.store.list_quarantine("bistrack_template")
    assert len(quarantined) == 1
    record = quarantined[0]
    assert record.reason_code == "MALFORMED_ROW"
    assert "column map is stale" in record.detail
    payload = json.loads(Path(record.quarantine_path).read_text(encoding="utf-8"))
    assert payload["columns"] == names
    assert len(payload["row"]) == len(names) - 1  # the offending shape is preserved
    assert connector.store.get_watermark("bistrack_template", SALES, "backfill") is None


def test_null_natural_key_quarantines_and_fails_closed(tmp_path: Path) -> None:
    good = _doc_row(SALES, 1, "ORDER")
    spec = _DOCUMENT_ENTITIES[SALES]
    names = [*spec.column_names, spec.type_field]
    bad = (*[good[c] for c in spec.column_names], "ORDER")
    bad = (*bad[: names.index("line_no")], None, *bad[names.index("line_no") + 1 :])
    conn = FakeDbApiConnection([(list(names), [bad])])
    connector = _connector(tmp_path, conn)

    with pytest.raises(ConnectorError, match="natural key fields"):
        connector.extract(SALES)

    quarantined = connector.store.list_quarantine("bistrack_template")
    assert len(quarantined) == 1 and "line_no" in quarantined[0].detail


def test_refused_connection_fails_closed_without_checkpoint(tmp_path: Path) -> None:
    config = ControlPlaneConfig(
        backend="sqlite",
        sqlite_path=tmp_path / "cp.db",
        control_plane_dsn=None,
        lake_root=tmp_path / "lake",
        analytics_duckdb_path=tmp_path / "analytics.duckdb",
        quarantine_root=tmp_path / "quarantine",
        environment="test",
    )
    store = SqliteControlPlaneStore(tmp_path / "cp.db")
    store.initialize()
    source = SourceConfig(
        source_id="bistrack_template",
        erp="bistrack",
        description="fixture source",
        settings=dict(BT_SETTINGS),
        enabled=False,
    )

    def refusing_factory(_settings: dict[str, str]) -> object:
        raise ConnectionRefusedError("driver refused the DSN")

    connector = BisTrackConnector(source, store, config, connection_factory=refusing_factory)

    with pytest.raises(ConnectorError, match="connection failed"):
        connector.extract(SALES)

    assert store.get_watermark("bistrack_template", SALES, "backfill") is None
    assert store.list_quarantine("bistrack_template") == []  # no payload was ever read


def test_corrupt_stored_watermark_refuses_to_scan(tmp_path: Path) -> None:
    """A checkpoint that fails to parse is a corrupt store, never a filter to
    guess at: refuse before building any SQL (NetSuite watermark precedent)."""
    conn = _scan_connection(SALES, [])
    connector = _connector(tmp_path, conn)
    connector.store.upsert_watermark(
        SyncCheckpoint(
            source_id="bistrack_template",
            entity=SALES,
            mode="incremental",
            watermark="not json",
            updated_at=datetime.now(UTC),
        )
    )

    with pytest.raises(ConnectorError, match="unparseable"):
        _incremental(connector, SALES)

    assert conn.executed == []  # nothing was scanned off a corrupt checkpoint


# ---------------------------------------------------------------------------
# Delete reconciliation (spec §6): key inventory + anti-join
# ---------------------------------------------------------------------------


def test_key_inventory_and_anti_join_tombstones_deleted_orders(tmp_path: Path) -> None:
    conn = _scan_connection(
        SALES,
        [_doc_row(SALES, 1, "ORDER"), _doc_row(SALES, 2, "ORDER")],
        [_doc_row(SALES, 1, "SPECIAL ORDER")],
    )
    connector = _connector(tmp_path, conn, settings={"order_document_types": "ORDER,SPECIAL ORDER"})
    connector.extract(SALES)  # stage three warehouse keys

    # the source now returns only two of the three keys (ORDER-0002 hard-deleted)
    spec = _DOCUMENT_ENTITIES[SALES]
    key_desc = ["order_no", "line_no", spec.type_field]
    key_rows = [("ORDER-0001", 1, "ORDER"), ("ORDER-0001", 1, "SPECIAL ORDER")]
    conn.next_result_sets.append((key_desc, key_rows))

    result = connector.reconcile_deletes(SALES)

    assert result.delete_semantics == DeleteSemantics.ANTI_JOIN.value
    assert result.source_key_count == 2
    assert result.warehouse_key_count == 3
    assert result.tombstoned_keys == ("ORDER:ORDER-0002:2",)  # type-scoped natural id
    # the key-only scan is type-scoped to the configured document types
    scan_sql, scan_params = conn.executed[-1]
    assert scan_sql.startswith("SELECT DISTINCT")
    assert "FROM OrderLine l JOIN OrderHeader h" in scan_sql
    assert "h.order_type IN (?, ?)" in scan_sql
    assert scan_params == ("ORDER", "SPECIAL ORDER")


def test_key_inventory_refuses_unmapped_entities(tmp_path: Path) -> None:
    connector = _connector(tmp_path, FakeDbApiConnection([]))
    with pytest.raises(ConnectorNotImplemented, match="discovery pack"):
        connector.source_key_inventory("items")
