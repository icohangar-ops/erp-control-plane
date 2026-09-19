"""Fixture-based tests for the legacy SQL connector pack (spec §5 rows 1-9).

No live ERP connections: every test drives a fake DB-API 2.0 connection
(queue-based) through the real extraction machinery, asserting on the SQL that
was executed, the staged Parquet, watermark advancement, and the §6 anti-join
delete reconciliation.
"""

from __future__ import annotations

import importlib
from dataclasses import replace
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml

from connectors.base import ConnectorError, DeleteSemantics, ExtractionMode
from connectors.csv_sftp.connector import CsvSftpConnector
from connectors.legacy.db2_iseries import Db2ISeriesConnector
from connectors.legacy.db2_iseries.cdc import DB2_ISERIES_CDC
from connectors.legacy.db2_luw import Db2LuwConnector
from connectors.legacy.db2_luw.cdc import DB2_LUW_CDC
from connectors.legacy.informix import InformixConnector
from connectors.legacy.informix.cdc import INFORMIX_CDC
from connectors.legacy.openedge import OpenEdgeConnector
from connectors.legacy.openedge.cdc import OPENEDGE_CDC
from connectors.legacy.oracle import OracleConnector
from connectors.legacy.oracle.cdc import ORACLE_CDC
from connectors.legacy.postgresql import PostgresConnector
from connectors.legacy.schemas import (
    CANONICAL_ENTITY_COLUMNS,
    NATURAL_KEY_FIELDS,
    canonical_arrow_schema,
)
from connectors.legacy.sql_source import (
    DbApiBatchConnector,
    validate_identifier,
)
from connectors.legacy.sybase_ase import SybaseAseConnector
from connectors.registry import load_source_configs
from control_plane.config import ControlPlaneConfig
from control_plane.store import SqliteControlPlaneStore
from tests.fixtures.fake_dbapi import FakeDbApiConnection
from tests.fixtures.legacy_rows import canonical_row, result_set

#: One connector class per legacy spec row (MariaDB shares MySQL's row).
LEGACY_CLASSES: tuple[type[DbApiBatchConnector], ...] = (
    InformixConnector,
    Db2LuwConnector,
    Db2ISeriesConnector,
    OracleConnector,
    PostgresConnector,
    SybaseAseConnector,
    OpenEdgeConnector,
)


class Harness:
    """Config + store + a connector class bound to fake connections."""

    def __init__(self, tmp_path: Path, connector_cls: type[DbApiBatchConnector]) -> None:
        self.connector_cls = connector_cls
        self.config = ControlPlaneConfig(
            backend="sqlite",
            sqlite_path=tmp_path / "cp.db",
            control_plane_dsn=None,
            lake_root=tmp_path / "lake",
            analytics_duckdb_path=tmp_path / "analytics.duckdb",
            quarantine_root=tmp_path / "quarantine",
            environment="test",
        )
        self.store = SqliteControlPlaneStore(tmp_path / "cp.db")
        self.store.initialize()

    def connector(self, connection: FakeDbApiConnection) -> DbApiBatchConnector:
        source = next(
            s
            for s in load_source_configs()
            if s.source_id == f"{self.connector_cls.erp_id}_template"
        )
        return self.connector_cls(
            source, self.store, self.config, connection_factory=lambda settings: connection
        )


@pytest.fixture()
def harness(tmp_path: Path) -> Harness:
    return Harness(tmp_path, InformixConnector)


def _watermark_tail(
    entity: str,
    rows: list[dict[str, object]],
    values: list[str],
    column: str = "last_modified",
) -> tuple[list[str], list[tuple]]:
    """Append the watermark column to a fixture result set (SELECT tail order).

    Real drivers return the watermark column in every mode — ``_select_sql``
    appends it to the SELECT unconditionally — so backfill fixtures carry the
    tail too (the GenBI demo found the width mismatch the hard way).
    """
    desc, tuples = result_set(entity, rows)
    assert len(values) == len(tuples)
    return [*desc, column], [(*t, v) for t, v in zip(tuples, values, strict=True)]


# ---------------------------------------------------------------------------
# Shared schema contract
# ---------------------------------------------------------------------------


def test_canonical_schemas_match_the_csv_sftp_registry():
    """One canonical schema per entity across every pack (CSV registry is origin)."""
    yml = (
        Path(__file__).resolve().parents[1] / "connectors" / "csv_sftp" / "schemas.yml"
    ).read_text(encoding="utf-8")
    registry: dict[str, dict] = yaml.safe_load(yml)
    assert set(CANONICAL_ENTITY_COLUMNS) == set(registry)
    for entity, spec in registry.items():
        ours = CANONICAL_ENTITY_COLUMNS[entity]
        assert [(name, kind) for name, kind in ours] == [
            (col["name"], col["type"]) for col in spec["columns"]
        ], f"{entity}: canonical schema drifted from the CSV registry"
    assert CsvSftpConnector.natural_key_fields == NATURAL_KEY_FIELDS


def test_every_legacy_connector_declares_all_nine_entities(tmp_path: Path) -> None:
    for cls in LEGACY_CLASSES:
        harness = Harness(tmp_path / cls.erp_id, cls)
        connector = harness.connector(FakeDbApiConnection([]))
        assert set(connector.entities()) == set(CANONICAL_ENTITY_COLUMNS)
        assert connector.delete_handling is DeleteSemantics.ANTI_JOIN


# ---------------------------------------------------------------------------
# Extraction shape
# ---------------------------------------------------------------------------


def test_backfill_extraction_writes_provenance(harness: Harness) -> None:
    rows = [canonical_row("items", i) for i in (1, 2, 3)]
    desc, tuples = _watermark_tail(
        "items", rows, ["2026-08-01T09:00:00"] * 3
    )  # backfill: real-driver width (watermark tail)
    conn = FakeDbApiConnection([(desc, tuples)])
    connector = harness.connector(conn)

    result = connector.extract("items")

    assert result.rows_extracted == 3
    assert result.mode == "backfill"
    sql, params = conn.executed[0]
    assert sql.startswith("SELECT item_no AS item_no, description AS description")
    assert " FROM ifx_items" in sql  # Informix identity-map table name
    assert "last_modified" in sql  # watermark column rides the SELECT tail
    assert params is None  # backfill: no watermark predicate

    table = pq.read_table(result.parquet_path)
    assert table.num_rows == 3
    assert {"source_system", "source_id", "loaded_at"}.issubset(set(table.column_names))
    assert len(set(table.column("source_system").to_pylist())) == 1
    assert set(table.column("source_id").to_pylist()) == {"ITEM-0001", "ITEM-0002", "ITEM-0003"}
    # The watermark column rides the SELECT tail (real drivers return it in
    # backfill too) but is observed, never staged — found by the GenBI demo.
    assert "last_modified" not in table.column_names


def test_column_map_extraction_db2_iseries(tmp_path: Path) -> None:
    """Db2 for i uses DDS column names — the map translates to canonical."""
    harness = Harness(tmp_path, Db2ISeriesConnector)
    src = Db2ISeriesConnector.entity_sources["sales_order_lines"]
    assert src.columns is not None, "Db2 for i sales_order_lines must declare a column map"
    row = canonical_row("sales_order_lines", 7)
    desc, tuples = result_set("sales_order_lines", [row], columns=src.columns)
    # Real-driver shape: the watermark column rides the SELECT tail in
    # backfill too (OLUPDT for Db2 for i sales_order_lines).
    desc, tuples = [*desc, src.incremental_column], [(*tuples[0], "2026-08-01T09:00:00")]
    conn = FakeDbApiConnection([(desc, tuples)])
    connector = harness.connector(conn)

    result = connector.extract("sales_order_lines")

    assert result.rows_extracted == 1
    sql = conn.executed[0][0]
    assert f" FROM {src.table}" in sql
    table = pq.read_table(result.parquet_path)
    assert table.column("source_id").to_pylist() == ["ORDER-0007:7"]


def test_incremental_watermark_filters_and_advances(harness: Harness) -> None:
    first = [canonical_row("items", i) for i in (1, 2, 3)]
    desc1, rows1 = _watermark_tail(
        "items",
        first,
        ["2026-08-01T09:00:00", "2026-08-01T09:30:00", "2026-08-01T10:00:00"],
    )
    desc2, rows2 = _watermark_tail("items", [canonical_row("items", 4)], ["2026-08-01T11:30:00"])
    conn = FakeDbApiConnection([(desc1, rows1), (desc2, rows2)])
    connector = harness.connector(conn)

    result = connector.extract("items", ExtractionMode.INCREMENTAL)  # first run: no checkpoint
    assert result.rows_extracted == 3
    assert result.watermark_after == "2026-08-01T10:00:00"

    second = connector.extract("items", ExtractionMode.INCREMENTAL)  # checkpoint persisted
    assert second.rows_extracted == 1
    assert second.watermark_before == "2026-08-01T10:00:00"
    assert second.watermark_after == "2026-08-01T11:30:00"
    sql, params = conn.executed[1]
    assert "WHERE last_modified > ?" in sql
    assert params == ("2026-08-01T10:00:00",)


# ---------------------------------------------------------------------------
# Anti-join delete reconciliation (spec §6 cross-cutting rule)
# ---------------------------------------------------------------------------


def test_anti_join_reconciliation_tombstones_deletes(harness: Harness) -> None:
    rows = [canonical_row("items", i) for i in (1, 2, 3)]
    desc, tuples = _watermark_tail(
        "items", rows, ["2026-08-01T09:00:00"] * 3
    )  # backfill: real-driver width (watermark tail)
    conn = FakeDbApiConnection([(desc, tuples), (["item_no"], [("ITEM-0001",), ("ITEM-0002",)])])
    connector = harness.connector(conn)
    connector.extract("items")

    result = connector.reconcile_deletes("items")

    assert result.delete_semantics == "anti_join"
    assert result.source_key_count == 2
    assert result.warehouse_key_count == 3
    assert result.tombstoned_keys == ("ITEM-0003",)
    # Reconciliation and tombstone are persisted for audit (spec §6).
    tombstones = harness.store.list_tombstones("informix_template", "items")
    assert [t.source_key for t in tombstones] == ["ITEM-0003"]
    key_scan_sql = conn.executed[-1][0]
    assert key_scan_sql.startswith("SELECT DISTINCT item_no AS item_no FROM ifx_items")


def test_soft_delete_excluded_from_key_inventory(tmp_path: Path) -> None:
    """Sybase soft-flagged rows never tombstone live keys (§6 soft-delete note).

    The database applies the soft-delete predicate; the fixture returns what a
    filtered key scan would return. The connector's job — emitting the
    predicate and binding the deleted-marker value — is asserted on the SQL.
    """
    harness = Harness(tmp_path, SybaseAseConnector)
    conn = FakeDbApiConnection([(["customer_no"], [("CUSTOMER-0001",)])])
    connector = harness.connector(conn)
    source = replace(
        connector.source,
        settings={
            **connector.source.settings,
            "soft_delete_column": "delete_flag",
            "soft_delete_value": "1",
        },
    )
    scoped = SybaseAseConnector(
        source, harness.store, harness.config, connection_factory=lambda settings: conn
    )

    inventory = scoped.source_key_inventory("customers")

    assert inventory == {"CUSTOMER-0001"}
    sql, params = conn.executed[0]
    assert "WHERE delete_flag <> ?" in sql
    assert params == ("1",)


def test_soft_delete_settings_must_be_paired(tmp_path: Path) -> None:
    harness = Harness(tmp_path, SybaseAseConnector)
    conn = FakeDbApiConnection([(["customer_no"], [])])
    connector = harness.connector(conn)
    column_only = replace(
        connector.source,
        settings={**connector.source.settings, "soft_delete_column": "delete_flag"},
    )
    value_only = replace(
        connector.source, settings={**connector.source.settings, "soft_delete_value": "1"}
    )
    with pytest.raises(ConnectorError, match="soft_delete_value"):
        SybaseAseConnector(
            column_only, harness.store, harness.config, connection_factory=lambda settings: conn
        ).source_key_inventory("customers")
    with pytest.raises(ConnectorError, match="soft_delete_column"):
        SybaseAseConnector(
            value_only, harness.store, harness.config, connection_factory=lambda settings: conn
        ).source_key_inventory("customers")


# ---------------------------------------------------------------------------
# SQL safety and dry-run planning
# ---------------------------------------------------------------------------


def test_identifier_validation_rejects_injection() -> None:
    for bad in ("db; DROP TABLE x", "a b", 'x" --', ""):
        with pytest.raises(ConnectorError, match="identifier"):
            validate_identifier(bad, "table")
    assert validate_identifier("vptb_ifx_items", "table") == "vptb_ifx_items"


def test_plan_dry_run_for_legacy_pack(tmp_path: Path) -> None:
    """Every legacy class plans all nine entities with zero network access."""
    for cls in LEGACY_CLASSES:
        harness = Harness(tmp_path / cls.erp_id, cls)
        connector = harness.connector(FakeDbApiConnection([]))
        plan = connector.dry_run()
        assert plan["source_id"] == f"{cls.erp_id}_template"
        assert len(plan["entities"]) == len(CANONICAL_ENTITY_COLUMNS)
        # Templates ship with empty settings: the missing-credential problems
        # are the gate that keeps them disabled, not a broken build.
        assert connector.validate_config()
        assert "missing settings" in ", ".join(plan["config_problems"])
        for entry in plan["entities"]:
            assert entry["surface"] and entry["notes"]


def test_arrow_schemas_are_typed_not_inferred() -> None:
    for entity in CANONICAL_ENTITY_COLUMNS:
        schema = canonical_arrow_schema(entity)
        assert schema is not None
        kinds = {field.name: str(field.type) for field in schema}
        for name, kind in CANONICAL_ENTITY_COLUMNS[entity]:
            if kind == "decimal":
                assert kinds[name].startswith("decimal"), f"{entity}.{name}"
            elif kind == "integer":
                assert kinds[name] == "int64", f"{entity}.{name}"


# ---------------------------------------------------------------------------
# dlt resource integration
# ---------------------------------------------------------------------------


def test_dlt_resource_streams_stamped_records(harness: Harness) -> None:
    pytest.importorskip("dlt")
    from connectors.legacy.dlt_source import as_dlt_resource

    rows = [canonical_row("vendors", i) for i in (1, 2)]
    desc, tuples = _watermark_tail(
        "vendors", rows, ["2026-08-01T09:00:00"] * 2
    )  # backfill: real-driver width (watermark tail)
    conn = FakeDbApiConnection([(desc, tuples)])
    connector = harness.connector(conn)

    streamed = list(as_dlt_resource(connector, "vendors")())

    assert len(streamed) == 2
    assert {r["source_id"] for r in streamed} == {"VENDOR-0001", "VENDOR-0002"}
    assert all(r["source_system"] == "informix" for r in streamed)


# ---------------------------------------------------------------------------
# CDC module bindings (documented posture; not CI-required per spec §6)
# ---------------------------------------------------------------------------


def test_cdc_modules_document_the_binding_corrections() -> None:
    assert DB2_LUW_CDC.license_gate is not None and "IIDR" in DB2_LUW_CDC.license_gate
    assert DB2_LUW_CDC.recommended_mode == "batch"
    assert "incubating" in DB2_ISERIES_CDC.notes
    assert "pin a Final release (3.0.0-3.1.1)" in " ".join(DB2_ISERIES_CDC.prerequisites)
    assert "v15" in " ".join(INFORMIX_CDC.sources)  # C4: driver v15 posture + 12.x works
    assert "LogMiner" in (ORACLE_CDC.license_gate or "")  # no extra license for LogMiner
    assert "GoldenGate" in (ORACLE_CDC.license_gate or "")  # XStream alternative is licensed
    assert "no Debezium connector exists" in OPENEDGE_CDC.debezium_maturity


def test_sybase_ase_has_no_cdc_module() -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("connectors.legacy.sybase_ase.cdc")
