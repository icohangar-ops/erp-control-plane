"""Control-plane store contract: registration, watermarks, idempotency, quarantine."""

from __future__ import annotations

from datetime import UTC, datetime

from control_plane.config import ControlPlaneConfig
from control_plane.models import (
    FileAuditRecord,
    QuarantineRecord,
    SourceConfig,
    SyncCheckpoint,
)
from control_plane.store import SqliteControlPlaneStore, open_store


def _source(source_id: str = "csvsftp_ridgeline") -> SourceConfig:
    return SourceConfig(
        source_id=source_id,
        erp="csv_sftp",
        description="test source",
        settings={"drop_root": "seed/dealer_export"},
    )


def test_open_store_selects_sqlite_backend(cp_config: ControlPlaneConfig, tmp_path):
    assert isinstance(open_store(cp_config), SqliteControlPlaneStore)


def test_register_source_is_idempotent(store: SqliteControlPlaneStore):
    first = store.register_source(_source(), "fp-0001")
    second = store.register_source(_source(), "fp-0001")
    assert first.source_id == second.source_id == "csvsftp_ridgeline"
    assert first.config_fingerprint == second.config_fingerprint
    assert len(store.list_sources()) == 1


def test_config_fingerprint_change_is_visible(store: SqliteControlPlaneStore):
    store.register_source(_source(), "fp-old")
    updated = store.register_source(_source(), "fp-new")  # re-registration after config drift
    assert updated.config_fingerprint == "fp-new"


def test_get_source_roundtrips_settings(store: SqliteControlPlaneStore):
    store.register_source(_source(), "fp-1")
    fetched = store.get_source("csvsftp_ridgeline")
    assert fetched is not None
    assert fetched.erp == "csv_sftp"
    assert fetched.settings["drop_root"] == "seed/dealer_export"
    assert store.get_source("missing") is None


def test_watermark_roundtrip_per_entity_and_mode(store: SqliteControlPlaneStore):
    store.upsert_watermark(
        SyncCheckpoint("csvsftp_ridgeline", "items", "backfill", "B001", datetime.now(UTC))
    )
    store.upsert_watermark(
        SyncCheckpoint("csvsftp_ridgeline", "gl_entries", "backfill", "B001", datetime.now(UTC))
    )
    assert store.get_watermark("csvsftp_ridgeline", "items", "backfill") == "B001"
    assert store.get_watermark("csvsftp_ridgeline", "gl_entries", "backfill") == "B001"
    assert store.get_watermark("csvsftp_ridgeline", "items", "incremental") is None
    # advancing the cursor overwrites in place
    store.upsert_watermark(
        SyncCheckpoint("csvsftp_ridgeline", "items", "backfill", "B002", datetime.now(UTC))
    )
    assert store.get_watermark("csvsftp_ridgeline", "items", "backfill") == "B002"


def test_file_hash_idempotency(store: SqliteControlPlaneStore):
    now = datetime.now(UTC)
    assert not store.has_file_been_processed("csvsftp_ridgeline", "hash-a")
    store.record_file_audit(
        FileAuditRecord(
            source_id="csvsftp_ridgeline",
            batch_id="RIDGELINE-B001",
            file_name="items.csv",
            file_hash="hash-a",
            rows_declared=32,
            rows_parsed=32,
            status="PROMOTED",
            reason_code=None,
            processed_at=now,
        )
    )
    assert store.has_file_been_processed("csvsftp_ridgeline", "hash-a")
    assert not store.has_file_been_processed("csvsftp_ridgeline", "hash-b")


def test_quarantine_records_roundtrip(store: SqliteControlPlaneStore):
    now = datetime.now(UTC)
    store.record_quarantine(
        QuarantineRecord(
            source_id="csvsftp_ridgeline",
            batch_id="RIDGELINE-B002",
            file_name="invoice_lines.csv",
            reason_code="CHECKSUM_MISMATCH",
            detail="manifest checksum did not match file bytes",
            quarantine_path="/quarantine/invoice_lines.csv",
            quarantined_at=now,
        )
    )
    rows = store.list_quarantine("csvsftp_ridgeline")
    assert len(rows) == 1
    assert rows[0].reason_code == "CHECKSUM_MISMATCH"
    assert rows[0].file_name == "invoice_lines.csv"
    assert store.list_quarantine("other-source") == []


def test_quality_results_can_be_recorded(store: SqliteControlPlaneStore):
    # no read API yet; the contract is that recording never raises and is durable
    store.record_quality_result(
        "csvsftp_ridgeline", "invoice_lines", "not_null_invoice_no", "error", "pass", "0 violations"
    )
    store.record_quality_result(
        "csvsftp_ridgeline", "invoice_lines", "not_null_invoice_no", "error", "fail", "2 nulls"
    )
