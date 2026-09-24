"""End-to-end csv_sftp tests against the seeded Ridgeline export.

Covers the full promotion path: manifest gates (checksum, row count, header
schema), Parquet promotion with provenance, content-hash idempotency, and
quarantine with a machine-readable reason.
"""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import duckdb
import pytest

from connectors.base import ExtractedEntity
from connectors.csv_sftp.connector import FileQuarantined
from connectors.registry import build_connector

ENTITIES = [
    "items",
    "salespeople",
    "customers",
    "vendors",
    "sales_order_lines",
    "purchase_order_lines",
    "invoice_lines",
    "inventory_snapshots",
    "gl_entries",
]


@pytest.fixture()
def ridgeline(ridgeline_config, cp_config, store):
    return build_connector(ridgeline_config, config=cp_config, store=store)


def test_extract_all_entities_matches_manifest_row_counts(ridgeline):
    manifest = json.loads(
        (Path(ridgeline.source.settings["drop_root"]) / "manifest.json").read_text(encoding="utf-8")
    )
    declared = {entry["name"].removesuffix(".csv"): entry["rows"] for entry in manifest["files"]}
    results: dict[str, ExtractedEntity] = {}
    for entity in ENTITIES:
        result = ridgeline.extract(entity)
        results[entity] = result
        assert result.mode == "backfill"
        assert result.rows_extracted == declared[entity], f"{entity}: row count vs manifest"
        assert Path(result.parquet_path).exists()
        assert result.watermark_after == "RIDGELINE-2026-09-19-B001"


def test_extracted_parquet_carries_provenance(ridgeline):
    result = ridgeline.extract("invoice_lines")
    table = duckdb.read_parquet(result.parquet_path)
    columns = set(table.columns)
    assert {"source_system", "source_id", "source_file", "loaded_at"}.issubset(columns)
    assert table.aggregate("count(DISTINCT source_system)").fetchone()[0] == 1
    assert table.aggregate("count(*)").fetchone()[0] == result.rows_extracted


def test_reextract_is_idempotent_by_content_hash(ridgeline):
    first = [ridgeline.extract(e).rows_extracted for e in ENTITIES]
    assert all(n > 0 for n in first)
    second = {e: ridgeline.extract(e) for e in ENTITIES}
    assert all(r.rows_extracted == 0 for r in second.values()), (
        "re-extracting an unchanged batch must extract zero new rows"
    )


def _with_drop_root(ridgeline, drop_root: Path):
    replaced = dataclasses.replace(
        ridgeline.source, settings={**ridgeline.source.settings, "drop_root": str(drop_root)}
    )
    return build_connector(replaced, config=ridgeline.config, store=ridgeline.store)


def test_tampered_file_is_quarantined_with_reason(ridgeline, drop_copy: Path):
    connector = _with_drop_root(ridgeline, drop_copy)
    target = drop_copy / "vendors.csv"
    original = target.read_bytes()
    target.write_bytes(original + b"VN-999,Extra Vendor,NET30,5\n")

    with pytest.raises(FileQuarantined):
        connector.extract("vendors")

    quarantined = connector.store.list_quarantine("csvsftp_ridgeline")
    assert quarantined, "tampered file must produce a quarantine record"
    assert quarantined[0].reason_code in ("CHECKSUM_MISMATCH", "ROW_COUNT_MISMATCH")
    assert Path(quarantined[0].quarantine_path).exists()


def test_quarantined_batch_leaves_no_new_watermark(ridgeline, drop_copy: Path):
    connector = _with_drop_root(ridgeline, drop_copy)
    target = drop_copy / "items.csv"
    original = target.read_bytes()
    target.write_bytes(original.replace(b"RM-1001", b"RM-9999", 1))

    with pytest.raises(FileQuarantined):
        connector.extract("items")

    assert connector.store.get_watermark("csvsftp_ridgeline", "items", "backfill") is None
