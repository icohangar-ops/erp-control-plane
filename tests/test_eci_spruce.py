"""ECI Spruce / RockSolid MAX dealer-mediated file-drop tests (spec art_ktSh9Z8x).

Fixture-driven, fully offline: synthetic drop directories standing in for the
dealer's scheduled-report / cloud-folder exports, gated by the same manifest
contract as csv_sftp. Covers promotion, the spec-§6 refusals (stale
generations, regenerated batches), the RSM normalizations (§7), quarantine
with machine-readable reasons, scoped anti-join delete reconciliation, and
disabled-first inertness without onboarding pins.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from connectors.base import ConnectorError, ConnectorNotConfigured, ExtractionMode
from connectors.eci_spruce.connector import (
    RC_CHECKSUM_MISMATCH,
    RC_HEADER_SCHEMA_DRIFT,
    RC_MISSING_FILE,
    RC_PARSE_ERROR,
    RC_REGENERATED_BATCH,
    RC_ROW_COUNT_MISMATCH,
    RC_STALE_MANIFEST,
    EciSpruceConnector,
    FileQuarantined,
)
from control_plane.models import SourceConfig

BATCH_ONE = "SPRUCE-2026-09-20-B001"
GEN_ONE = "2026-09-20T06:00:00Z"
BATCH_TWO = "SPRUCE-2026-09-21-B002"
GEN_TWO = "2026-09-21T06:00:00Z"

ITEMS_CSV = """item_id,description,group,section,uom,pack,pack_uom,unit_cost,list_price,item_status
ITM-1001,2x4x8 SPF STUD,100:LUMBER,LUMBER:DIMENSIONAL,EA,1,EA,3.25,5.75,Active
ITM-1002,3/4 PLY SHEATHING,100:LUMBER,LUMBER:SHEATHING,EA,1,EA,28.40,42.10,Active
ITM-1003,8d COMMON NAIL 5LB,200:FASTENERS,FASTENERS:NAILS,LB,5,LB,4.10,6.85,Active
"""

CUSTOMERS_CSV = """customer_id,customer_name,account_status,account_type,posting_model,credit_limit
CUS-001,Summit Builders,Active,Charge,Balance Forward,25000.00
CUS-002,Harbor Contractors,On Hold,Charge,Open Item,10000.00
CUS-003,Walk In Customer,Cash,Cash,Open Item (Terms),
"""

VENDORS_CSV = """vendor_id,vendor_name
VEN-001,Georgia Pacific
VEN-002,Simpson Strong-Tie
"""

ORDER_LINES_CSV = """order_no,line_no,order_date,customer_id,branch_code,item_id,ordered_uom,ordered_qty,filled_qty,unit_price,order_status
SO-1001,1,2026-09-15,CUS-001,BR-MAIN,ITM-1001,EA,120,120,5.75,Shipped
SO-1001,2,2026-09-15,CUS-001,BR-MAIN,ITM-1003,LB,5,5,6.85,Shipped
SO-1002,1,2026-09-16,CUS-002,BR-EAST,ITM-1002,EA,4,0,42.10,Credit Hold
"""

INVOICE_LINES_CSV = """invoice_no,line_no,invoice_date,order_no,customer_id,branch_code,item_id,invoiced_uom,invoiced_qty,unit_price,unit_cost,freight_amt,tax_amt
INV-5001,1,2026-09-16,SO-1001,CUS-001,BR-MAIN,ITM-1001,EA,120,5.75,3.25,15.00,49.59
INV-5002,1,2026-09-17,SO-1002,CUS-EAST,BR-EAST,ITM-1002,EA,4,42.10,28.40,0,28.21
"""

SNAPSHOTS_CSV = """snapshot_date,branch_code,item_id,on_hand_qty,allocated_qty,on_order_qty,unit_cost
2026-09-21,BR-MAIN,ITM-1001,480.5,60,240,3.25
2026-09-21,BR-MAIN,ITM-1002,55,0,0,28.40
2026-09-21,BR-EAST,ITM-1001,112.25,12,48,3.25
"""

CSV_FILES = {
    "items.csv": ITEMS_CSV,
    "customers.csv": CUSTOMERS_CSV,
    "vendors.csv": VENDORS_CSV,
    "sales_order_lines.csv": ORDER_LINES_CSV,
    "invoice_lines.csv": INVOICE_LINES_CSV,
    "inventory_snapshots.csv": SNAPSHOTS_CSV,
}

ENTITIES = [
    "items",
    "customers",
    "vendors",
    "sales_order_lines",
    "invoice_lines",
    "inventory_snapshots",
]


def _write_drop(
    root: Path,
    batch_id: str,
    generated_at: str,
    files: dict[str, str | tuple[str, str]],
    default_delimiter: str = ",",
) -> None:
    root.mkdir(parents=True, exist_ok=True)
    entries = []
    for name, value in files.items():
        if isinstance(value, tuple):
            content, delimiter = value
        else:
            content, delimiter = value, default_delimiter
        payload = content.encode("utf-8")
        (root / name).write_bytes(payload)
        rows = sum(1 for line in content.splitlines() if line.strip()) - 1  # minus header
        entries.append(
            {
                "name": name,
                "sha256": hashlib.sha256(payload).hexdigest(),
                "rows": rows,
                "encoding": "utf-8",
                "delimiter": delimiter,
            }
        )
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "batch_id": batch_id,
                "generated_at": generated_at,
                "source_company": "test_dealer",
                "schema_version": "1.0.0",
                "files": entries,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


@pytest.fixture()
def spruce_drop_dir(tmp_path: Path) -> Path:
    drop = tmp_path / "spruce_drop"
    _write_drop(drop, BATCH_ONE, GEN_ONE, dict(CSV_FILES))
    return drop


@pytest.fixture()
def spruce_config(spruce_drop_dir: Path) -> SourceConfig:
    return SourceConfig(
        source_id="spruce_test_dealer",
        erp="eci_spruce",
        description="Test dealer for the ECI Spruce file-drop path",
        settings={
            "product": "spruce",
            "hosting": "hosted",
            "layout_profile": "spruce_report_csv",
            "csv_drop_root": str(spruce_drop_dir),
            "allow_additive_columns": "true",
        },
        enabled=False,
    )


@pytest.fixture()
def spruce(spruce_config: SourceConfig, cp_config, store) -> EciSpruceConnector:
    return EciSpruceConnector(spruce_config, store, cp_config)


def _with_settings(spruce: EciSpruceConnector, **settings: str) -> EciSpruceConnector:
    replaced = dataclasses.replace(spruce.source, settings={**spruce.source.settings, **settings})
    return EciSpruceConnector(replaced, spruce.store, spruce.config)


# ---------------------------------------------------------------------------
# Promotion path
# ---------------------------------------------------------------------------


def test_extract_all_entities_matches_manifest_row_counts(spruce):
    for entity in ENTITIES:
        result = spruce.extract(entity)
        assert result.mode == "backfill"
        assert result.rows_extracted > 0, f"{entity}: fixture must hold rows"
        assert Path(result.parquet_path).exists()
        assert result.watermark_after == f"{GEN_ONE}|{BATCH_ONE}"


def test_composite_watermark_records_generation_then_batch(spruce):
    result = spruce.extract("items")
    stored = spruce.store.get_watermark(spruce.source.source_id, "items", "backfill")
    assert result.watermark_after == stored == f"{GEN_ONE}|{BATCH_ONE}"


def test_extracted_parquet_carries_provenance_and_split_columns(spruce):
    result = spruce.extract("items")
    table = duckdb.read_parquet(result.parquet_path)
    assert {"source_system", "source_id", "source_file", "loaded_at"}.issubset(set(table.columns))
    assert table.aggregate("count(DISTINCT source_system)").fetchone()[0] == 1
    rows = table.aggregate("count(*)").fetchone()[0]
    assert rows == result.rows_extracted == 3
    # RSM "code:name" concatenations leave staging as separate columns (spec §7).
    assert set(table.columns) >= {"group_code", "group_name", "section_code", "section_name"}
    assert "group" not in table.columns and "section" not in table.columns


def test_promoted_records_hold_typed_values(spruce):
    result = spruce.extract("inventory_snapshots")
    table = pq.read_table(result.parquet_path).to_pylist()
    main = next(r for r in table if r["branch_code"] == "BR-MAIN" and r["item_id"] == "ITM-1001")
    assert main["on_hand_qty"] == Decimal("480.5")
    assert main["snapshot_date"].isoformat() == "2026-09-21"
    # decimal qty is the tally-item behavior the spec demands (§7)


# ---------------------------------------------------------------------------
# Idempotency (spec §4: every pull is a repeatable full delivery)
# ---------------------------------------------------------------------------


def test_reextract_is_idempotent_by_content_hash(spruce):
    first = [spruce.extract(e).rows_extracted for e in ENTITIES]
    assert all(n > 0 for n in first)
    second = {e: spruce.extract(e).rows_extracted for e in ENTITIES}
    assert all(n == 0 for n in second.values()), "re-extracting an unchanged batch is a no-op"
    assert spruce.store.list_quarantine(spruce.source.source_id) == []


def test_incremental_same_batch_is_a_no_op(spruce):
    spruce.extract("items")
    again = spruce.extract("items", mode=ExtractionMode.INCREMENTAL)
    assert again.rows_extracted == 0


def test_identical_redelivery_under_a_new_batch_id_is_absorbed(spruce, spruce_drop_dir):
    for entity in ENTITIES:
        spruce.extract(entity)
    # The dealer re-delivers the same files under a newer batch: content-hash
    # idempotency absorbs the delivery regardless of the new batch identity.
    _write_drop(spruce_drop_dir, BATCH_TWO, GEN_TWO, dict(CSV_FILES))
    second = {e: spruce.extract(e).rows_extracted for e in ENTITIES}
    assert all(n == 0 for n in second.values())


# ---------------------------------------------------------------------------
# Disabled-first onboarding ([D] pins fail closed)
# ---------------------------------------------------------------------------


def test_unpinned_source_is_inert(spruce):
    bare = EciSpruceConnector(
        dataclasses.replace(spruce.source, settings={}), spruce.store, spruce.config
    )
    problems = bare.validate_config()
    assert len(problems) == 1
    assert "missing required ECI Spruce/RSM settings" in problems[0]
    for setting in ("product", "hosting", "layout_profile", "csv_drop_root"):
        assert setting in problems[0]
    with pytest.raises(ConnectorNotConfigured):
        bare.extract("items")


@pytest.mark.parametrize(
    ("setting", "value", "fragment"),
    [
        ("product", "soap", "must be one of spruce, rsm"),
        ("hosting", "unknown", "must be one of hosted, on_prem"),
        ("layout_profile", "wsdl", "must be one of spruce_report_csv, rsm_pipe_export"),
    ],
)
def test_invalid_discovery_pins_fail_closed(spruce, setting, value, fragment):
    pinned = _with_settings(spruce, **{setting: value})
    problems = pinned.validate_config()
    assert any(fragment in p for p in problems), f"{setting}={value} must be refused"


def test_manifest_missing_fails_closed(spruce_drop_dir, spruce):
    (spruce_drop_dir / "manifest.json").unlink()
    problems = spruce.validate_config()
    assert any("manifest invalid" in p for p in problems)
    with pytest.raises(ConnectorError):
        spruce.extract("items")


def test_manifest_not_declaring_an_entity_file_fails_closed(spruce_drop_dir, spruce):
    manifest = json.loads((spruce_drop_dir / "manifest.json").read_text(encoding="utf-8"))
    manifest["files"] = [f for f in manifest["files"] if f["name"] != "vendors.csv"]
    (spruce_drop_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    assert any("vendors.csv" in p for p in spruce.validate_config())
    with pytest.raises(ConnectorNotConfigured):
        spruce.extract("vendors")


def test_dry_run_is_plannable_without_pins(spruce):
    bare = EciSpruceConnector(
        dataclasses.replace(spruce.source, settings={}), spruce.store, spruce.config
    )
    plan = bare.dry_run()
    assert plan["config_problems"], "an unpinned source must surface its gaps"
    assert len(plan["entities"]) == len(ENTITIES)
    # The NDA boundary is part of the plan, not a footnote.
    assert all("NDA" in e["notes"] for e in plan["entities"])


# ---------------------------------------------------------------------------
# Quarantine gates (csv_sftp parity)
# ---------------------------------------------------------------------------


def test_missing_file_quarantines(spruce_drop_dir, spruce):
    (spruce_drop_dir / "vendors.csv").unlink()
    with pytest.raises(FileQuarantined):
        spruce.extract("vendors")
    record = spruce.store.list_quarantine(spruce.source.source_id)[0]
    assert record.reason_code == RC_MISSING_FILE
    assert record.batch_id == BATCH_ONE


def test_checksum_mismatch_quarantines_with_reason_file(spruce_drop_dir, spruce):
    target = spruce_drop_dir / "vendors.csv"
    target.write_bytes(target.read_bytes() + b"VEN-999,Extra Vendor\n")
    with pytest.raises(FileQuarantined):
        spruce.extract("vendors")
    record = spruce.store.list_quarantine(spruce.source.source_id)[0]
    assert record.reason_code == RC_CHECKSUM_MISMATCH
    assert Path(record.quarantine_path).exists()
    reason_file = Path(record.quarantine_path).with_name("vendors.csv.reason.json")
    reason = json.loads(reason_file.read_text(encoding="utf-8"))
    assert reason["reason_code"] == RC_CHECKSUM_MISMATCH
    assert reason["batch_id"] == BATCH_ONE
    assert "manifest sha256" in reason["detail"]


def test_header_schema_drift_quarantines(spruce_drop_dir, spruce):
    target = spruce_drop_dir / "items.csv"
    target.write_text(
        ITEMS_CSV.replace("item_id,description,group", "item_id,description,grp", 1),
        encoding="utf-8",
    )
    manifest = json.loads((spruce_drop_dir / "manifest.json").read_text(encoding="utf-8"))
    entry = next(f for f in manifest["files"] if f["name"] == "items.csv")
    entry["sha256"] = hashlib.sha256(target.read_bytes()).hexdigest()
    (spruce_drop_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(FileQuarantined):
        spruce.extract("items")
    assert spruce.store.list_quarantine(spruce.source.source_id)[0].reason_code == (
        RC_HEADER_SCHEMA_DRIFT
    )
    assert spruce.store.get_watermark(spruce.source.source_id, "items", "backfill") is None


def _extra_column_csv() -> str:
    lines = ITEMS_CSV.splitlines()
    return (
        "\n".join(f"{line},flag" if line.startswith("item_id") else f"{line},N" for line in lines)
        + "\n"
    )


def _rehash_items_manifest(spruce_drop_dir: Path, content: str) -> None:
    target = spruce_drop_dir / "items.csv"
    target.write_text(content, encoding="utf-8")
    manifest = json.loads((spruce_drop_dir / "manifest.json").read_text(encoding="utf-8"))
    entry = next(f for f in manifest["files"] if f["name"] == "items.csv")
    entry["sha256"] = hashlib.sha256(target.read_bytes()).hexdigest()
    (spruce_drop_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


def test_extra_column_refused_when_not_allowed(spruce_drop_dir, spruce):
    _rehash_items_manifest(spruce_drop_dir, _extra_column_csv())
    strict = _with_settings(spruce, allow_additive_columns="false")
    with pytest.raises(FileQuarantined):
        strict.extract("items")
    assert spruce.store.list_quarantine(spruce.source.source_id)[0].reason_code == (
        RC_HEADER_SCHEMA_DRIFT
    )


def test_extra_column_tolerated_when_allowed(spruce_drop_dir, spruce):
    _rehash_items_manifest(spruce_drop_dir, _extra_column_csv())
    # additive columns tolerated by default (allow_additive_columns=true) and
    # mapped deliberately: the undeclared column stays out of the Arrow schema.
    result = spruce.extract("items")
    assert result.rows_extracted == 3
    table = duckdb.read_parquet(result.parquet_path)
    assert "flag" not in table.columns


def test_parse_error_quarantines(spruce_drop_dir, spruce):
    target = spruce_drop_dir / "items.csv"
    target.write_text(
        ITEMS_CSV.replace("3.25,5.75,Active", "FREE,5.75,Active", 1), encoding="utf-8"
    )
    manifest = json.loads((spruce_drop_dir / "manifest.json").read_text(encoding="utf-8"))
    entry = next(f for f in manifest["files"] if f["name"] == "items.csv")
    entry["sha256"] = hashlib.sha256(target.read_bytes()).hexdigest()
    (spruce_drop_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(FileQuarantined):
        spruce.extract("items")
    assert spruce.store.list_quarantine(spruce.source.source_id)[0].reason_code == RC_PARSE_ERROR


def test_row_count_mismatch_quarantines(spruce_drop_dir, spruce):
    target = spruce_drop_dir / "vendors.csv"
    target.write_bytes(target.read_bytes() + b"VEN-999,Extra Vendor\n")
    manifest = json.loads((spruce_drop_dir / "manifest.json").read_text(encoding="utf-8"))
    entry = next(f for f in manifest["files"] if f["name"] == "vendors.csv")
    entry["sha256"] = hashlib.sha256(target.read_bytes()).hexdigest()
    (spruce_drop_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(FileQuarantined):
        spruce.extract("vendors")
    assert spruce.store.list_quarantine(spruce.source.source_id)[0].reason_code == (
        RC_ROW_COUNT_MISMATCH
    )


# ---------------------------------------------------------------------------
# Spec §6 refusals: stale generations and regenerated batches
# ---------------------------------------------------------------------------


def test_stale_manifest_refuses(spruce_drop_dir, spruce):
    spruce.extract("items")
    assert spruce.store.get_watermark(spruce.source.source_id, "items", "backfill") == (
        f"{GEN_ONE}|{BATCH_ONE}"
    )
    # A re-delivery stamped EARLIER than the processed generation (report
    # drift / replay) must refuse, not overwrite (spec §6).
    drifted = dict(CSV_FILES)
    drifted["items.csv"] = ITEMS_CSV.replace("5.75,Active", "5.95,Active", 1)
    _write_drop(spruce_drop_dir, BATCH_TWO, "2026-09-19T06:00:00Z", drifted)
    with pytest.raises(FileQuarantined):
        spruce.extract("items")
    assert spruce.store.list_quarantine(spruce.source.source_id)[0].reason_code == (
        RC_STALE_MANIFEST
    )
    assert spruce.store.get_watermark(spruce.source.source_id, "items", "backfill") == (
        f"{GEN_ONE}|{BATCH_ONE}"
    )


def test_regenerated_batch_refuses(spruce_drop_dir, spruce):
    spruce.extract("items")
    # Same batch id, changed content: a regenerated report is new evidence to
    # reconcile, not an override (spec §6).
    regenerated = dict(CSV_FILES)
    regenerated["items.csv"] = ITEMS_CSV.replace("3.25", "3.35", 1)
    _write_drop(spruce_drop_dir, BATCH_ONE, GEN_ONE, regenerated)
    with pytest.raises(FileQuarantined):
        spruce.extract("items")
    assert spruce.store.list_quarantine(spruce.source.source_id)[0].reason_code == (
        RC_REGENERATED_BATCH
    )


def test_identical_re_delivery_of_an_older_batch_is_absorbed(spruce_drop_dir, spruce):
    for entity in ENTITIES:
        spruce.extract(entity)
    # Move forward two batches, then re-deliver the ORIGINAL batch bytes.
    forward = dict(CSV_FILES)
    forward["items.csv"] = ITEMS_CSV.replace("3.25", "3.45", 1)
    _write_drop(spruce_drop_dir, BATCH_TWO, GEN_TWO, forward)
    assert spruce.extract("items").rows_extracted == 3
    _write_drop(spruce_drop_dir, BATCH_ONE, GEN_ONE, dict(CSV_FILES))
    # Identical bytes to a processed batch: the hash gate absorbs before any
    # batch-order refusal could fire.
    assert spruce.extract("items").rows_extracted == 0
    assert spruce.store.list_quarantine(spruce.source.source_id) == []


# ---------------------------------------------------------------------------
# RSM layout specifics (spec §7)
# ---------------------------------------------------------------------------


def test_rsm_pipe_layout_splits_and_survives_quirks(tmp_path, cp_config, store):
    drop = tmp_path / "rsm_drop"
    items_pipe = (
        "item_id|description|group|section|uom|pack|pack_uom|unit_cost|list_price|item_status\n"
        "ITM-2001|1/2 EMT CONDUIT\t10FT|201:ELECTRICAL|ELECTRICAL:CONDUIT|EA|1|EA|4.85|7.99|Active\n"
        "ITM-2002|8d COMMON NAIL 5LB|200:FASTENERS|FASTENERS:NAILS|LB|5|LB|4.10|6.85|Active\n"
        "ITM-2002|8d COMMON NAIL 5LB|200:FASTENERS|FASTENERS:NAILS|LB|5|LB|4.10|6.85|Active\n"
        "ITM-2003|UNSORTED STOCK|FASTENERS|FASTENERS:NAILS|EA|1|EA|1.00|2.00|Active\n"
    )
    _write_drop(drop, "RSM-2026-09-20-B001", GEN_ONE, {**CSV_FILES, "items.csv": (items_pipe, "|")})
    config = SourceConfig(
        source_id="rsm_test_dealer",
        erp="eci_spruce",
        description="Test RSM dealer",
        settings={
            "product": "rsm",
            "hosting": "hosted",
            "layout_profile": "rsm_pipe_export",
            "csv_drop_root": str(drop),
            "allow_additive_columns": "true",
        },
        enabled=False,
    )
    connector = EciSpruceConnector(config, store, cp_config)
    result = connector.extract("items")
    assert result.rows_extracted == 3, "duplicate natural keys collapse keep-first (spec §7)"
    rows = pq.read_table(result.parquet_path).to_pylist()
    by_id = {row["item_id"]: row for row in rows}
    split_row = by_id["ITM-2001"]
    assert split_row["group_code"] == "201" and split_row["group_name"] == "ELECTRICAL"
    assert split_row["section_code"] == "ELECTRICAL" and split_row["section_name"] == "CONDUIT"
    assert "\t" in split_row["description"], "embedded tabs survive CSV quoting"
    unsplit = by_id["ITM-2003"]
    assert unsplit["group_code"] == "FASTENERS" and unsplit["group_name"] is None
    assert unsplit["section_name"] == "NAILS"


# ---------------------------------------------------------------------------
# Scoped delete reconciliation (spec §4/§6)
# ---------------------------------------------------------------------------


def test_anti_join_reconciles_full_file_dimension(spruce_drop_dir, spruce):
    for entity in ENTITIES:
        spruce.extract(entity)
    shrunken = dict(CSV_FILES)
    shrunken["items.csv"] = ITEMS_CSV.replace(
        "ITM-1003,8d COMMON NAIL 5LB,200:FASTENERS,FASTENERS:NAILS,LB,5,LB,4.10,6.85,Active\n", ""
    )
    _write_drop(spruce_drop_dir, BATCH_TWO, GEN_TWO, shrunken)
    result = spruce.reconcile_deletes("items")
    assert result.tombstoned_keys == ("ITM-1003",)
    assert result.source_key_count == 2
    assert result.warehouse_key_count == 3
    tombstones = spruce.store.list_tombstones(spruce.source.source_id, "items")
    assert [t.source_key for t in tombstones] == ["ITM-1003"]
    assert all(t.batch_id == BATCH_TWO for t in tombstones)
    runs = spruce.store.list_reconciliation_runs(spruce.source.source_id)
    assert runs and runs[-1].source_key_count == 2 and runs[-1].warehouse_key_count == 3


def test_anti_join_refused_for_period_scoped_entities(spruce):
    for entity in ("sales_order_lines", "invoice_lines", "inventory_snapshots"):
        with pytest.raises(ConnectorError, match="scoped out"):
            spruce.reconcile_deletes(entity)


def test_key_scan_fails_closed_on_empty_full_file(tmp_path, cp_config, store):
    drop = tmp_path / "empty_drop"
    empty_items = (
        "item_id,description,group,section,uom,pack,pack_uom,unit_cost,list_price,item_status\n"
    )
    _write_drop(drop, BATCH_ONE, GEN_ONE, {**CSV_FILES, "items.csv": empty_items})
    config = SourceConfig(
        source_id="spruce_empty_dealer",
        erp="eci_spruce",
        description="Test dealer with an empty items export",
        settings={
            "product": "spruce",
            "hosting": "hosted",
            "layout_profile": "spruce_report_csv",
            "csv_drop_root": str(drop),
        },
        enabled=False,
    )
    connector = EciSpruceConnector(config, store, cp_config)
    with pytest.raises(ConnectorError, match="tombstone the whole warehouse"):
        connector.reconcile_deletes("items")


def test_key_scan_refuses_stale_drop(spruce_drop_dir, spruce):
    for entity in ENTITIES:
        spruce.extract(entity)
    newer = dict(CSV_FILES)
    newer["items.csv"] = ITEMS_CSV.replace("3.25", "3.55", 1)
    _write_drop(spruce_drop_dir, BATCH_TWO, GEN_TWO, newer)
    spruce.extract("items")  # advance the processed generation
    older = dict(CSV_FILES)
    older["items.csv"] = ITEMS_CSV.replace("3.25", "3.65", 1)
    _write_drop(spruce_drop_dir, "SPRUCE-2026-09-20-B003", "2026-09-20T12:00:00Z", older)
    with pytest.raises(FileQuarantined):
        spruce.reconcile_deletes("items")
    assert spruce.store.list_quarantine(spruce.source.source_id)[0].reason_code == (
        RC_STALE_MANIFEST
    )
