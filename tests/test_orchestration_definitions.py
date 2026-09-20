"""The Dagster Definitions load with the spec §6 reconciliation asset wired in."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from dagster import AssetKey, build_asset_context

from connectors.base import DeleteSemantics
from control_plane.stigmergy import SignalKind, StigmergyBoard, save_board
from orchestration import assets as assets_mod
from orchestration.definitions import defs


def test_definitions_load_with_delete_reconciliation_asset() -> None:
    keys = defs.resolve_all_asset_keys()
    assert AssetKey("delete_reconciliation") in keys


def test_definitions_still_register_the_demo_extraction_assets() -> None:
    keys = defs.resolve_all_asset_keys()
    assert AssetKey("source_registry") in keys
    demo_entities = {
        key.to_user_string().split("/")[1]
        for key in keys
        if key.to_user_string().startswith("csvsftp_ridgeline/")
    }
    assert {"items", "invoice_lines"} <= demo_entities


def test_reconciliation_job_selection_includes_all_assets() -> None:
    jobs = {job.name: job for job in defs.jobs}
    assert "demo_job" in jobs


def test_reconciliation_metadata_reports_this_run_not_stale_board(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Materialize metadata is this run's facts, not the board's history.

    Each reconciliation run hashes its ran_at into the signal payload, so
    prior runs' signals stay on the board until their half-life expires. The
    asset must report only what it posted this run, and signals_posted must
    count this run's posts rather than a post-sweep board-size delta.
    """
    now = datetime.now(UTC)
    board_path = tmp_path / "state" / "stigmergy_board.json"
    stale_board = StigmergyBoard()
    stale_board.post(
        SignalKind.RECONCILIATION_RUN,
        "old_source/items",
        emitted_at=now - timedelta(minutes=5),  # well inside the 12h half-life
        payload={
            "tombstoned": 9,
            "tombstone_ratio": 0.9,
            "ran_at": (now - timedelta(minutes=5)).isoformat(),
        },
    )
    save_board(stale_board, board_path, now)

    source = SimpleNamespace(enabled=True, source_id="demo")
    run_result = SimpleNamespace(
        source_key_count=3,
        warehouse_key_count=10,
        tombstoned_keys=("k-1",),
        ran_at=now,
    )
    connector = SimpleNamespace(
        delete_handling=DeleteSemantics.ANTI_JOIN,
        entities=lambda: ["items"],
        reconcile_deletes=lambda entity: run_result,
    )
    monkeypatch.setattr(assets_mod, "_config", lambda: None)
    monkeypatch.setattr(assets_mod, "open_store", lambda config: None)
    monkeypatch.setattr(assets_mod, "_board_path", lambda: board_path)
    monkeypatch.setattr(assets_mod, "load_source_configs", lambda: [source])
    monkeypatch.setattr(assets_mod, "build_connector", lambda s, config, store: connector)

    materialized = assets_mod.delete_reconciliation(build_asset_context())
    metadata = materialized.metadata

    assert metadata["reconciled_entities"] == 1  # stale run not counted
    assert metadata["tombstoned_keys"] == 1  # stale 9 tombstones not counted
    assert metadata["signals_posted"] == 2  # RECONCILIATION_RUN + TOMBSTONE_BATCH
    assert [p["tombstoned"] for p in metadata["reconciliations"].value] == [1]
