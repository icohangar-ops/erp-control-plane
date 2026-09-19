"""The Dagster Definitions load with the spec §6 reconciliation asset wired in."""

from __future__ import annotations

from dagster import AssetKey

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
