"""Extraction assets: source registration plus one asset per (source, entity).

Asset keys are ``[<source_id>, <entity>]`` -- the same keys dagster-dbt derives
for the matching dbt external sources, so dbt models inherit lineage edges for
free and the demo job runs extraction before the dbt build automatically.
"""

from datetime import UTC, datetime
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    MaterializeResult,
    MetadataValue,
    asset,
)

from connectors.base import ConnectorNotImplemented, DeleteSemantics, ExtractionMode
from connectors.registry import build_connector, load_source_configs
from control_plane.config import ControlPlaneConfig
from control_plane.stigmergy import (
    DEFAULT_BOARD_PATH,
    SignalKind,
    load_board,
    save_board,
    tombstone_ratio,
)
from control_plane.store import open_store
from orchestration.exceptions import triage


def _config() -> ControlPlaneConfig:
    return ControlPlaneConfig.from_env()


def _board_path() -> Path:
    """Stigmergy board snapshot under the deployment's gitignored data root."""
    return ControlPlaneConfig.from_env().lake_root.parent / "state" / DEFAULT_BOARD_PATH.name


@asset(name="source_registry", tags={"layer": "control_plane"})
def source_registry() -> MaterializeResult:
    """Register every enabled source from connectors/sources.yml in the store."""
    store = open_store(_config())
    registered: list[str] = []
    for source in load_source_configs():
        if not source.enabled:
            continue
        connector = build_connector(source, config=_config(), store=store)
        registration = connector.register()
        registered.append(f"{registration.source_id}@{registration.config_fingerprint}")
    return MaterializeResult(
        metadata={"registered_sources": MetadataValue.json(registered)},
    )


def _extraction_assets() -> list[AssetsDefinition]:
    assets: list[AssetsDefinition] = []
    for source in load_source_configs():
        if not source.enabled:
            continue
        connector = build_connector(source, config=_config(), store=open_store(_config()))
        for entity in connector.entities():
            assets.append(_one_extraction_asset(source.source_id, entity))
    return assets


def _one_extraction_asset(source_id: str, entity: str) -> AssetsDefinition:
    @asset(
        key=[source_id, entity],
        deps=[source_registry],
        tags={"layer": "staging", "erp": "csv_sftp"},
    )
    def _extract(context: AssetExecutionContext) -> MaterializeResult:
        config = _config()
        store = open_store(config)
        connector = build_connector(
            next(s for s in load_source_configs() if s.source_id == source_id),
            config=config,
            store=store,
        )
        result = connector.extract(entity, ExtractionMode.BACKFILL)
        context.log.info(
            f"{source_id}/{entity}: {result.rows_extracted} rows "
            f"(watermark {result.watermark_before} -> {result.watermark_after})"
        )
        return MaterializeResult(
            metadata={
                "rows_extracted": result.rows_extracted,
                "parquet_path": result.parquet_path,
                "watermark_before": result.watermark_before,
                "watermark_after": result.watermark_after,
                "mode": result.mode,
            },
        )

    return _extract


extraction_assets: list[AssetsDefinition] = _extraction_assets()


@asset(name="delete_reconciliation", tags={"layer": "control_plane"})
def delete_reconciliation(context: AssetExecutionContext) -> MaterializeResult:
    """Run the anti-join delete reconciliation for every enabled batch source.

    This is the scheduled job behind spec §6's cross-cutting rule: every batch
    path reconciles deletes — warehouse keys absent from the source become
    tombstones — as part of the connector contract, not an optional extra.
    CDC-native sources are skipped (their change stream carries deletes);
    sources without a key-inventory scan yet are skipped loudly, never
    silently (the skip is posted as a typed signal, not a log line).

    The reconciliation agent's outputs go onto the stigmergy board
    (``control_plane/stigmergy.py``) as typed signals — one
    ``RECONCILIATION_RUN`` (or ``TOMBSTONE_BATCH`` when keys were tombstoned)
    per source/entity and one ``KEY_SCAN_MISSING`` per loud skip. The
    exception agent (``exception_triage`` below) reads those signals from the
    board snapshot; no result dicts cross the agent boundary.
    """
    config = _config()
    store = open_store(config)
    now = datetime.now(UTC)
    board_path = _board_path()
    board = load_board(board_path)
    # Materialize metadata reports THIS run only; the board's accumulated
    # signals are the exception agent's channel, and past runs' signals stay
    # salient there until their half-life expires (each run hashes its ran_at
    # into the payload, so posts never replace a prior run's signal).
    reconciliations: list[dict[str, object]] = []
    skipped: list[str] = []
    tombstoned_total = 0
    signals_posted = 0
    for source in load_source_configs():
        if not source.enabled:
            continue
        connector = build_connector(source, config=config, store=store)
        if connector.delete_handling is not DeleteSemantics.ANTI_JOIN:
            continue
        for entity in connector.entities():
            try:
                result = connector.reconcile_deletes(entity)
            except ConnectorNotImplemented as exc:
                # Skipped loudly: a typed signal the exception agent triages.
                board.post(
                    SignalKind.KEY_SCAN_MISSING,
                    f"{source.source_id}/{entity}",
                    emitted_at=now,
                    payload={"detail": str(exc)},
                )
                signals_posted += 1
                skipped.append(f"{source.source_id}/{entity}: {exc}")
                continue
            ratio = tombstone_ratio(len(result.tombstoned_keys), result.warehouse_key_count)
            payload = {
                "source_keys": result.source_key_count,
                "warehouse_keys": result.warehouse_key_count,
                "tombstoned": len(result.tombstoned_keys),
                "tombstone_ratio": ratio,
                "ran_at": result.ran_at.isoformat(),
            }
            board.post(
                SignalKind.RECONCILIATION_RUN,
                f"{source.source_id}/{entity}",
                emitted_at=now,
                payload=payload,
            )
            signals_posted += 1
            reconciliations.append(payload)
            tombstoned_total += len(result.tombstoned_keys)
            if result.tombstoned_keys:
                key_list = ", ".join(result.tombstoned_keys)
                context.log.warning(
                    f"{source.source_id}/{entity}: tombstoned "
                    f"{len(result.tombstoned_keys)} key(s): {key_list}"
                )
                board.post(
                    SignalKind.TOMBSTONE_BATCH,
                    f"{source.source_id}/{entity}",
                    emitted_at=now,
                    payload={"keys": list(result.tombstoned_keys)},
                )
                signals_posted += 1
    save_board(board, board_path, now)
    return MaterializeResult(
        metadata={
            "reconciled_entities": len(reconciliations),
            "tombstoned_keys": tombstoned_total,
            "reconciliations": MetadataValue.json(reconciliations),
            "skipped_without_key_scan": MetadataValue.json(skipped),
            "signals_posted": signals_posted,
        },
    )


@asset(name="exception_triage", deps=["delete_reconciliation"], tags={"layer": "control_plane"})
def exception_triage(context: AssetExecutionContext) -> MaterializeResult:
    """The exception agent: triage fresh reconciliation signals off the board.

    Mass-tombstone runs (tombstoned share at or above the alert ratio) are
    held for review instead of standing silently; normal runs are applied;
    missing key scans stay skipped-loud. Every verdict is posted back to the
    board as an ``EXCEPTION_DISPOSITION`` signal — the exchange between the
    two agents flows entirely through typed signals.
    """
    now = datetime.now(UTC)
    board_path = _board_path()
    board = load_board(board_path)
    dispositions = triage(board, now=now)
    save_board(board, board_path, now)
    context.log.info(
        "exception triage: " + "; ".join(f"{d.subject}={d.disposition}" for d in dispositions)
    )
    return MaterializeResult(
        metadata={
            "exceptions_triaged": len(dispositions),
            "held_for_review": MetadataValue.json(
                [d.subject for d in dispositions if d.disposition == "REVIEW"]
            ),
            "dispositions": MetadataValue.json(
                [{"subject": d.subject, "disposition": d.disposition} for d in dispositions]
            ),
        },
    )
