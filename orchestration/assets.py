"""Extraction assets: source registration plus one asset per (source, entity).

Asset keys are ``[<source_id>, <entity>]`` -- the same keys dagster-dbt derives
for the matching dbt external sources, so dbt models inherit lineage edges for
free and the demo job runs extraction before the dbt build automatically.
"""

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
from control_plane.store import open_store


def _config() -> ControlPlaneConfig:
    return ControlPlaneConfig.from_env()


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
    silently (the skip is counted and surfaced in the asset metadata).
    """
    config = _config()
    store = open_store(config)
    reconciled: list[dict[str, object]] = []
    skipped: list[str] = []
    total_tombstoned = 0
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
                skipped.append(f"{source.source_id}/{entity}: {exc}")
                continue
            total_tombstoned += len(result.tombstoned_keys)
            reconciled.append(
                {
                    "source_id": result.source_id,
                    "entity": result.entity,
                    "source_keys": result.source_key_count,
                    "warehouse_keys": result.warehouse_key_count,
                    "tombstoned": list(result.tombstoned_keys),
                    "ran_at": result.ran_at.isoformat(),
                }
            )
            if result.tombstoned_keys:
                key_list = ", ".join(result.tombstoned_keys)
                context.log.warning(
                    f"{source.source_id}/{entity}: tombstoned "
                    f"{len(result.tombstoned_keys)} key(s): {key_list}"
                )
    return MaterializeResult(
        metadata={
            "reconciled_entities": len(reconciled),
            "tombstoned_keys": total_tombstoned,
            "reconciliations": MetadataValue.json(reconciled),
            "skipped_without_key_scan": MetadataValue.json(skipped),
        },
    )
