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

from connectors.base import ExtractionMode
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
