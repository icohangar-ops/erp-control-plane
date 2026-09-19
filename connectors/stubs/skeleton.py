"""Shared machinery for documented skeleton connectors.

A skeleton connector is honest scaffolding: it declares the entities it will
feed, the documented extraction surface per entity (from the ERP landscape
research), and its credential prerequisites — but ``extract()`` raises
:class:`ConnectorNotImplemented` until per-tenant discovery is done. Skeletons
must never fabricate API behavior; everything they know is written down in
``describe_extraction`` and the module notes.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import ClassVar

from connectors.base import (
    BaseConnector,
    ConnectorError,
    ConnectorMaturity,
    ConnectorNotImplemented,
    ExtractionMode,
    ExtractionPlan,
)


class SkeletonConnector(BaseConnector):
    """Base for skeleton connectors — subclass and fill the ClassVars."""

    maturity = ConnectorMaturity.SKELETON
    #: entity -> documented extraction surface (where the data lives and how
    #: it is read). This is the content that makes a skeleton useful.
    extraction_surface: ClassVar[dict[str, str]] = {}
    #: entity -> incremental key the live implementation should use.
    incremental_keys: ClassVar[dict[str, str]] = {}
    #: settings that must be resolved (credentials/endpoints) before live use.
    required_settings: ClassVar[tuple[str, ...]] = ()

    def entities(self) -> list[str]:
        return list(self.extraction_surface)

    def validate_config(self) -> list[str]:
        missing = [f for f in self.required_settings if not self.source.settings.get(f)]
        if missing:
            return [
                f"missing settings required for live extraction: {', '.join(missing)} "
                "(see .env.example; the source stays disabled until discovered)"
            ]
        return []

    def describe_extraction(self, entity: str) -> ExtractionPlan:
        if entity not in self.extraction_surface:
            raise ConnectorError(
                f"entity '{entity}' is not declared by skeleton '{self.erp_id}'; "
                f"declared entities: {', '.join(sorted(self.extraction_surface))}"
            )
        return ExtractionPlan(
            entity=entity,
            surface=self.extraction_surface[entity],
            incremental_key=self.incremental_keys.get(entity),
            notes=self.extraction_notes,
        )

    def _iter_records(
        self, entity: str, mode: ExtractionMode, watermark: str | None
    ) -> Iterator[dict[str, object]]:
        raise ConnectorNotImplemented(
            f"connector '{self.erp_id}' is a documented skeleton: extract() is not "
            f"implemented until per-tenant discovery completes (see "
            f"docs/CONNECTOR_GUIDE.md). Planned surface for '{entity}': "
            f"{self.extraction_surface.get(entity, 'n/a')}"
        )
