"""The BaseConnector contract.

Every extraction connector — implemented or skeleton — derives from
:class:`BaseConnector`. The contract separates what varies per ERP (the
``_iter_records`` extraction surface) from what must never vary (provenance
stamping, watermark handling, Parquet promotion), so the control plane can
treat all sources uniformly:

* ``register()``      — validate config and persist the source registration.
* ``entities()``      — the canonical staging entities the connector can feed.
* ``validate_config()`` — credential/config gaps as human-readable problems.
* ``describe_extraction(entity)`` — the extraction plan (surface, incremental
  key, caveats) without touching the network; this is how skeleton connectors
  stay testable without fabricating API behavior.
* ``extract(entity, mode)`` — template method: reads the stored watermark,
  streams records from ``_iter_records``, stamps provenance, writes Parquet
  under the lake root, and persists the new watermark.

Maturity is explicit: ``ConnectorMaturity.IMPLEMENTED`` connectors must pass
the full behavioral contract suite (see tests/test_connector_contract.py);
``ConnectorMaturity.SKELETON`` connectors are documented, importable, and
plannable but their ``extract()`` raises :class:`ConnectorNotImplemented`
until per-tenant discovery is done.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from hashlib import sha256
from pathlib import Path
from typing import ClassVar

import pyarrow as pa
import pyarrow.parquet as pq

from control_plane.config import ControlPlaneConfig
from control_plane.models import SourceConfig, SourceRegistration, SyncCheckpoint
from control_plane.store import ControlPlaneStore

#: Provenance columns stamped onto every staging record. Facts and dimensions
#: in the canonical model carry these through to enable audit back to source.
PROVENANCE_COLUMNS: tuple[str, ...] = (
    "source_system",
    "source_id",
    "source_file",
    "source_row_no",
    "batch_id",
    "loaded_at",
)

#: Stamped by the connector itself when it has them (file connectors do;
#: API sources fold document identity into source_id instead).
CONNECTOR_STAMPED_COLUMNS: tuple[str, ...] = ("source_file", "source_row_no", "batch_id")

#: Stamped by the base class — connectors must not set these.
BASE_STAMPED_COLUMNS: tuple[str, ...] = ("source_system", "source_id", "loaded_at")

PARQUET_BATCH_SIZE = 25_000


class ExtractionMode(str, Enum):
    """Backfill = full history; Incremental = watermark-based delta."""

    BACKFILL = "backfill"
    INCREMENTAL = "incremental"


class ConnectorMaturity(str, Enum):
    IMPLEMENTED = "implemented"
    SKELETON = "skeleton"


class ConnectorError(RuntimeError):
    """Base class for connector failures — never silently swallowed."""


class ConnectorNotConfigured(ConnectorError):
    """The connector is missing credentials or mandatory settings."""


class ConnectorNotImplemented(ConnectorError):
    """The connector is a documented skeleton; per-tenant work remains."""


@dataclass(frozen=True)
class ExtractionPlan:
    """What a connector *would* do for an entity — the testable surface of skeletons."""

    entity: str
    surface: str  # e.g. "SuiteQL REST query", "SQL Server ODBC read-only", "SFTP CSV drop"
    incremental_key: str | None
    notes: str


@dataclass
class ExtractedEntity:
    source_id: str
    entity: str
    mode: str
    parquet_path: str
    rows_extracted: int
    watermark_before: str | None
    watermark_after: str | None


def config_fingerprint(settings: Mapping[str, str]) -> str:
    """Stable hash of resolved settings — detects config drift between runs."""
    canonical = json.dumps(sorted((k, str(v)) for k, v in settings.items()), sort_keys=True)
    return sha256(canonical.encode("utf-8")).hexdigest()[:16]


class BaseConnector(ABC):
    """Contract every source connector implements. See module docstring."""

    erp_id: ClassVar[str]
    maturity: ClassVar[ConnectorMaturity]
    #: Research-backed extraction notes; required reading before implementation.
    extraction_notes: ClassVar[str] = ""
    #: entity -> columns forming the natural source key (used for source_id stamping).
    natural_key_fields: ClassVar[dict[str, tuple[str, ...]]] = {}

    def __init__(
        self, source: SourceConfig, store: ControlPlaneStore, config: ControlPlaneConfig
    ) -> None:
        self.source = source
        self.store = store
        self.config = config

    # ------------------------------------------------------------------
    # Contract surface
    # ------------------------------------------------------------------

    @abstractmethod
    def entities(self) -> list[str]: ...

    @abstractmethod
    def validate_config(self) -> list[str]:
        """Return a list of human-readable configuration problems (empty = ready)."""

    @abstractmethod
    def describe_extraction(self, entity: str) -> ExtractionPlan: ...

    @abstractmethod
    def _iter_records(
        self, entity: str, mode: ExtractionMode, watermark: str | None
    ) -> Iterator[dict[str, object]]:
        """Yield canonical staging records (plain dicts) for the entity."""

    # ------------------------------------------------------------------
    # Shared behavior (template method)
    # ------------------------------------------------------------------

    def register(self) -> SourceRegistration:
        problems = self.validate_config()
        if problems:
            raise ConnectorNotConfigured(
                f"source {self.source.source_id} ({self.erp_id}) is not configurable: "
                + "; ".join(problems)
            )
        fingerprint = config_fingerprint(self.source.settings)
        return self.store.register_source(self.source, fingerprint)

    def extract(self, entity: str, mode: ExtractionMode = ExtractionMode.BACKFILL) -> ExtractedEntity:
        if entity not in self.entities():
            raise ConnectorError(
                f"entity '{entity}' is not exposed by {self.erp_id}; "
                f"available: {', '.join(self.entities())}"
            )
        watermark_before = self.store.get_watermark(self.source.source_id, entity, mode.value)
        parquet_path = self._parquet_path(entity)
        rows = self._write_records_to_parquet(entity, parquet_path, mode, watermark_before)
        watermark_after = self.current_watermark(entity, mode, watermark_before)
        self.store.upsert_watermark(
            SyncCheckpoint(
                source_id=self.source.source_id,
                entity=entity,
                mode=mode.value,
                watermark=watermark_after,
                updated_at=datetime.now(UTC),
            )
        )
        return ExtractedEntity(
            source_id=self.source.source_id,
            entity=entity,
            mode=mode.value,
            parquet_path=str(parquet_path),
            rows_extracted=rows,
            watermark_before=watermark_before,
            watermark_after=watermark_after,
        )

    def current_watermark(
        self, entity: str, mode: ExtractionMode, watermark_before: str | None
    ) -> str | None:
        """Watermark to persist after a successful extract. Default: unchanged."""
        return watermark_before

    def arrow_schema(self, entity: str) -> pa.Schema | None:
        """Declared Arrow schema for an entity's staging Parquet, if known.

        Returning an explicit schema avoids inferring types from the first
        extracted row (decimals, dates, and nulls make that unstable across
        batches). Connectors with a typed schema registry (e.g. csv_sftp's
        schemas.yml) should implement this; the default infers from data.
        """
        return None

    def dry_run(self) -> dict[str, object]:
        """Plan the full extraction without touching any system.

        Credential-gated and skeleton connectors must be plannable without a
        live tenant — this is what the contract tests assert on.
        """
        problems = self.validate_config()
        return {
            "source_id": self.source.source_id,
            "erp": self.erp_id,
            "maturity": self.maturity.value,
            "config_problems": problems,
            "entities": [self.describe_extraction(e).__dict__ for e in self.entities()],
        }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _parquet_path(self, entity: str) -> Path:
        # One Parquet file per entity per run of the current batch; the demo
        # batch is a full export, so the latest file replaces prior ones.
        # Incremental API sources append watermark-partitioned parts in v0.2.
        return self.config.parquet_root(self.source.source_id) / f"{entity}.parquet"

    def _write_records_to_parquet(
        self, entity: str, parquet_path: Path, mode: ExtractionMode, watermark: str | None
    ) -> int:
        """Stream ``_iter_records`` through provenance stamping into Parquet."""
        rows = 0
        batch_iter = self._iter_records(entity, mode, watermark)
        first = next(batch_iter, None)
        if first is None:
            return 0
        stamped = self._stamp(first, entity)
        declared = self.arrow_schema(entity)
        schema = declared if declared is not None else pa.Table.from_pylist([stamped]).schema
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        pending: list[dict[str, object]] = [stamped]
        rows += 1
        with pq.ParquetWriter(parquet_path, schema) as writer:
            for record in batch_iter:
                pending.append(self._stamp(record, entity))
                rows += 1
                if len(pending) >= PARQUET_BATCH_SIZE:
                    writer.write_table(pa.Table.from_pylist(pending, schema=schema))
                    pending = []
            if pending:
                writer.write_table(pa.Table.from_pylist(pending, schema=schema))
        return rows

    def _stamp(self, record: dict[str, object], entity: str) -> dict[str, object]:
        """Enrich a raw record with provenance stamps (see PROVENANCE_COLUMNS)."""
        collisions = [c for c in BASE_STAMPED_COLUMNS if c in record]
        if collisions:
            raise ConnectorError(
                f"record for {self.source.source_id}/{entity} already carries base-stamped "
                f"provenance columns {collisions}; connectors must not set them"
            )
        key_fields = self.natural_key_fields.get(entity)
        if not key_fields:
            raise ConnectorError(
                f"connector {self.erp_id} does not declare natural_key_fields for entity '{entity}'"
            )
        missing = [f for f in key_fields if f not in record]
        if missing:
            raise ConnectorError(
                f"record for {self.source.source_id}/{entity} is missing natural key fields {missing}"
            )
        natural_id = ":".join(str(record[f]) for f in key_fields)
        stamped: dict[str, object] = {**record}
        stamped["source_system"] = self.erp_id
        stamped["source_id"] = natural_id
        stamped["loaded_at"] = datetime.now(UTC).isoformat()
        return stamped
