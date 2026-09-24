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
import uuid
from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from hashlib import sha256
from pathlib import Path
from typing import ClassVar

import pyarrow as pa
import pyarrow.parquet as pq

from control_plane.config import ControlPlaneConfig
from control_plane.models import (
    ReconciliationResult,
    SourceConfig,
    SourceRegistration,
    SyncCheckpoint,
)
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


class ExtractionMode(StrEnum):
    """Backfill = full history; Incremental = watermark-based delta."""

    BACKFILL = "backfill"
    INCREMENTAL = "incremental"


class ConnectorMaturity(StrEnum):
    IMPLEMENTED = "implemented"
    SKELETON = "skeleton"


class DeleteSemantics(StrEnum):
    """How hard deletes from the source reach the warehouse (spec §6).

    ANTI_JOIN — batch merge/upserts never see hard deletes; a scheduled
    key-inventory anti-join (:meth:`BaseConnector.reconcile_deletes`) finds
    warehouse keys the source no longer has and tombstones them. This is the
    default and applies to every batch path — it is part of the connector
    contract, not an optional extra.
    CDC_NATIVE — the change stream carries delete events; the anti-join is
    not applicable and ``reconcile_deletes`` refuses to run.
    """

    ANTI_JOIN = "anti_join"
    CDC_NATIVE = "cdc_native"


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


def natural_id_for(key_fields: tuple[str, ...], record: Mapping[str, object]) -> str:
    """Canonical natural-key string for a record — ':'-joined key field values.

    The single definition of a source's natural identity: provenance stamping
    (``_stamp``) and key-inventory scans (``source_key_inventory``) must agree
    on this form or the anti-join would compare incompatible id spaces.
    """
    return ":".join(str(record[field]) for field in key_fields)


class BaseConnector(ABC):
    """Contract every source connector implements. See module docstring."""

    erp_id: ClassVar[str]
    maturity: ClassVar[ConnectorMaturity]
    #: How hard deletes reach the warehouse (spec §6). Batch paths run the
    #: anti-join contract method below; CDC paths carry delete events natively
    #: and refuse the anti-join.
    delete_handling: ClassVar[DeleteSemantics] = DeleteSemantics.ANTI_JOIN
    #: True when an extraction is a full snapshot of the source entity (the
    #: staged Parquet is replaced wholesale) — extract() then diffs the
    #: previous snapshot's keys automatically at promotion time (spec §5's
    #: "missing-key diff in newest snapshot = delete").
    full_snapshot: ClassVar[bool] = False
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

    def extract(
        self, entity: str, mode: ExtractionMode = ExtractionMode.BACKFILL
    ) -> ExtractedEntity:
        if entity not in self.entities():
            raise ConnectorError(
                f"entity '{entity}' is not exposed by {self.erp_id}; "
                f"available: {', '.join(self.entities())}"
            )
        watermark_before = self.store.get_watermark(self.source.source_id, entity, mode.value)
        parquet_path = self._parquet_path(entity)
        old_warehouse_keys = (
            self.warehouse_key_inventory(entity)
            if self.full_snapshot and mode is ExtractionMode.BACKFILL
            else None
        )
        rows = self._write_records_to_parquet(entity, parquet_path, mode, watermark_before)
        if old_warehouse_keys is not None and rows > 0:
            # A zero-row extract is never treated as "the source deleted
            # everything" — for file sources that state is a quarantined empty
            # drop, for SQL sources it is far more likely a fault than a mass
            # delete.
            self._reconcile_full_snapshot(entity, old_warehouse_keys, parquet_path)
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

    # ------------------------------------------------------------------
    # Delete reconciliation (spec §6 cross-cutting rule)
    # ------------------------------------------------------------------

    def source_key_inventory(self, entity: str) -> set[str]:
        """Natural keys the source holds for an entity, right now.

        A cheap full-key scan of the source (key columns only, never whole
        rows) in the canonical natural-id form. Batch connectors override
        this; the default has no source surface to scan.
        """
        raise ConnectorNotImplemented(
            f"connector '{self.erp_id}' does not implement a key-inventory scan for "
            f"'{entity}'; the anti-join delete reconciliation needs one (spec §6)"
        )

    def warehouse_key_inventory(self, entity: str) -> set[str]:
        """Natural keys currently staged in the warehouse for an entity."""
        parquet_path = self._parquet_path(entity)
        if not parquet_path.exists():
            return set()
        keys = pq.read_table(parquet_path, columns=["source_id"]).column("source_id").to_pylist()
        return {str(key) for key in keys}

    def reconcile_deletes(self, entity: str, batch_id: str | None = None) -> ReconciliationResult:
        """Anti-join: warehouse keys absent from the source -> tombstones.

        The scheduled job behind spec §6's cross-cutting rule — every batch
        path reconciles deletes; it is part of the connector contract, not an
        optional extra. Direction note: the tombstoned set is warehouse keys
        the source no longer returns ("missing-key diff in newest snapshot =
        delete", spec §5 file-source row); keys not yet extracted are simply
        new, never tombstones.
        """
        if self.delete_handling is not DeleteSemantics.ANTI_JOIN:
            raise ConnectorError(
                f"connector '{self.erp_id}' carries deletes natively via change data "
                f"capture; the anti-join reconciliation is not applicable for '{entity}'"
            )
        if entity not in self.entities():
            raise ConnectorError(
                f"entity '{entity}' is not exposed by {self.erp_id}; "
                f"available: {', '.join(self.entities())}"
            )
        source_keys = self.source_key_inventory(entity)
        warehouse_keys = self.warehouse_key_inventory(entity)
        result = ReconciliationResult(
            source_id=self.source.source_id,
            entity=entity,
            delete_semantics=self.delete_handling.value,
            source_key_count=len(source_keys),
            warehouse_key_count=len(warehouse_keys),
            tombstoned_keys=tuple(sorted(warehouse_keys - source_keys)),
            ran_at=datetime.now(UTC),
        )
        self.store.record_reconciliation(result, batch_id)
        return result

    def _reconcile_full_snapshot(
        self, entity: str, old_warehouse_keys: set[str], parquet_path: Path
    ) -> ReconciliationResult:
        """Diff the previous snapshot's keys against the snapshot just staged.

        Full-snapshot loads replace the staged entity wholesale, so the only
        place the previous snapshot's key inventory exists is the pre-replace
        Parquet — captured by extract() before the write.
        """
        new_keys = {
            str(k)
            for k in pq.read_table(parquet_path, columns=["source_id"])
            .column("source_id")
            .to_pylist()
        }
        result = ReconciliationResult(
            source_id=self.source.source_id,
            entity=entity,
            delete_semantics=DeleteSemantics.ANTI_JOIN.value,
            source_key_count=len(new_keys),
            warehouse_key_count=len(old_warehouse_keys),
            tombstoned_keys=tuple(sorted(old_warehouse_keys - new_keys)),
            ran_at=datetime.now(UTC),
        )
        self.store.record_reconciliation(result, self._snapshot_batch_id())
        return result

    def _snapshot_batch_id(self) -> str | None:
        """Batch id to attribute a full-snapshot reconciliation to, if known."""
        return None

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

    def stamped_records(
        self, entity: str, mode: ExtractionMode, watermark: str | None
    ) -> Iterator[dict[str, object]]:
        """Yield provenance-stamped records — the shared ingestion shape.

        Every consumer of an extraction (the Parquet writer here, the dlt
        resource wrapper, tests) streams through this one surface so staging
        output cannot drift between them.
        """
        for record in self._iter_records(entity, mode, watermark):
            yield self._stamp(record, entity)

    def _write_records_to_parquet(
        self, entity: str, parquet_path: Path, mode: ExtractionMode, watermark: str | None
    ) -> int:
        """Stream ``stamped_records`` into Parquet (records arrive stamped)."""
        rows = 0
        batch_iter = self.stamped_records(entity, mode, watermark)
        first = next(batch_iter, None)
        if first is None:
            return 0
        declared = self.arrow_schema(entity)
        schema = declared if declared is not None else pa.Table.from_pylist([first]).schema
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        # Incremental runs must not replace the historical entity file. Write
        # the delta to a unique sibling first, then atomically publish the
        # merged file after the complete batch has been materialized.
        incremental = mode is ExtractionMode.INCREMENTAL and parquet_path.exists()
        output_path = (
            parquet_path.with_name(f".{parquet_path.name}.{uuid.uuid4().hex}.tmp")
            if incremental
            else parquet_path
        )
        pending: list[dict[str, object]] = [first]
        rows += 1
        with pq.ParquetWriter(output_path, schema) as writer:
            for record in batch_iter:
                pending.append(record)
                rows += 1
                if len(pending) >= PARQUET_BATCH_SIZE:
                    writer.write_table(pa.Table.from_pylist(pending, schema=schema))
                    pending = []
            if pending:
                writer.write_table(pa.Table.from_pylist(pending, schema=schema))
        if incremental:
            try:
                historical = pq.read_table(parquet_path)
                delta = pq.read_table(output_path)
                merged = pa.concat_tables([historical, delta], promote=True)
                merged_path = output_path.with_suffix(".merged.parquet")
                pq.write_table(merged, merged_path)
                merged_path.replace(parquet_path)
            finally:
                output_path.unlink(missing_ok=True)
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
        natural_id = natural_id_for(key_fields, record)
        stamped: dict[str, object] = {**record}
        stamped["source_system"] = self.erp_id
        stamped["source_id"] = natural_id
        stamped["loaded_at"] = datetime.now(UTC).isoformat()
        return stamped
