"""Typed records shared by the control plane and every connector."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class SourceConfig:
    """One registered source (an acquired dealer's feed) as declared in sources.yml."""

    source_id: str
    erp: str  # connector key, e.g. csv_sftp, netsuite, bistrack
    description: str
    settings: dict[str, str] = field(default_factory=dict)
    enabled: bool = True
    default_entities: tuple[str, ...] = ()  # empty = all connector entities


@dataclass(frozen=True)
class SourceRegistration:
    """Result of registering a source in the control-plane store."""

    source_id: str
    erp: str
    config_fingerprint: str
    registered_at: datetime


@dataclass(frozen=True)
class SyncCheckpoint:
    """Watermark for one source-entity pair — the incremental sync cursor.

    The semantics of `watermark` are connector-defined: a last-modified
    timestamp for API sources, a batch id or sequence for file drops.
    """

    source_id: str
    entity: str
    mode: str
    watermark: str | None
    updated_at: datetime


@dataclass(frozen=True)
class FileAuditRecord:
    """Append-only ingestion audit: one row per file seen by a file-based connector."""

    source_id: str
    batch_id: str
    file_name: str
    file_hash: str
    rows_declared: int | None
    rows_parsed: int | None
    status: str  # PROMOTED | SKIPPED_DUPLICATE | QUARANTINED
    reason_code: str | None
    processed_at: datetime


@dataclass(frozen=True)
class QuarantineRecord:
    """A file rejected before promotion, with a machine-readable reason."""

    source_id: str
    batch_id: str | None
    file_name: str
    reason_code: str
    detail: str
    quarantine_path: str
    quarantined_at: datetime


@dataclass(frozen=True)
class ReconciliationResult:
    """Outcome of one anti-join delete-reconciliation run (spec §6).

    ``tombstoned_keys`` are warehouse keys absent from the source key
    inventory — records the source has hard-deleted since the last load.
    """

    source_id: str
    entity: str
    delete_semantics: str  # anti_join | cdc_native
    source_key_count: int
    warehouse_key_count: int
    tombstoned_keys: tuple[str, ...]
    ran_at: datetime


@dataclass(frozen=True)
class TombstoneRecord:
    """One warehouse key tombstoned by reconciliation (hard delete at source)."""

    source_id: str
    entity: str
    source_key: str
    batch_id: str | None
    tombstoned_at: datetime
