"""Control-plane metadata store: source registry, sync state, file audit, quarantine.

Postgres is the deployment backend (see docs/ARCHITECTURE.md — transactional
guarantees belong to registry/state data). SQLite is the zero-dependency demo
path so `make demo` runs without any containers. Both backends implement the
same `ControlPlaneStore` protocol; the DDL is deliberately portable
(TEXT/INTEGER/TIMESTAMP only) so one code path serves both.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from control_plane.config import POSTGRES_BACKEND, SQLITE_BACKEND, ControlPlaneConfig
from control_plane.models import (
    FileAuditRecord,
    QuarantineRecord,
    SourceConfig,
    SourceRegistration,
    SyncCheckpoint,
)

# Portable DDL: valid on SQLite and Postgres alike.
DDL = [
    """
    CREATE TABLE IF NOT EXISTS source_registry (
        source_id          TEXT PRIMARY KEY,
        erp                TEXT NOT NULL,
        description        TEXT,
        settings_json      TEXT NOT NULL DEFAULT '{}',
        config_fingerprint TEXT NOT NULL,
        enabled            INTEGER NOT NULL DEFAULT 1,
        registered_at      TIMESTAMP NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS sync_state (
        source_id   TEXT NOT NULL,
        entity      TEXT NOT NULL,
        mode        TEXT NOT NULL,
        watermark   TEXT,
        updated_at  TIMESTAMP NOT NULL,
        PRIMARY KEY (source_id, entity, mode)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS file_audit (
        source_id     TEXT NOT NULL,
        batch_id      TEXT NOT NULL,
        file_name     TEXT NOT NULL,
        file_hash     TEXT NOT NULL,
        rows_declared INTEGER,
        rows_parsed   INTEGER,
        status        TEXT NOT NULL,
        reason_code   TEXT,
        processed_at  TIMESTAMP NOT NULL,
        PRIMARY KEY (source_id, file_hash)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quarantine (
        quarantine_id   TEXT PRIMARY KEY,
        source_id       TEXT NOT NULL,
        batch_id        TEXT,
        file_name       TEXT NOT NULL,
        reason_code     TEXT NOT NULL,
        detail          TEXT,
        quarantine_path TEXT,
        quarantined_at  TIMESTAMP NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS data_quality_results (
        result_id     TEXT PRIMARY KEY,
        source_id     TEXT NOT NULL,
        entity        TEXT NOT NULL,
        check_name    TEXT NOT NULL,
        severity      TEXT NOT NULL,
        status        TEXT NOT NULL,
        detail        TEXT,
        evaluated_at  TIMESTAMP NOT NULL
    )
    """,
]


def _now() -> datetime:
    return datetime.now(UTC)


class ControlPlaneStore(ABC):
    """Contract for the control-plane metadata backend."""

    @abstractmethod
    def initialize(self) -> None: ...

    @abstractmethod
    def register_source(self, source: SourceConfig, fingerprint: str) -> SourceRegistration: ...

    @abstractmethod
    def get_source(self, source_id: str) -> SourceConfig | None: ...

    @abstractmethod
    def list_sources(self) -> list[SourceConfig]: ...

    @abstractmethod
    def upsert_watermark(self, checkpoint: SyncCheckpoint) -> None: ...

    @abstractmethod
    def get_watermark(self, source_id: str, entity: str, mode: str) -> str | None: ...

    @abstractmethod
    def record_file_audit(self, record: FileAuditRecord) -> None: ...

    @abstractmethod
    def has_file_been_processed(self, source_id: str, file_hash: str) -> bool: ...

    @abstractmethod
    def record_quarantine(self, record: QuarantineRecord) -> None: ...

    @abstractmethod
    def list_quarantine(self, source_id: str | None = None) -> list[QuarantineRecord]: ...

    @abstractmethod
    def record_quality_result(
        self, source_id: str, entity: str, check_name: str, severity: str, status: str, detail: str
    ) -> None: ...

    def close(self) -> None:  # optional on backends
        return None


def _registration_from(source: SourceConfig, fingerprint: str) -> SourceRegistration:
    return SourceRegistration(
        source_id=source.source_id,
        erp=source.erp,
        config_fingerprint=fingerprint,
        registered_at=_now(),
    )


def _settings_from_json(raw: Any) -> dict[str, str]:
    parsed = json.loads(raw) if isinstance(raw, str) else (raw or {})
    return {str(k): str(v) for k, v in parsed.items()}


class SqliteControlPlaneStore(ControlPlaneStore):
    """Demo-path backend: a single-file SQLite database, stdlib only."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._conn: sqlite3.Connection | None = None

    def _connection(self) -> sqlite3.Connection:
        if self._conn is None:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def initialize(self) -> None:
        with self._connection() as conn:
            for statement in DDL:
                conn.execute(statement)

    def register_source(self, source: SourceConfig, fingerprint: str) -> SourceRegistration:
        registration = _registration_from(source, fingerprint)
        with self._connection() as conn:
            conn.execute(
                """
                INSERT INTO source_registry
                    (source_id, erp, description, settings_json, config_fingerprint, enabled, registered_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_id) DO UPDATE SET
                    erp=excluded.erp, description=excluded.description,
                    settings_json=excluded.settings_json,
                    config_fingerprint=excluded.config_fingerprint,
                    enabled=excluded.enabled, registered_at=excluded.registered_at
                """,
                (
                    source.source_id,
                    source.erp,
                    source.description,
                    json.dumps(source.settings),
                    fingerprint,
                    int(source.enabled),
                    registration.registered_at.isoformat(),
                ),
            )
        return registration

    def get_source(self, source_id: str) -> SourceConfig | None:
        row = (
            self._connection()
            .execute("SELECT * FROM source_registry WHERE source_id = ?", (source_id,))
            .fetchone()
        )
        return self._source_from_row(row) if row else None

    def list_sources(self) -> list[SourceConfig]:
        rows = (
            self._connection()
            .execute("SELECT * FROM source_registry ORDER BY source_id")
            .fetchall()
        )
        return [self._source_from_row(row) for row in rows]

    @staticmethod
    def _source_from_row(row: Any) -> SourceConfig:
        return SourceConfig(
            source_id=row["source_id"],
            erp=row["erp"],
            description=row["description"] or "",
            settings=_settings_from_json(row["settings_json"]),
            enabled=bool(row["enabled"]),
        )

    def upsert_watermark(self, checkpoint: SyncCheckpoint) -> None:
        with self._connection() as conn:
            conn.execute(
                """
                INSERT INTO sync_state (source_id, entity, mode, watermark, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(source_id, entity, mode) DO UPDATE SET
                    watermark=excluded.watermark, updated_at=excluded.updated_at
                """,
                (
                    checkpoint.source_id,
                    checkpoint.entity,
                    checkpoint.mode,
                    checkpoint.watermark,
                    checkpoint.updated_at.isoformat(),
                ),
            )

    def get_watermark(self, source_id: str, entity: str, mode: str) -> str | None:
        row = (
            self._connection()
            .execute(
                "SELECT watermark FROM sync_state WHERE source_id = ? AND entity = ? AND mode = ?",
                (source_id, entity, mode),
            )
            .fetchone()
        )
        return row["watermark"] if row else None

    def record_file_audit(self, record: FileAuditRecord) -> None:
        with self._connection() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO file_audit
                    (source_id, batch_id, file_name, file_hash, rows_declared, rows_parsed,
                     status, reason_code, processed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.source_id,
                    record.batch_id,
                    record.file_name,
                    record.file_hash,
                    record.rows_declared,
                    record.rows_parsed,
                    record.status,
                    record.reason_code,
                    record.processed_at.isoformat(),
                ),
            )

    def has_file_been_processed(self, source_id: str, file_hash: str) -> bool:
        row = (
            self._connection()
            .execute(
                "SELECT 1 FROM file_audit WHERE source_id = ? AND file_hash = ?",
                (source_id, file_hash),
            )
            .fetchone()
        )
        return row is not None

    def record_quarantine(self, record: QuarantineRecord) -> None:
        with self._connection() as conn:
            conn.execute(
                """
                INSERT INTO quarantine
                    (quarantine_id, source_id, batch_id, file_name, reason_code, detail,
                     quarantine_path, quarantined_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    uuid.uuid4().hex,
                    record.source_id,
                    record.batch_id,
                    record.file_name,
                    record.reason_code,
                    record.detail,
                    record.quarantine_path,
                    record.quarantined_at.isoformat(),
                ),
            )

    def list_quarantine(self, source_id: str | None = None) -> list[QuarantineRecord]:
        if source_id:
            rows = (
                self._connection()
                .execute(
                    "SELECT * FROM quarantine WHERE source_id = ? ORDER BY quarantined_at",
                    (source_id,),
                )
                .fetchall()
            )
        else:
            rows = (
                self._connection()
                .execute("SELECT * FROM quarantine ORDER BY quarantined_at")
                .fetchall()
            )
        return [
            QuarantineRecord(
                source_id=r["source_id"],
                batch_id=r["batch_id"],
                file_name=r["file_name"],
                reason_code=r["reason_code"],
                detail=r["detail"] or "",
                quarantine_path=r["quarantine_path"] or "",
                quarantined_at=datetime.fromisoformat(r["quarantined_at"]),
            )
            for r in rows
        ]

    def record_quality_result(
        self, source_id: str, entity: str, check_name: str, severity: str, status: str, detail: str
    ) -> None:
        with self._connection() as conn:
            conn.execute(
                """
                INSERT INTO data_quality_results
                    (result_id, source_id, entity, check_name, severity, status, detail, evaluated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    uuid.uuid4().hex,
                    source_id,
                    entity,
                    check_name,
                    severity,
                    status,
                    detail,
                    _now().isoformat(),
                ),
            )

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


class PostgresControlPlaneStore(ControlPlaneStore):
    """Deployment backend: Postgres via psycopg2 (imported lazily so the demo
    path never needs the driver installed)."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self._conn: Any = None

    def _connection(self) -> Any:
        if self._conn is None:
            import psycopg2

            self._conn = psycopg2.connect(self.dsn)
        return self._conn

    def initialize(self) -> None:
        conn = self._connection()
        with conn.cursor() as cur:
            for statement in DDL:
                cur.execute(statement)
        conn.commit()

    def register_source(self, source: SourceConfig, fingerprint: str) -> SourceRegistration:
        registration = _registration_from(source, fingerprint)
        conn = self._connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO source_registry
                    (source_id, erp, description, settings_json, config_fingerprint, enabled, registered_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_id) DO UPDATE SET
                    erp=EXCLUDED.erp, description=EXCLUDED.description,
                    settings_json=EXCLUDED.settings_json,
                    config_fingerprint=EXCLUDED.config_fingerprint,
                    enabled=EXCLUDED.enabled, registered_at=EXCLUDED.registered_at
                """,
                (
                    source.source_id,
                    source.erp,
                    source.description,
                    json.dumps(source.settings),
                    fingerprint,
                    int(source.enabled),
                    registration.registered_at,
                ),
            )
        conn.commit()
        return registration

    def get_source(self, source_id: str) -> SourceConfig | None:
        with self._connection().cursor() as cur:
            cur.execute(
                "SELECT source_id, erp, description, settings_json, enabled FROM source_registry WHERE source_id = %s",
                (source_id,),
            )
            row = cur.fetchone()
        if not row:
            return None
        return SourceConfig(
            source_id=row[0],
            erp=row[1],
            description=row[2] or "",
            settings=_settings_from_json(row[3]),
            enabled=bool(row[4]),
        )

    def list_sources(self) -> list[SourceConfig]:
        with self._connection().cursor() as cur:
            cur.execute(
                "SELECT source_id, erp, description, settings_json, enabled FROM source_registry ORDER BY source_id"
            )
            rows = cur.fetchall()
        return [
            SourceConfig(
                source_id=r[0],
                erp=r[1],
                description=r[2] or "",
                settings=_settings_from_json(r[3]),
                enabled=bool(r[4]),
            )
            for r in rows
        ]

    def upsert_watermark(self, checkpoint: SyncCheckpoint) -> None:
        conn = self._connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sync_state (source_id, entity, mode, watermark, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (source_id, entity, mode) DO UPDATE SET
                    watermark=EXCLUDED.watermark, updated_at=EXCLUDED.updated_at
                """,
                (
                    checkpoint.source_id,
                    checkpoint.entity,
                    checkpoint.mode,
                    checkpoint.watermark,
                    checkpoint.updated_at,
                ),
            )
        conn.commit()

    def get_watermark(self, source_id: str, entity: str, mode: str) -> str | None:
        with self._connection().cursor() as cur:
            cur.execute(
                "SELECT watermark FROM sync_state WHERE source_id = %s AND entity = %s AND mode = %s",
                (source_id, entity, mode),
            )
            row = cur.fetchone()
        return row[0] if row else None

    def record_file_audit(self, record: FileAuditRecord) -> None:
        conn = self._connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO file_audit
                    (source_id, batch_id, file_name, file_hash, rows_declared, rows_parsed,
                     status, reason_code, processed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_id, file_hash) DO NOTHING
                """,
                (
                    record.source_id,
                    record.batch_id,
                    record.file_name,
                    record.file_hash,
                    record.rows_declared,
                    record.rows_parsed,
                    record.status,
                    record.reason_code,
                    record.processed_at,
                ),
            )
        conn.commit()

    def has_file_been_processed(self, source_id: str, file_hash: str) -> bool:
        with self._connection().cursor() as cur:
            cur.execute(
                "SELECT 1 FROM file_audit WHERE source_id = %s AND file_hash = %s LIMIT 1",
                (source_id, file_hash),
            )
            return cur.fetchone() is not None

    def record_quarantine(self, record: QuarantineRecord) -> None:
        conn = self._connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quarantine
                    (quarantine_id, source_id, batch_id, file_name, reason_code, detail,
                     quarantine_path, quarantined_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    uuid.uuid4().hex,
                    record.source_id,
                    record.batch_id,
                    record.file_name,
                    record.reason_code,
                    record.detail,
                    record.quarantine_path,
                    record.quarantined_at,
                ),
            )
        conn.commit()

    def list_quarantine(self, source_id: str | None = None) -> list[QuarantineRecord]:
        with self._connection().cursor() as cur:
            if source_id:
                cur.execute(
                    "SELECT * FROM quarantine WHERE source_id = %s ORDER BY quarantined_at",
                    (source_id,),
                )
            else:
                cur.execute("SELECT * FROM quarantine ORDER BY quarantined_at")
            rows = cur.fetchall()
        return [
            QuarantineRecord(
                source_id=r[1],
                batch_id=r[2],
                file_name=r[3],
                reason_code=r[4],
                detail=r[5] or "",
                quarantine_path=r[6] or "",
                quarantined_at=r[7]
                if isinstance(r[7], datetime)
                else datetime.fromisoformat(str(r[7])),
            )
            for r in rows
        ]

    def record_quality_result(
        self, source_id: str, entity: str, check_name: str, severity: str, status: str, detail: str
    ) -> None:
        conn = self._connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO data_quality_results
                    (result_id, source_id, entity, check_name, severity, status, detail, evaluated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (uuid.uuid4().hex, source_id, entity, check_name, severity, status, detail, _now()),
            )
        conn.commit()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


def open_store(config: ControlPlaneConfig) -> ControlPlaneStore:
    """Open the configured control-plane backend, initializing its schema."""
    store: ControlPlaneStore
    if config.backend == SQLITE_BACKEND:
        if config.sqlite_path is None:
            raise ValueError("CONTROL_PLANE_SQLITE_PATH must be set when backend is sqlite")
        store = SqliteControlPlaneStore(config.sqlite_path)
    elif config.backend == POSTGRES_BACKEND:
        assert config.control_plane_dsn is not None  # validated in from_env
        store = PostgresControlPlaneStore(config.control_plane_dsn)
    else:
        raise ValueError(f"Unknown control-plane backend: {config.backend}")
    store.initialize()
    return store
