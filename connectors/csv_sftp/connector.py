"""csv_sftp — validated CSV drop / SFTP connector (IMPLEMENTED, drives the demo).

The most common first-integration surface for an acquired dealer: nightly CSV
exports dropped on SFTP (or a watched folder) with a manifest control file.
This connector is the reference implementation of the ingestion pattern from
the research doc:

1. manifest arrives and validates structurally (batch completeness signal)
2. per-file gates: present -> non-empty -> checksum match -> header matches
   the approved schema registry -> every row casts -> row count matches
3. any gate failure quarantines the file (byte copy + machine-readable reason)
   and raises — a batch is promoted whole or not at all
4. idempotency: SHA-256 file hash checked against the control-plane audit
   table before parsing; re-delivered files are skipped
5. promoted rows are streamed to Parquet with full provenance stamps

For a real SFTP endpoint, the drop directory is fetched with paramiko (see
CONNECTOR_GUIDE.md); v0.1 reads a local directory so the demo path needs no
credentials. The validation/promotion/idempotency logic is identical — only
the file acquisition step is pluggable. Files are loaded fully into memory
during validation; multi-GB drops should stream to a temp dir first (noted in
CONNECTOR_GUIDE.md).
"""

from __future__ import annotations

import csv
import json
import shutil
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from importlib import resources
from io import StringIO
from pathlib import Path
from typing import ClassVar

import pyarrow as pa
import yaml

from connectors.base import (
    BaseConnector,
    ConnectorError,
    ConnectorMaturity,
    ExtractedEntity,
    ExtractionMode,
    ExtractionPlan,
)
from control_plane.config import ControlPlaneConfig
from control_plane.models import FileAuditRecord, QuarantineRecord, SourceConfig
from control_plane.store import ControlPlaneStore

SCHEMAS_PATH = resources.files("connectors.csv_sftp") / "schemas.yml"

AUDIT_PROMOTED = "PROMOTED"
AUDIT_SKIPPED_DUPLICATE = "SKIPPED_DUPLICATE"
AUDIT_QUARANTINED = "QUARANTINED"

#: reason codes recorded on quarantined batches
RC_MISSING_FILE = "MISSING_FILE"
RC_EMPTY_FILE = "EMPTY_FILE"
RC_CHECKSUM_MISMATCH = "CHECKSUM_MISMATCH"
RC_HEADER_SCHEMA_DRIFT = "HEADER_SCHEMA_DRIFT"
RC_PARSE_ERROR = "PARSE_ERROR"
RC_ROW_COUNT_MISMATCH = "ROW_COUNT_MISMATCH"


class FileQuarantined(ConnectorError):
    """A source file failed a promotion gate and was quarantined."""


def _load_schema_registry() -> dict[str, dict]:
    raw = yaml.safe_load(SCHEMAS_PATH.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or not raw:
        raise ConnectorError("connectors/csv_sftp/schemas.yml is empty or malformed")
    return raw


def _cast_value(column: str, raw: str, value_type: str, row_no: int) -> object:
    text = (raw or "").strip()
    if text == "":
        return None
    try:
        if value_type == "string":
            return text
        if value_type == "integer":
            return int(text)
        if value_type == "decimal":
            return Decimal(text)
        if value_type == "date":
            return date.fromisoformat(text)
    except (ValueError, InvalidOperation) as exc:
        raise ValueError(
            f"row {row_no}: column '{column}' value {raw!r} is not a valid {value_type}"
        ) from exc
    raise ConnectorError(f"unknown column type '{value_type}' in schema registry for '{column}'")


class CsvSftpConnector(BaseConnector):
    """Validated CSV-drop connector. Implemented against the Ridgeline seed."""

    erp_id = "csv_sftp"
    maturity = ConnectorMaturity.IMPLEMENTED
    extraction_notes = (
        "Dealer CSV exports on SFTP with a manifest.json control file. The manifest "
        "is the batch completeness signal; files are validated against the approved "
        "schema registry, quarantined on any gate failure, and deduplicated by "
        "content hash. Nightly cadence; full-refresh per batch."
    )
    natural_key_fields: ClassVar[dict[str, tuple[str, ...]]] = {
        "items": ("item_no",),
        "salespeople": ("salesperson_code",),
        "customers": ("customer_no",),
        "vendors": ("vendor_no",),
        "sales_order_lines": ("order_no", "line_no"),
        "purchase_order_lines": ("po_no", "line_no"),
        "invoice_lines": ("invoice_no", "line_no"),
        "inventory_snapshots": ("snapshot_date", "branch_code", "item_no"),
        "gl_entries": ("journal_no", "line_no"),
    }

    def __init__(
        self, source: SourceConfig, store: ControlPlaneStore, config: ControlPlaneConfig
    ) -> None:
        super().__init__(source, store, config)
        self._schemas = _load_schema_registry()
        self._pending_audit: FileAuditRecord | None = None
        self._batch_id = ""

    # ------------------------------------------------------------------
    # Contract surface
    # ------------------------------------------------------------------

    def entities(self) -> list[str]:
        return list(self.natural_key_fields)

    _ARROW_TYPE_BY_KIND = {
        "string": pa.string(),
        "integer": pa.int64(),
        "decimal": pa.decimal128(38, 9),  # wide enough for any money/qty value
        "date": pa.date32(),
    }

    def arrow_schema(self, entity: str) -> pa.Schema:
        spec = self._schemas[entity]
        fields = [
            pa.field(c["name"], self._ARROW_TYPE_BY_KIND[c["type"]]) for c in spec["columns"]
        ]
        # Provenance stamps the base writer appends (see base._stamp and
        # CONNECTOR_STAMPED_COLUMNS); loaded_at is an ISO string.
        fields += [
            pa.field("source_system", pa.string()),
            pa.field("source_id", pa.string()),
            pa.field("source_file", pa.string()),
            pa.field("source_row_no", pa.int64()),
            pa.field("batch_id", pa.string()),
            pa.field("loaded_at", pa.string()),
        ]
        return pa.schema(fields)

    def validate_config(self) -> list[str]:
        drop_root = self._drop_root()
        if not drop_root.is_dir():
            return [f"drop_root '{drop_root}' does not exist or is not a directory"]
        try:
            manifest = self._manifest()
        except ConnectorError as exc:
            return [f"manifest invalid: {exc}"]
        problems: list[str] = []
        for entity, spec in self._schemas.items():
            file_name = spec["file"]
            if manifest.entry_for(file_name) is None:
                problems.append(f"manifest does not declare '{file_name}' (entity {entity})")
        return problems

    def describe_extraction(self, entity: str) -> ExtractionPlan:
        spec = self._schemas[entity]
        return ExtractionPlan(
            entity=entity,
            surface=f"local/SFTP CSV drop file '{spec['file']}' validated against the "
            "approved schema registry with manifest-gated promotion",
            incremental_key="manifest.batch_id (batch-level; per-file content hash dedupes re-delivery)",
            notes=self.extraction_notes,
        )

    def current_watermark(
        self, entity: str, mode: ExtractionMode, watermark_before: str | None
    ) -> str | None:
        return self._batch_id or watermark_before

    # ------------------------------------------------------------------
    # Extraction
    # ------------------------------------------------------------------

    def extract(
        self, entity: str, mode: ExtractionMode = ExtractionMode.BACKFILL
    ) -> ExtractedEntity:
        """Extract one entity, then write the file-audit row on success.

        The audit row is the promotion receipt: it is written only after the
        Parquet write has returned, so an audit row of PROMOTED always implies
        a readable Parquet file for that batch. A quarantined file records a
        QUARANTINED audit row and re-raises.
        """
        try:
            result = super().extract(entity, mode)
        except FileQuarantined:
            self._flush_pending_audit()
            raise
        self._flush_pending_audit(rows_parsed=result.rows_extracted)
        return result

    def _flush_pending_audit(self, rows_parsed: int = 0) -> None:
        pending = self._pending_audit
        if pending is None:
            return
        if pending.status == AUDIT_PROMOTED and pending.rows_parsed != rows_parsed:
            pending = replace(pending, rows_parsed=rows_parsed)
        self.store.record_file_audit(pending)
        self._pending_audit = None

    def _iter_records(
        self, entity: str, mode: ExtractionMode, watermark: str | None
    ) -> Iterator[dict[str, object]]:
        spec = self._schemas[entity]
        file_name: str = spec["file"]
        manifest = self._manifest()
        self._batch_id = manifest.batch_id
        entry = manifest.entry_for(file_name)
        if entry is None:
            raise ConnectorError(
                f"manifest {manifest.batch_id} does not declare '{file_name}' for entity '{entity}'"
            )
        local_path = self._drop_root() / file_name
        file_hash = self._sha256(local_path) if local_path.exists() else ""

        # Idempotency gate: same bytes already promoted (or the same batch in
        # incremental mode) -> skip without re-reading.
        already_promoted = self.store.has_file_been_processed(self.source.source_id, file_hash)
        same_batch_incremental = (
            mode is ExtractionMode.INCREMENTAL and watermark == manifest.batch_id
        )
        if already_promoted or same_batch_incremental:
            self._pending_audit = self._audit(
                file_name,
                file_hash,
                entry.rows,
                AUDIT_SKIPPED_DUPLICATE,
                "ALREADY_SYNCED" if same_batch_incremental else "DUPLICATE_FILE",
            )
            return

        # Promotion gates — validate everything before yielding the first row
        # so a bad file never leaves partial output behind.
        try:
            rows, gate_detail = self._validated_rows(spec, file_name, entry)
        except FileQuarantined:
            self._pending_audit = self._audit(
                file_name, file_hash, entry.rows, AUDIT_QUARANTINED, "SEE_QUARANTINE"
            )
            raise
        self._pending_audit = self._audit(
            file_name, file_hash, entry.rows, AUDIT_PROMOTED, gate_detail
        )
        yield from rows

    # ------------------------------------------------------------------
    # Gate implementations
    # ------------------------------------------------------------------

    def _validated_rows(self, spec: dict, file_name: str, entry) -> tuple[list[dict[str, object]], str]:
        local_path = self._drop_root() / file_name
        if not local_path.exists():
            self._quarantine(
                RC_MISSING_FILE,
                f"manifest declares '{file_name}' but the file is absent",
                file_name,
                None,
                entry,
            )
            raise FileQuarantined(f"{file_name}: missing from drop directory")
        raw_bytes = local_path.read_bytes()
        if not raw_bytes.strip():
            self._quarantine(RC_EMPTY_FILE, "file is empty", file_name, local_path, entry)
            raise FileQuarantined(f"{file_name}: empty file")
        actual_hash = sha256(raw_bytes).hexdigest()
        if actual_hash != entry.sha256:
            detail = f"manifest sha256={entry.sha256} but content sha256={actual_hash}"
            self._quarantine(RC_CHECKSUM_MISMATCH, detail, file_name, local_path, entry)
            raise FileQuarantined(f"{file_name}: {detail}")

        expected_columns = [c["name"] for c in spec["columns"]]
        column_types = {c["name"]: c["type"] for c in spec["columns"]}
        text = raw_bytes.decode(entry.encoding)
        reader = csv.DictReader(StringIO(text), delimiter=entry.delimiter)
        if reader.fieldnames is None:
            self._quarantine(RC_HEADER_SCHEMA_DRIFT, "file has no header row", file_name, local_path, entry)
            raise FileQuarantined(f"{file_name}: missing header row")
        header = [f.strip() for f in reader.fieldnames]
        missing = [c for c in expected_columns if c not in header]
        extra = [c for c in header if c not in expected_columns]
        if missing:
            detail = f"header is missing declared columns {missing}"
            self._quarantine(RC_HEADER_SCHEMA_DRIFT, detail, file_name, local_path, entry)
            raise FileQuarantined(f"{file_name}: {detail}")
        allow_additive = self.source.settings.get("allow_additive_columns", "true").lower() == "true"
        if extra and not allow_additive:
            detail = f"header carries undeclared columns {extra} (allow_additive_columns=false)"
            self._quarantine(RC_HEADER_SCHEMA_DRIFT, detail, file_name, local_path, entry)
            raise FileQuarantined(f"{file_name}: {detail}")

        parsed: list[dict[str, object]] = []
        parse_errors: list[str] = []
        for row_no, raw_row in enumerate(reader, start=1):
            record: dict[str, object] = {}
            for column in expected_columns:
                raw_value = raw_row.get(column) or ""
                try:
                    record[column] = _cast_value(column, raw_value, column_types[column], row_no)
                except ValueError as exc:
                    parse_errors.append(str(exc))
                    record[column] = None
            # connector-stamped provenance (the base stamps the rest)
            record["source_file"] = file_name
            record["source_row_no"] = row_no
            record["batch_id"] = self._batch_id
            parsed.append(record)
        if parse_errors:
            detail = "; ".join(parse_errors[:5])
            self._quarantine(RC_PARSE_ERROR, detail, file_name, local_path, entry)
            raise FileQuarantined(f"{file_name}: {len(parse_errors)} row(s) failed casts: {detail}")
        if len(parsed) != entry.rows:
            detail = f"manifest declares {entry.rows} data rows but file contains {len(parsed)}"
            self._quarantine(RC_ROW_COUNT_MISMATCH, detail, file_name, local_path, entry)
            raise FileQuarantined(f"{file_name}: {detail}")
        return parsed, (f"validated clean; extra columns {extra}" if extra else "validated clean")

    def _quarantine(
        self, reason_code: str, detail: str, file_name: str, local_path: Path | None, entry
    ) -> None:
        quarantine_root = Path(
            self.source.settings.get("quarantine_dir", str(self.config.quarantine_root))
        )
        target_dir = quarantine_root / self.source.source_id / (self._batch_id or "unbatched")
        target_dir.mkdir(parents=True, exist_ok=True)
        quarantined_path = ""
        if local_path is not None and local_path.exists():
            dest = target_dir / file_name
            shutil.copy2(local_path, dest)
            quarantined_path = str(dest)
        (target_dir / f"{file_name}.reason.json").write_text(
            json.dumps(
                {
                    "source_id": self.source.source_id,
                    "batch_id": self._batch_id,
                    "file_name": file_name,
                    "reason_code": reason_code,
                    "detail": detail,
                    "manifest_sha256": entry.sha256,
                    "manifest_rows": entry.rows,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.record_quarantine(
            QuarantineRecord(
                source_id=self.source.source_id,
                batch_id=self._batch_id or None,
                file_name=file_name,
                reason_code=reason_code,
                detail=detail,
                quarantine_path=quarantined_path,
            )
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _drop_root(self) -> Path:
        return Path(self.source.settings["drop_root"])

    def _manifest(self):
        from connectors.csv_sftp.manifest import ManifestError, load_manifest

        try:
            return load_manifest(self._drop_root() / "manifest.json")
        except ManifestError as exc:
            raise ConnectorError(str(exc)) from exc

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _audit(
        self, file_name: str, file_hash: str, rows_declared: int, status: str, reason_code: str
    ) -> FileAuditRecord:
        return FileAuditRecord(
            source_id=self.source.source_id,
            batch_id=self._batch_id,
            file_name=file_name,
            file_hash=file_hash or "unknown",
            rows_declared=rows_declared,
            rows_parsed=0,
            status=status,
            reason_code=reason_code,
            processed_at=datetime.now(UTC),
        )
