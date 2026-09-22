"""eci_spruce — ECI Spruce / RockSolid MAX extraction (dealer-mediated file drop).

STATUS: implemented against the documented file surfaces — Spruce Report
Service scheduled outputs with file creation, the S: cloud-folder handoff, and
the RockSolid MAX pipe-delimited full-file export (spec art_ktSh9Z8x §1/§4) —
but NOT exercised against a live dealer. The dealer's actual file layouts are
[D] (undocumented publicly), so product, hosting, and layout pin at onboarding
fail-closed, and the declared schemas.yml is the contract a real export is
validated against. The SOAP Ecommerce API stays documented NDA-gated future
work (spec §2/§8): no operation names, schemas, or limits ship here.

Extraction notes (spec sections referenced per point):

- Transport (§1): no public SFTP and no ungated API. Files arrive by
  dealer-mediated exchange into this connector's drop root. The manifest
  control file is the batch completeness signal — filenames, SHA-256
  checksums, row counts, and a generation timestamp (§4's "repeatable full
  delivery" shape). The control-file parser is shared machinery
  (``connectors.csv_sftp.manifest`` — the same manifest contract as the
  seeded CSV/SFTP path).
- Idempotency (§4): every pull is a repeatable full delivery. Identical
  re-delivered files are absorbed by content hash (no double staging); rows
  upsert on natural keys downstream. This absorption also covers an identical
  re-delivery of an older batch — nothing overwrites, nothing to refuse.
- Stale/refusal semantics (§6): report output drifts retroactively when items
  are merged or group/vendor/location/document assignments change, so a
  manifest older than the processed watermark REFUSES (RC_STALE_MANIFEST),
  and a re-delivery under an already-processed batch id with changed content
  refuses rather than overrides (RC_REGENERATED_BATCH) — regenerated pulls
  are "new evidence to reconcile against, not overrides". The quarantine
  record is the reconciliation flag; forcing re-ingest is a deliberate
  checkpoint-clearing operator action, not a connector default.
- RSM quirks (§7): "group:group name"/"section:section name" concatenations
  split into code+name at the connector layer; store identifiers stay strings
  (RSM exports may trim leading zeros — the normalization convention is
  confirmed at onboarding); embedded tabs/newlines either survive CSV quoting
  or fail the row-count gate fail-closed; duplicate natural keys collapse
  keep-first with the duplicate count recorded on the promotion audit
  (downstream importers drop duplicates silently — here it is recorded).
- UOM (§7): ordered UOM (line quantities), stocked UOM + pack/pack_uom (item
  master), and conversion factors stay separate fields. The canonical
  line-schema constraint (no conversion-factor column) is tracked for the ERP
  formalization task; nothing is invented into canonical here.
- Branch semantics (§7): the ecommerce item sync publishes default-branch
  values only; per-branch on-hand arrives via the dealer-side file extract
  this connector consumes, and branch_code stays a required snapshot key.
- Thin domains (§3/§7): vendors have no API surface (the dealer-mediated file
  extract is the whole story); GL transactions and salespeople stay OUT of the
  entity set until the NDA'd guide or an ECI statement documents a layout —
  flagged, not improvised.
- Delete reconciliation (§4/§6): full-file dimension exports (items,
  customers, vendors) anti-join against the current drop's key inventory;
  period-scoped transaction files (order/invoice lines, dated snapshots)
  REFUSE the anti-join — in a period slice a missing key is history, not a
  delete.
- Onboarding pins ([D] → fail-closed REQUIRED settings, BisTrack §7.1
  pattern): product (spruce|rsm), hosting (hosted|on_prem — drives the VPN
  path, archive behavior, and backfill options, §1/§6), and layout_profile
  (spruce_report_csv|rsm_pipe_export).
"""

from __future__ import annotations

import csv
import datetime as dt
import json
import shutil
from collections.abc import Iterator
from dataclasses import dataclass, replace
from datetime import date
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
    ConnectorNotConfigured,
    ExtractedEntity,
    ExtractionMode,
    ExtractionPlan,
    natural_id_for,
)
from connectors.csv_sftp.manifest import Manifest, ManifestError, ManifestFile, load_manifest
from control_plane.config import ControlPlaneConfig
from control_plane.models import FileAuditRecord, QuarantineRecord, SourceConfig
from control_plane.store import ControlPlaneStore

SCHEMAS_PATH = resources.files("connectors.eci_spruce") / "schemas.yml"

AUDIT_PROMOTED = "PROMOTED"
AUDIT_SKIPPED_DUPLICATE = "SKIPPED_DUPLICATE"
AUDIT_QUARANTINED = "QUARANTINED"

#: reason codes recorded on quarantined batches — csv_sftp parity for the
#: shared file gates, plus the two spec-§6 refusal codes.
RC_MISSING_FILE = "MISSING_FILE"
RC_EMPTY_FILE = "EMPTY_FILE"
RC_CHECKSUM_MISMATCH = "CHECKSUM_MISMATCH"
RC_HEADER_SCHEMA_DRIFT = "HEADER_SCHEMA_DRIFT"
RC_PARSE_ERROR = "PARSE_ERROR"
RC_ROW_COUNT_MISMATCH = "ROW_COUNT_MISMATCH"
RC_STALE_MANIFEST = "STALE_MANIFEST"
RC_REGENERATED_BATCH = "REGENERATED_BATCH"


class FileQuarantined(ConnectorError):
    """A source file failed a promotion gate and was quarantined."""


def _load_schema_registry() -> dict[str, dict]:
    raw = yaml.safe_load(SCHEMAS_PATH.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or not raw:
        raise ConnectorError("connectors/eci_spruce/schemas.yml is empty or malformed")
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


def _parse_generation(raw: str) -> dt.datetime:
    """Manifest generation stamp as a comparable UTC instant.

    ISO-8601 only (the manifest contract). Naive stamps read as UTC: the spec
    documents no dealer-local offset (§4), so ordering — not locality — is
    the semantic.
    """
    try:
        parsed = dt.datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ConnectorError(
            f"manifest generated_at {raw!r} is not an ISO-8601 timestamp — refusing to "
            "order batches on it (an unparseable generation would silently skip records)"
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


@dataclass(frozen=True)
class BatchStamp:
    """Parsed manifest identity: batch id plus its generation timestamp.

    The watermark format is ``<generated_at>|<batch_id>`` — the generation
    timestamp orders deliveries (stale detection, spec §6), the batch id
    identifies them (regeneration detection). The store treats watermarks as
    opaque strings; only this connector parses the composite back.
    """

    batch_id: str
    generated_at: str  # raw manifest value

    @property
    def generation(self) -> dt.datetime:
        return _parse_generation(self.generated_at)

    def as_watermark(self) -> str:
        return f"{self.generated_at}|{self.batch_id}"

    @staticmethod
    def from_watermark(watermark: str | None) -> BatchStamp | None:
        if not watermark:
            return None
        generated_at, separator, batch_id = watermark.partition("|")
        if not separator or not generated_at or not batch_id:
            raise ConnectorError(
                f"stored watermark {watermark!r} is not an eci_spruce composite "
                "(generated_at|batch_id) — refusing to guess batch order across formats"
            )
        return BatchStamp(batch_id=batch_id, generated_at=generated_at)


class EciSpruceConnector(BaseConnector):
    """ECI Spruce / RockSolid MAX dealer-mediated file-drop connector.

    Rides the csv_sftp machinery's gates (manifest completeness signal,
    checksum, header/schema check, content-hash idempotency, quarantine with
    reason.json) against the Spruce schema registry, and adds the spec's
    full-delivery semantics: stale-generation and regenerated-batch refusal
    (§6) plus full-file-only delete reconciliation.
    """

    erp_id = "eci_spruce"
    maturity = ConnectorMaturity.IMPLEMENTED
    extraction_notes = (
        "ECI Spruce / RockSolid MAX dealer-mediated file drop (spec art_ktSh9Z8x): "
        "scheduled-report / cloud-folder file outputs (CSV or RSM pipe export) gated "
        "by a manifest control file (checksums, row counts, generation stamps). "
        "Full-delivery idempotency by content hash; stale or regenerated manifests "
        "refuse fail-closed (§6 report drift). Product, hosting, and layout pin at "
        "onboarding ([D] discovery outcomes, fail-closed on empty). The SOAP "
        "Ecommerce API stays NDA-gated future work (§2/§8)."
    )
    natural_key_fields: ClassVar[dict[str, tuple[str, ...]]] = {
        "items": ("item_id",),
        "customers": ("customer_id",),
        "vendors": ("vendor_id",),
        "sales_order_lines": ("order_no", "line_no"),
        "invoice_lines": ("invoice_no", "line_no"),
        "inventory_snapshots": ("snapshot_date", "branch_code", "item_id"),
    }
    #: [D] discovery pins (spec §1/§8) — fail-closed REQUIRED settings so
    #: onboarding forces the deployment/layout conversation (BisTrack §7.1
    #: pattern). Defaults are empty; nothing is guessed.
    REQUIRED_SETTINGS: ClassVar[tuple[str, ...]] = (
        "product",
        "hosting",
        "layout_profile",
        "csv_drop_root",
    )
    PRODUCT_CHOICES: ClassVar[tuple[str, ...]] = ("spruce", "rsm")
    HOSTING_CHOICES: ClassVar[tuple[str, ...]] = ("hosted", "on_prem")
    LAYOUT_CHOICES: ClassVar[tuple[str, ...]] = ("spruce_report_csv", "rsm_pipe_export")
    #: Entities whose drop file is a full-file export of the domain (spec §4:
    #: RSM full-file export; a scheduled report is current full state) — the
    #: only surfaces where missing-key-in-newest-file legitimately means
    #: "deleted".
    FULL_FILE_ANTI_JOIN_ENTITIES: ClassVar[frozenset[str]] = frozenset(
        {"items", "customers", "vendors"}
    )
    #: RSM concatenation splits (spec §7): "group:group name" ->
    #: group_code/group_name; "section:section name" -> section_code/section_name.
    SPLIT_COLUMNS: ClassVar[dict[str, tuple[str, str]]] = {
        "group": ("group_code", "group_name"),
        "section": ("section_code", "section_name"),
    }
    _ARROW_TYPE_BY_KIND: ClassVar[dict[str, pa.DataType]] = {
        "string": pa.string(),
        "integer": pa.int64(),
        "decimal": pa.decimal128(38, 9),  # wide enough for any money/qty value
        "date": pa.date32(),
    }

    def __init__(
        self, source: SourceConfig, store: ControlPlaneStore, config: ControlPlaneConfig
    ) -> None:
        super().__init__(source, store, config)
        self._schemas = _load_schema_registry()
        self._pending_audit: FileAuditRecord | None = None
        self._batch_stamp: BatchStamp | None = None

    # ------------------------------------------------------------------
    # Contract surface
    # ------------------------------------------------------------------

    def entities(self) -> list[str]:
        return list(self.natural_key_fields)

    def validate_config(self) -> list[str]:
        missing = [f for f in self.REQUIRED_SETTINGS if not self.source.settings.get(f)]
        if missing:
            return [
                f"missing required ECI Spruce/RSM settings: {', '.join(missing)} "
                "(see .env.example; the source stays disabled until onboarding pins them — "
                "product/hosting/layout are [D] discovery outcomes, spec §1/§8)"
            ]
        problems: list[str] = []
        product = self.source.settings["product"].strip().lower()
        if product not in self.PRODUCT_CHOICES:
            problems.append(
                f"setting 'product' must be one of {', '.join(self.PRODUCT_CHOICES)} — the "
                "two products expose different export surfaces (spec §1)"
            )
        hosting = self.source.settings["hosting"].strip().lower()
        if hosting not in self.HOSTING_CHOICES:
            problems.append(
                f"setting 'hosting' must be one of {', '.join(self.HOSTING_CHOICES)} — "
                "hosting drives the network path, archive behavior, and backfill "
                "options (spec §1/§6)"
            )
        layout = self.source.settings["layout_profile"].strip().lower()
        if layout not in self.LAYOUT_CHOICES:
            problems.append(
                f"setting 'layout_profile' must be one of {', '.join(self.LAYOUT_CHOICES)} "
                "— the dealer's actual export layout is undocumented and pins at "
                "onboarding (spec §8)"
            )
        drop_root = Path(self.source.settings["csv_drop_root"])
        if not drop_root.is_dir():
            problems.append(f"csv_drop_root '{drop_root}' does not exist or is not a directory")
        else:
            try:
                manifest = self._manifest()
            except ConnectorError as exc:
                return [*problems, f"manifest invalid: {exc}"]
            undeclared = [
                f"'{spec['file']}' (entity {entity})"
                for entity, spec in self._schemas.items()
                if manifest.entry_for(spec["file"]) is None
            ]
            if undeclared:
                problems.append(f"manifest does not declare {', '.join(undeclared)}")
        return problems

    def describe_extraction(self, entity: str) -> ExtractionPlan:
        spec = self._schemas[entity]
        layout = (
            self.source.settings.get("layout_profile")
            or "layout_profile unpinned ([D] — fails closed at onboarding)"
        )
        return ExtractionPlan(
            entity=entity,
            surface=(
                f"dealer-mediated file export '{spec['file']}' ({layout}) validated against "
                "the approved Spruce schema registry with manifest-gated promotion; the "
                "SOAP Ecommerce API equivalent stays NDA-gated future work (spec §2/§8)"
            ),
            incremental_key=(
                "manifest generation stamp (generated_at|batch_id composite); per-file "
                "content hash dedupes re-delivery; stale or regenerated manifests refuse "
                "fail-closed (spec §4/§6)"
            ),
            notes=self.extraction_notes,
        )

    # ------------------------------------------------------------------
    # Watermark semantics — the spec §4/§6 full-delivery shape
    # ------------------------------------------------------------------

    def current_watermark(
        self, entity: str, mode: ExtractionMode, watermark_before: str | None
    ) -> str | None:
        if self._batch_stamp is not None:
            return self._batch_stamp.as_watermark()
        return watermark_before

    # ------------------------------------------------------------------
    # Extraction
    # ------------------------------------------------------------------

    def extract(
        self, entity: str, mode: ExtractionMode = ExtractionMode.BACKFILL
    ) -> ExtractedEntity:
        """Extract one entity, then write the file-audit row on success.

        The audit row is the promotion receipt: written only after the
        Parquet write has returned, so a PROMOTED audit row always implies a
        readable Parquet file for that batch. A quarantined file records a
        QUARANTINED audit row and re-raises (csv_sftp parity).
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
        self._require_config()
        spec = self._schemas[entity]
        file_name: str = spec["file"]
        manifest = self._manifest()
        self._batch_stamp = BatchStamp(
            batch_id=manifest.batch_id, generated_at=manifest.generated_at
        )
        entry = manifest.entry_for(file_name)
        if entry is None:
            raise ConnectorError(
                f"manifest {manifest.batch_id} does not declare '{file_name}' for entity '{entity}'"
            )
        local_path = self._drop_root() / file_name
        file_hash = self._sha256(local_path) if local_path.exists() else ""

        # Idempotency gate (spec §4): identical bytes already promoted ->
        # absorbed without re-reading. Also absorbs an identical re-delivery
        # of an older batch — no overwrite, nothing to refuse.
        if self.store.has_file_been_processed(self.source.source_id, file_hash):
            self._pending_audit = self._audit(
                file_name, file_hash, entry.rows, AUDIT_SKIPPED_DUPLICATE, "DUPLICATE_FILE"
            )
            return

        # Spec-§6 gates: refuse stale generations and regenerated batches
        # before any row is read — report drift must never overwrite.
        self._enforce_batch_order(manifest, watermark, file_name, file_hash)

        try:
            rows, gate_detail = self._validated_rows(entity, spec, file_name, entry)
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
    # Spec-§6 batch-order refusals
    # ------------------------------------------------------------------

    def _enforce_batch_order(
        self,
        manifest: Manifest,
        prior_watermark: str | None,
        file_name: str,
        file_hash: str,
    ) -> None:
        """Refuse stale generations and regenerated batches, fail-closed.

        Report output drifts retroactively (spec §6), so a delivery may never
        overwrite a newer generation, and an already-processed batch id with
        changed content is a regenerated report — new evidence to reconcile
        against, not an override. Identical re-deliveries never reach here
        (the content-hash gate absorbs them first).
        """
        prior = BatchStamp.from_watermark(prior_watermark)
        if prior is None:
            return
        if prior.batch_id == manifest.batch_id:
            detail = (
                f"batch {manifest.batch_id} was already processed but the re-delivered "
                f"'{file_name}' content is new (hash {file_hash[:16]}..) — a regenerated "
                "report is new evidence to reconcile against, not an override (spec §6)"
            )
            self._refuse_manifest(RC_REGENERATED_BATCH, detail, manifest)
            raise FileQuarantined(f"{file_name}: {detail}")
        if _parse_generation(manifest.generated_at) <= prior.generation:
            detail = (
                f"manifest generated {manifest.generated_at} (batch {manifest.batch_id}) is "
                f"not newer than the processed batch {prior.batch_id} ({prior.generated_at}) "
                "— out-of-order or replayed delivery; retroactively drifted report output "
                "must not overwrite newer state (spec §6)"
            )
            self._refuse_manifest(RC_STALE_MANIFEST, detail, manifest)
            raise FileQuarantined(f"{file_name}: {detail}")

    def _refuse_manifest(self, reason_code: str, detail: str, manifest: Manifest) -> None:
        """Quarantine a refused batch at the manifest level and record the audit row."""
        manifest_path = self._drop_root() / "manifest.json"
        file_hash = self._sha256(manifest_path) if manifest_path.exists() else ""
        self.store.record_file_audit(
            self._audit("manifest.json", file_hash, 0, AUDIT_QUARANTINED, reason_code)
        )
        self._quarantine_manifest(reason_code, detail, manifest_path)

    # ------------------------------------------------------------------
    # Delete reconciliation (spec §4/§6 scoping)
    # ------------------------------------------------------------------

    def reconcile_deletes(self, entity: str, batch_id: str | None = None):
        if entity not in self.FULL_FILE_ANTI_JOIN_ENTITIES:
            raise ConnectorError(
                f"anti-join delete reconciliation for '{entity}' is scoped out: the drop "
                "carries period-scoped slices, so a missing key is history, not a delete "
                "(spec §4/§6) — only full-file dimension exports (items, customers, "
                "vendors) reconcile"
            )
        # Tombstones are stamped with the drop's batch id unless the caller
        # supplies one — the base contract leaves that to the connector.
        if batch_id is None:
            batch_id = self._manifest().batch_id
        return super().reconcile_deletes(entity, batch_id)

    def source_key_inventory(self, entity: str) -> set[str]:
        """Key scan of the current full-file export — the anti-join's source side.

        Gates the scan on the spec-§6 stale-generation refusal (a stale drop
        must never tombstone newer warehouse state) and fails closed on a
        missing/empty file: an empty inventory would tombstone the whole
        warehouse.
        """
        if entity not in self.FULL_FILE_ANTI_JOIN_ENTITIES:
            raise ConnectorError(
                f"'{entity}' is a period-scoped delivery; no full-key scan exists for it"
            )
        self._require_config()
        spec = self._schemas[entity]
        file_name: str = spec["file"]
        manifest = self._manifest()
        prior = BatchStamp.from_watermark(
            self.store.get_watermark(self.source.source_id, entity, ExtractionMode.BACKFILL.value)
        )
        if (
            prior is not None
            and manifest.batch_id != prior.batch_id
            and _parse_generation(manifest.generated_at) <= prior.generation
        ):
            # The scan refuses stale generations like extraction does; an
            # already-processed batch id is NOT a refusal here (the current
            # drop is the newest state the scan should see).
            detail = (
                f"manifest generated {manifest.generated_at} (batch {manifest.batch_id}) is "
                f"not newer than the processed batch {prior.batch_id} ({prior.generated_at}) "
                "— refusing the key scan on a stale drop (spec §6)"
            )
            self._refuse_manifest(RC_STALE_MANIFEST, detail, manifest)
            raise FileQuarantined(detail)
        entry = manifest.entry_for(file_name)
        if entry is None:
            raise ConnectorError(
                f"manifest {manifest.batch_id} does not declare '{file_name}' for entity '{entity}'"
            )
        local_path = self._drop_root() / file_name
        if not local_path.exists():
            raise ConnectorError(
                f"manifest declares '{file_name}' but the file is absent — refusing the "
                "key scan (an empty inventory would tombstone the whole warehouse)"
            )
        text = local_path.read_bytes().decode(entry.encoding)
        reader = csv.DictReader(StringIO(text), delimiter=entry.delimiter)
        key_fields = self.natural_key_fields[entity]
        column_types = {c["name"]: c["type"] for c in spec["columns"]}
        keys: set[str] = set()
        for row_no, raw_row in enumerate(reader, start=1):
            record: dict[str, object] = {}
            try:
                for column in key_fields:
                    record[column] = _cast_value(
                        column, raw_row.get(column) or "", column_types[column], row_no
                    )
            except ValueError as exc:
                raise ConnectorError(f"key scan failed on '{file_name}': {exc}") from exc
            keys.add(natural_id_for(key_fields, record))
        if not keys:
            raise ConnectorError(
                f"'{file_name}' holds no rows — refusing the key scan (an empty "
                "inventory would tombstone the whole warehouse)"
            )
        return keys

    # ------------------------------------------------------------------
    # Gate implementations (csv_sftp parity + RSM normalizations)
    # ------------------------------------------------------------------

    def _validated_rows(
        self, entity: str, spec: dict, file_name: str, entry: ManifestFile
    ) -> tuple[list[dict[str, object]], str]:
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
            self._quarantine(
                RC_HEADER_SCHEMA_DRIFT, "file has no header row", file_name, local_path, entry
            )
            raise FileQuarantined(f"{file_name}: missing header row")
        header = [f.strip() for f in reader.fieldnames]
        missing = [c for c in expected_columns if c not in header]
        extra = [c for c in header if c not in expected_columns]
        if missing:
            detail = f"header is missing declared columns {missing}"
            self._quarantine(RC_HEADER_SCHEMA_DRIFT, detail, file_name, local_path, entry)
            raise FileQuarantined(f"{file_name}: {detail}")
        allow_additive = (
            self.source.settings.get("allow_additive_columns", "true").lower() == "true"
        )
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
            self._split_concatenations(record)
            # connector-stamped provenance (the base stamps the rest)
            record["source_file"] = file_name
            record["source_row_no"] = row_no
            record["batch_id"] = self._batch_stamp.batch_id if self._batch_stamp else ""
            parsed.append(record)
        if parse_errors:
            detail = "; ".join(parse_errors[:5])
            self._quarantine(RC_PARSE_ERROR, detail, file_name, local_path, entry)
            raise FileQuarantined(f"{file_name}: {len(parse_errors)} row(s) failed casts: {detail}")
        if len(parsed) != entry.rows:
            detail = f"manifest declares {entry.rows} data rows but file contains {len(parsed)}"
            self._quarantine(RC_ROW_COUNT_MISMATCH, detail, file_name, local_path, entry)
            raise FileQuarantined(f"{file_name}: {detail}")
        deduped = self._collapse_duplicates(entity, parsed)
        duplicates = len(parsed) - len(deduped)
        gate_detail = f"validated clean; extra columns {extra}" if extra else "validated clean"
        if duplicates:
            gate_detail += f"; {duplicates} duplicate natural key(s) collapsed keep-first"
        return deduped, gate_detail

    def _split_concatenations(self, record: dict[str, object]) -> None:
        """Split RSM "code:name" concatenations into code+name (spec §7).

        A value with no separator passes through as the code alone — some
        dealer reports export the field unsplit; the code survives either way.
        """
        for raw_field, (code_field, name_field) in self.SPLIT_COLUMNS.items():
            raw = record.pop(raw_field, None)
            if raw is None:
                record[code_field] = None
                record[name_field] = None
                continue
            code, separator, name = str(raw).partition(":")
            if separator:
                record[code_field] = code
                record[name_field] = name
            else:
                record[code_field] = raw
                record[name_field] = None

    def _collapse_duplicates(
        self, entity: str, parsed: list[dict[str, object]]
    ) -> list[dict[str, object]]:
        """Keep-first collapse of duplicate natural keys (RSM quirk, spec §7)."""
        key_fields = self.natural_key_fields[entity]
        deduped: list[dict[str, object]] = []
        seen: set[str] = set()
        for record in parsed:
            natural_id = natural_id_for(key_fields, record)
            if natural_id in seen:
                continue
            seen.add(natural_id)
            deduped.append(record)
        return deduped

    def arrow_schema(self, entity: str) -> pa.Schema:
        spec = self._schemas[entity]
        fields: list[pa.Field] = []
        for column in spec["columns"]:
            split = self.SPLIT_COLUMNS.get(column["name"])
            if split:
                # The concatenated raw column leaves staging as code+name.
                fields += [pa.field(split[0], pa.string()), pa.field(split[1], pa.string())]
            else:
                fields.append(pa.field(column["name"], self._ARROW_TYPE_BY_KIND[column["type"]]))
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

    # ------------------------------------------------------------------
    # Quarantine (csv_sftp parity: byte copy + machine-readable reason)
    # ------------------------------------------------------------------

    def _quarantine(
        self,
        reason_code: str,
        detail: str,
        file_name: str,
        local_path: Path | None,
        entry: ManifestFile,
    ) -> None:
        target_dir = self._quarantine_dir()
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
                    "batch_id": self._batch_stamp.batch_id if self._batch_stamp else None,
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
                batch_id=self._batch_stamp.batch_id if self._batch_stamp else None,
                file_name=file_name,
                reason_code=reason_code,
                detail=detail,
                quarantine_path=quarantined_path,
                quarantined_at=dt.datetime.now(dt.UTC),
            )
        )

    def _quarantine_manifest(self, reason_code: str, detail: str, manifest_path: Path) -> None:
        """Manifest-level quarantine (stale/regenerated refusals, spec §6)."""
        target_dir = self._quarantine_dir()
        target_dir.mkdir(parents=True, exist_ok=True)
        quarantined_path = ""
        if manifest_path.exists():
            dest = target_dir / manifest_path.name
            shutil.copy2(manifest_path, dest)
            quarantined_path = str(dest)
        (target_dir / f"{manifest_path.name}.reason.json").write_text(
            json.dumps(
                {
                    "source_id": self.source.source_id,
                    "batch_id": self._batch_stamp.batch_id if self._batch_stamp else None,
                    "generated_at": self._batch_stamp.generated_at if self._batch_stamp else None,
                    "file_name": manifest_path.name,
                    "reason_code": reason_code,
                    "detail": detail,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.record_quarantine(
            QuarantineRecord(
                source_id=self.source.source_id,
                batch_id=self._batch_stamp.batch_id if self._batch_stamp else None,
                file_name=manifest_path.name,
                reason_code=reason_code,
                detail=detail,
                quarantine_path=quarantined_path,
                quarantined_at=dt.datetime.now(dt.UTC),
            )
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _quarantine_dir(self) -> Path:
        # `or` (not .get default): the registry interpolates ${VAR:-} to an
        # empty string that is PRESENT — an empty override must fall back to
        # the config root, not resolve Path("") to the current directory.
        quarantine_root = Path(
            self.source.settings.get("quarantine_dir") or str(self.config.quarantine_root)
        )
        batch_id = self._batch_stamp.batch_id if self._batch_stamp else "unbatched"
        return quarantine_root / self.source.source_id / batch_id

    def _require_config(self) -> None:
        """Fail closed BEFORE any drop access — inert without onboarding pins."""
        problems = self.validate_config()
        if problems:
            raise ConnectorNotConfigured(
                f"source {self.source.source_id} ({self.erp_id}) is not configurable: "
                + "; ".join(problems)
            )

    def _drop_root(self) -> Path:
        return Path(self.source.settings["csv_drop_root"])

    def _manifest(self) -> Manifest:
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
            batch_id=self._batch_stamp.batch_id if self._batch_stamp else "",
            file_name=file_name,
            file_hash=file_hash or "unknown",
            rows_declared=rows_declared,
            rows_parsed=0,
            status=status,
            reason_code=reason_code,
            processed_at=dt.datetime.now(dt.UTC),
        )
