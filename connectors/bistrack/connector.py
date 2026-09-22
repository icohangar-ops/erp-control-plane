"""bistrack — Epicor BisTrack extraction (ODBC implemented, Smart View documented plan).

STATUS: the ODBC mode is implemented on the shared ``DbApiBatchConnector`` engine
(``connectors/legacy/sql_source.py``) but NOT exercised against a live BisTrack
site: no fabricated schema ships as tested. Only three physical table names are
public — ``OrderHeader``, ``OrderLine``, ``InvoiceHeader`` (spec §3.2 [V]) — so
only the entities they serve (``sales_order_lines``, ``invoice_lines``) map to
tables; every other entity stays a documented plan whose extraction raises
:class:`ConnectorNotImplemented` until the physical name is pinned at onboarding
with the spec §3.3 discovery pack. Every column name in the maps below is a
placeholder in the same sense: the discovery pack pins the real names per site
and release (spec §7.6 schema drift), and the maps are the artifact to edit.
``plan --source <id>`` (dry-run) needs no network and no driver.

Mode dispatch (spec §3.1's read surfaces):

* ``mode: odbc`` — read-only ODBC (pyodbc) against the on-prem SQL Server,
  implemented here. Connection factories are injectable; the fixture tests drive
  the real extraction machinery without a driver or a live DB. Extraction scans
  one BisTrack document type at a time and pages with a keyset cursor.
* ``mode: smartview`` — the BisTrack Web Smart View API. This stays a DOCUMENTED
  PLAN: the BisTrack API is a separately licensed Epicor product whose token
  scheme, endpoints, and limits are not public (spec §2.1 [G]) — the adapter
  reports the planned surface and raises :class:`ConnectorNotImplemented` rather
  than inventing API behavior.

Per-type numbering watermarks (spec §4.2 + pitfall §7.1): ``OrderHeader`` carries
separate numbering sequences for orders, quotes, call-off orders, reservations,
and template orders — a quote is a sibling transaction with its own number
sequence, not a status flag. So extraction filters on a configured document-type
column (``order_document_types``; quotes/call-offs/reservations/templates are
never ingested as orders) and tracks the maximum document number PER document
type: the stored checkpoint is a JSON map ``{"<doc_type>": "<max_doc_no>"}``.
Document numbers are compared number-aware (unpadded integers compare correctly)
and the incremental predicate binds the stored maximum as a parameter.

Paging (spec §6.4): pages advance by a keyset predicate on
``(document_no, line_no)`` — ``WHERE doc > ? OR (doc = ? AND line > ?)`` —
never a sliding OFFSET window (the ``OFFSET 0`` clause exists only because
T-SQL requires it before ``FETCH NEXT``).

Units of measure (spec pitfall §7.3): BisTrack converts across UOMs (buy MBF,
sell linear feet/pieces). The connector carries the SOURCE UOM on every line and
maps quantities and prices raw — normalizing to a single unit at extraction
would silently corrupt margin analytics. Reported gap: the spec wants conversion
factors to ride into the canonical model too, but the shared canonical schema
(locked to the CSV registry by a parity test) has no conversion-factor column;
factor resolution therefore lands downstream of staging, and the per-site
staging-view decision is an onboarding item.

Branch partitioning (spec pitfall §7.4): ``branch_code`` rides every
transaction row; the inventory-snapshot natural key is branch-scoped. If a site
discovers per-branch document numbering, natural keys must gain that scope at
onboarding (the Business Central/NetSuite prefix precedent).

Deletes (spec §6): no BisTrack soft-delete flag is publicly documented [G], so
there is none to filter — hard deletes reconcile through the scheduled anti-join
key inventory. If a site surfaces a soft-delete flag, add the predicate then.

Read-only, full stop (spec pitfall §7.9): this connector never writes through
SQL, even when a fix "would be trivial".
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import ClassVar

from connectors.base import (
    ConnectorError,
    ConnectorMaturity,
    ConnectorNotConfigured,
    ConnectorNotImplemented,
    ExtractionMode,
    ExtractionPlan,
    natural_id_for,
)
from connectors.legacy.sql_source import DbApiBatchConnector, validate_identifier
from control_plane.models import QuarantineRecord

ODBC_MODE = "odbc"
SMARTVIEW_MODE = "smartview"

#: Reason code recorded on malformed source rows (csv_sftp/NetSuite/P21 parity).
RC_MALFORMED_ROW = "MALFORMED_ROW"

#: Watermark key for an entity scanned without a document-type filter (the
#: optional invoice path): one site-global numbering sequence assumed [I §4.2].
UNSCOPED = "*"

#: per-mode, per-entity extraction surfaces for entities whose physical tables
#: are not public (spec §3.2 [D]) — the documented plan until onboarding pins them.
_ODBC_SURFACE: dict[str, str] = {
    "items": "SQL Server ODBC read-only: product/inventory master tables",
    "customers": "SQL Server ODBC read-only: customer master tables",
    "vendors": "SQL Server ODBC read-only: vendor master tables",
    "purchase_order_lines": "SQL Server ODBC read-only: PO header + line tables",
    "inventory_snapshots": "SQL Server ODBC read-only: stock by branch (snapshot job)",
    "gl_entries": "SQL Server ODBC read-only: GL detail tables",
}

_SMARTVIEW_SURFACE: dict[str, str] = {
    "items": "BisTrack Web Smart View API: product reporting endpoints",
    "customers": "BisTrack Web Smart View API: customer reporting endpoints",
    "vendors": "BisTrack Web Smart View API: vendor reporting endpoints",
    "sales_order_lines": "BisTrack Web Smart View API: order reporting endpoints",
    "invoice_lines": "BisTrack Web Smart View API: invoice reporting endpoints",
    "purchase_order_lines": "BisTrack Web Smart View API: PO reporting endpoints",
    "inventory_snapshots": "BisTrack Web Smart View API: stock reporting endpoints",
    "gl_entries": "BisTrack Web Smart View API: financial data exchange endpoints",
}

#: Planned incremental keys for entities whose watermark column is not public
#: (the skeleton's plan; the discovery pack pins the real columns).
_PLAN_INCREMENTAL_KEYS: dict[str, str] = {
    "items": "last-maintained timestamp (confirm on tenant)",
    "customers": "last-maintained timestamp (confirm on tenant)",
    "vendors": "last-maintained timestamp (confirm on tenant)",
    "purchase_order_lines": "PO date (confirm on tenant)",
    "inventory_snapshots": "snapshot date",
    "gl_entries": "GL entry date (confirm on tenant)",
}


@dataclass(frozen=True)
class DocumentEntitySpec:
    """A canonical line entity served by a spec-verified header/line table pair.

    ``columns`` maps canonical staging columns to source columns (header columns
    use the ``h.`` alias, line columns the ``l.`` alias — placeholder names the
    discovery pack pins per site). ``type_column`` is the header-side document
    type column (§7.1); its value rides every record under ``type_field`` and
    scopes both the scan predicate and the per-type watermark.
    """

    entity: str
    header_table: str
    line_table: str
    join_on: str  # code-declared join predicate between the h. and l. aliases
    columns: tuple[tuple[str, str], ...]  # (canonical, source) — header + line maps
    type_column: str
    type_field: str
    doc_column: str  # qualified document-number column (the keyset/watermark key)
    doc_field: str  # its canonical name in the mapped record
    line_no_column: str  # qualified line-number column (keyset tiebreaker)
    types_setting: str  # settings key holding the configured document-type values
    types_required: bool  # orders MUST be type-filtered (§7.1); invoices may not be

    @property
    def column_names(self) -> list[str]:
        return [canonical for canonical, _ in self.columns]


#: The spec-verified document surfaces (spec §3.2 rows 6-7 [V]). OrderLine and
#: InvoiceHeader follow the two public names; InvoiceLine is the expected
#: naming-pattern table ([D] — confirm at onboarding before live use).
_DOCUMENT_ENTITIES: dict[str, DocumentEntitySpec] = {
    "sales_order_lines": DocumentEntitySpec(
        entity="sales_order_lines",
        header_table="OrderHeader",
        line_table="OrderLine",
        join_on="h.order_no = l.order_no",
        columns=(
            ("order_no", "h.order_no"),
            ("line_no", "l.line_no"),
            ("order_date", "h.order_date"),
            ("customer_no", "h.customer_no"),
            ("branch_code", "h.branch_code"),
            ("salesperson_code", "h.salesperson_code"),
            ("item_no", "l.item_no"),
            ("uom", "l.uom"),
            ("ordered_qty", "l.ordered_qty"),
            ("filled_qty", "l.filled_qty"),
            ("cancelled_qty", "l.cancelled_qty"),
            ("unit_price", "l.unit_price"),
            ("unit_cost", "l.unit_cost"),
            ("promised_date", "l.promised_date"),
            ("shipped_date", "l.shipped_date"),
            ("order_status", "l.order_status"),
        ),
        type_column="h.order_type",
        type_field="order_type",
        doc_column="h.order_no",
        doc_field="order_no",
        line_no_column="l.line_no",
        types_setting="order_document_types",
        types_required=True,
    ),
    "invoice_lines": DocumentEntitySpec(
        entity="invoice_lines",
        header_table="InvoiceHeader",
        line_table="InvoiceLine",  # expected by naming pattern [D — confirm]
        join_on="h.invoice_no = l.invoice_no",
        columns=(
            ("invoice_no", "h.invoice_no"),
            ("line_no", "l.line_no"),
            ("invoice_date", "h.invoice_date"),
            ("order_no", "h.order_no"),
            ("customer_no", "h.customer_no"),
            ("branch_code", "h.branch_code"),
            ("item_no", "l.item_no"),
            ("uom", "l.uom"),
            ("invoiced_qty", "l.invoiced_qty"),
            ("unit_price", "l.unit_price"),
            ("unit_cost", "l.unit_cost"),
            ("freight_amt", "l.freight_amt"),
            ("tax_amt", "l.tax_amt"),
        ),
        type_column="h.invoice_type",
        type_field="invoice_type",
        doc_column="h.invoice_no",
        doc_field="invoice_no",
        line_no_column="l.line_no",
        types_setting="invoice_document_types",
        types_required=False,
    ),
}


def _max_document(a: str | None, b: str | None) -> str | None:
    """Number-aware max of two document numbers.

    Per-type sequences are monotonic (spec §4.2) but may be unpadded integers,
    where a lexical compare misorders ("999" > "1000"); when both sides parse
    as integers compare numerically, else lexically (opaque document codes).
    """
    if a is None:
        return b
    if b is None:
        return a
    try:
        return a if int(a) >= int(b) else b
    except ValueError:
        return a if a >= b else b


def _parse_type_watermark(raw: str | None, entity: str) -> dict[str, str]:
    """Parse the stored per-type checkpoint (strictly).

    The checkpoint is connector-written JSON — a value that fails to parse is a
    corrupt store, never a filter to guess at (NetSuite watermark precedent):
    refuse before building any SQL.
    """
    if raw is None:
        return {}
    try:
        parsed: object = json.loads(raw)
    except ValueError as exc:
        raise ConnectorError(
            f"unparseable BisTrack per-type watermark for {entity} ({raw!r}) — "
            "refusing to build a scan from a corrupt checkpoint"
        ) from exc
    if not isinstance(parsed, dict) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in parsed.items()
    ):
        raise ConnectorError(
            f"unparseable BisTrack per-type watermark for {entity} ({raw!r}) — "
            "expected a JSON object of document type to max document number"
        )
    return parsed


def _format_type_watermark(mapping: dict[str, str]) -> str:
    return json.dumps({key: mapping[key] for key in sorted(mapping)})


class BisTrackConnector(DbApiBatchConnector):
    """BisTrack adapter: ODBC extraction implemented, Smart View a documented plan.

    Credential-gated and discovery-gated; ``dry_run()`` needs no network, no
    driver, and no live SQL Server.
    """

    erp_id = "bistrack"
    transport_label = "read-only ODBC (SQL Server) via pyodbc"
    param_placeholder = "?"  # pyodbc/ODBC paramstyle
    extraction_notes = (
        "Epicor BisTrack: on-prem SQL Server via a dealer-granted read-only ODBC "
        "login (spec §2.2 — db_datareader only, encrypted connection, replica over "
        "the transactional primary), or the BisTrack Web Smart View API where direct "
        "DB access is not granted (separately licensed Epicor product — documented "
        "plan only, spec §2.1). Only OrderHeader/OrderLine/InvoiceHeader are public "
        "table names (spec §3.2); every column name is a placeholder pinned per site "
        "with the §3.3 discovery pack. Order extraction filters on the configured "
        "document types and checkpoints the maximum document number PER type (§4.2, "
        "§7.1 — quotes/call-offs/reservations/templates have their own numbering and "
        "are never ingested as orders); pages advance by a keyset predicate, never "
        "OFFSET windows (§6.4). Source UOM rides every quantity and price — values "
        "are never normalized at extraction (§7.3). branch_code rides every row; "
        "inventory keys are branch-scoped (§7.4). No soft-delete flag is publicly "
        "documented — deletes reconcile via the anti-join key inventory (§6). "
        "Malformed rows quarantine with a machine-readable reason and fail the run. "
        "Status strings map per release version (§7.5) and pass through raw. Not yet "
        "exercised against a live site; run the discovery pack and pin tables, "
        "columns, document types, and number formats at onboarding."
    )
    #: Natural keys — the document entities carry their document type as scope
    #: (per-type numbering sequences can repeat a number across types, §7.1;
    #: NetSuite subsidiary-prefix precedent). Inventory stays branch-scoped.
    natural_key_fields: ClassVar[dict[str, tuple[str, ...]]] = {
        "items": ("item_no",),
        "customers": ("customer_no",),
        "vendors": ("vendor_no",),
        "sales_order_lines": ("order_type", "order_no", "line_no"),
        "invoice_lines": ("invoice_type", "invoice_no", "line_no"),
        "purchase_order_lines": ("po_no", "line_no"),
        "inventory_snapshots": ("snapshot_date", "branch_code", "item_no"),
        "gl_entries": ("journal_no", "line_no"),
    }
    PAGE_SIZE = 500

    def __init__(self, source, store, config, connection_factory=None) -> None:
        super().__init__(source, store, config, connection_factory)
        self._max_type_seen: dict[tuple[str, str], str] = {}

    # ------------------------------------------------------------------
    # Mode awareness (spec §3.1's read surfaces)
    # ------------------------------------------------------------------

    @property
    def mode(self) -> str:
        mode = self.source.settings.get("mode", ODBC_MODE)
        if mode not in (ODBC_MODE, SMARTVIEW_MODE):
            raise ConnectorError(
                f"bistrack mode '{mode}' is invalid; expected '{ODBC_MODE}' or '{SMARTVIEW_MODE}'"
            )
        return mode

    @property
    def maturity(self) -> ConnectorMaturity:
        """ODBC extraction is coded (unexercised live); Smart View is a plan."""
        return (
            ConnectorMaturity.IMPLEMENTED if self.mode == ODBC_MODE else ConnectorMaturity.SKELETON
        )

    # ------------------------------------------------------------------
    # Contract surface
    # ------------------------------------------------------------------

    def entities(self) -> list[str]:
        return list(self.natural_key_fields)

    def validate_config(self) -> list[str]:
        try:
            mode = self.mode
        except ConnectorError as exc:
            return [str(exc)]
        if mode == SMARTVIEW_MODE:
            missing = [
                setting
                for setting in ("smartview_base_url", "smartview_api_key")
                if not self.source.settings.get(setting)
            ]
            if missing:
                return [
                    f"missing settings required for live extraction: {', '.join(missing)} "
                    "(see .env.example; the source stays disabled until configured)"
                ]
            return []
        missing = [
            setting
            for setting in ("odbc_dsn", "db_user", "db_password")
            if not self.source.settings.get(setting)
        ]
        problems = []
        if missing:
            problems.append(
                f"missing settings required for live extraction: {', '.join(missing)} "
                "(read-only login per spec §2.2; see .env.example; the source stays "
                "disabled until configured)"
            )
        if not self._configured_types("order_document_types"):
            problems.append(
                "order_document_types must list the BisTrack order document types to "
                "extract (spec §7.1: quotes, call-off orders, reservations, and template "
                "orders carry their own numbering sequences and must never be ingested "
                "as orders; per-site type values are pinned at onboarding)"
            )
        return problems

    def describe_extraction(self, entity: str) -> ExtractionPlan:
        if self.mode == SMARTVIEW_MODE:
            return ExtractionPlan(
                entity=entity,
                surface=(
                    f"{_SMARTVIEW_SURFACE[entity]} — separately licensed BisTrack API "
                    "product (spec §2.1): documented plan only"
                ),
                incremental_key=_PLAN_INCREMENTAL_KEYS.get(entity),
                notes=self.extraction_notes,
            )
        spec = _DOCUMENT_ENTITIES.get(entity)
        if spec is not None:
            return ExtractionPlan(
                entity=entity,
                surface=(
                    f"{self.transport_label}: SELECT {len(spec.columns)} canonical columns "
                    f"FROM {spec.line_table} l JOIN {spec.header_table} h ON {spec.join_on} "
                    f"WHERE {spec.type_column} = <configured {spec.types_setting}> "
                    f"[AND {spec.doc_column} > <per-type watermark>] "
                    f"ORDER BY {spec.doc_column}, {spec.line_no_column} — keyset pages of "
                    f"{self.PAGE_SIZE} (spec §6.4: never OFFSET windows)"
                ),
                incremental_key=(
                    f"{spec.doc_column} per {spec.type_column} value ({spec.types_setting}) "
                    "— per-type numbering watermark, spec §4.2/§7.1"
                ),
                notes=self.extraction_notes,
            )
        return ExtractionPlan(
            entity=entity,
            surface=(
                f"{self.transport_label}: {_ODBC_SURFACE[entity]} — physical table name is "
                "not public (spec §3.2 [D]): pinned at onboarding via the §3.3 discovery "
                "pack before mapping code ships"
            ),
            incremental_key=_PLAN_INCREMENTAL_KEYS.get(entity),
            notes=self.extraction_notes,
        )

    # ------------------------------------------------------------------
    # Extraction
    # ------------------------------------------------------------------

    def _iter_records(
        self, entity: str, mode: ExtractionMode, watermark: str | None
    ) -> Iterator[dict[str, object]]:
        self._require_config()
        if self.mode == SMARTVIEW_MODE:
            raise ConnectorNotImplemented(
                f"bistrack smartview extraction is a documented plan behind the separately "
                f"licensed BisTrack API (spec §2.1); planned surface for '{entity}': "
                f"{_SMARTVIEW_SURFACE[entity]}"
            )
        spec = _DOCUMENT_ENTITIES.get(entity)
        if spec is None:
            raise ConnectorNotImplemented(
                f"bistrack extraction for '{entity}' awaits the physical table name: pin it "
                f"at onboarding with the spec §3.3 discovery pack, then map it (planned "
                f"surface: {self.describe_extraction(entity).surface})"
            )
        yield from self._iter_document_records(entity, spec, mode, watermark)

    def _iter_document_records(
        self,
        entity: str,
        spec: DocumentEntitySpec,
        mode: ExtractionMode,
        watermark: str | None,
    ) -> Iterator[dict[str, object]]:
        """Scan one configured document type at a time, keyset-paged (spec §4.2/§6.4)."""
        stored = (
            _parse_type_watermark(watermark, entity) if mode is ExtractionMode.INCREMENTAL else {}
        )
        names = spec.column_names
        expected_width = len(names) + 1  # the document-type column rides the SELECT tail
        for scan_key, doc_type in self._scan_plan(entity, spec):
            type_watermark = stored.get(scan_key) if mode is ExtractionMode.INCREMENTAL else None
            last_doc: str | None = None
            last_line: object = None
            while True:
                sql, params = self._document_page_sql(
                    spec, doc_type, type_watermark, last_doc, last_line
                )
                page = self._fetch_page(sql, params)
                for row in page:
                    # Observation rides inside _map_document_row (incremental only).
                    yield self._map_document_row(
                        entity, spec, names, expected_width, row, scan_key, mode
                    )
                if len(page) < self.PAGE_SIZE:
                    break
                cursor_row = page[-1]
                last_doc = str(cursor_row[names.index(spec.doc_field)])
                last_line = cursor_row[names.index("line_no")]

    def _map_document_row(
        self,
        entity: str,
        spec: DocumentEntitySpec,
        names: list[str],
        expected_width: int,
        row: tuple,
        scan_key: str,
        mode: ExtractionMode,
    ) -> dict[str, object]:
        """Positionally map one driver row; quarantine and fail closed on malformation."""
        if len(row) != expected_width:
            self._quarantine_row(
                entity,
                [*names, spec.type_field],
                row,
                f"source returned {len(row)} columns for bistrack/{entity}; expected "
                f"{expected_width} — column map is stale",
            )
            raise ConnectorError(
                f"malformed BisTrack row for {entity}: {len(row)} columns, expected "
                f"{expected_width} — column map is stale"
            )
        record: dict[str, object] = dict(zip(names, row[: len(names)], strict=True))
        record[spec.type_field] = row[-1]
        missing = [field for field in self.natural_key_fields[entity] if record.get(field) is None]
        if missing:
            self._quarantine_row(
                entity,
                [*names, spec.type_field],
                row,
                f"natural key fields {missing} are NULL for bistrack/{entity} — refusing "
                "to stage an unstampeable row",
            )
            raise ConnectorError(
                f"malformed BisTrack row for {entity}: natural key fields {missing} are NULL"
            )
        if mode is ExtractionMode.INCREMENTAL:
            self._observe_type_max(entity, scan_key, record[spec.doc_field])
        return record

    def source_key_inventory(self, entity: str) -> set[str]:
        """Key-only scan of the configured document types — the anti-join's source side."""
        self._require_config()
        if self.mode == SMARTVIEW_MODE:
            raise ConnectorNotImplemented(
                f"bistrack smartview key inventory is a documented plan behind the "
                f"separately licensed BisTrack API (spec §2.1); no key-inventory "
                f"surface exists for '{entity}'"
            )
        spec = _DOCUMENT_ENTITIES.get(entity)
        if spec is None:
            raise ConnectorNotImplemented(
                f"bistrack key inventory for '{entity}' awaits the physical table name "
                "(spec §3.3 discovery pack)"
            )
        key_fields = self.natural_key_fields[entity]
        mapped = {canonical: source for canonical, source in spec.columns}
        key_canons = [field for field in key_fields if field != spec.type_field]
        missing = [field for field in key_canons if field not in mapped]
        if missing:
            raise ConnectorError(
                f"bistrack {entity} column map does not declare natural key columns {missing}"
            )
        select_list = ", ".join(
            f"{validate_identifier(mapped[canon], 'source column')} AS "
            f"{validate_identifier(canon, 'alias')}"
            for canon in key_canons
        )
        select_list += f", {validate_identifier(spec.type_column, 'document-type column')} AS {spec.type_field}"
        sql = (
            f"SELECT DISTINCT {select_list} FROM {validate_identifier(spec.line_table, 'table')} l "
            f"JOIN {validate_identifier(spec.header_table, 'table')} h ON {spec.join_on}"
        )
        params: list[object] = []
        types = [doc_type for _, doc_type in self._scan_plan(entity, spec) if doc_type is not None]
        if types:
            placeholders = ", ".join(self.param_placeholder for _ in types)
            sql += f" WHERE {spec.type_column} IN ({placeholders})"
            params = tuple(types)
        cursor = self._connect().cursor()
        try:
            cursor.execute(sql, params or None)
            rows = cursor.fetchall()
        finally:
            cursor.close()
        scan_names = [*key_canons, spec.type_field]
        keys: set[str] = set()
        for row in rows:
            flat = dict(zip(scan_names, row, strict=True))
            # ids are built in the DECLARED natural-key order so they live in
            # the same id space as _stamp's staged keys — otherwise the
            # anti-join compares incompatible formats (base.natural_id_for)
            keys.add(natural_id_for(key_fields, flat))
        return keys

    def current_watermark(
        self, entity: str, mode: ExtractionMode, watermark_before: str | None
    ) -> str | None:
        """Compose the per-type checkpoint: stored maxima merged with this run's."""
        if mode is not ExtractionMode.INCREMENTAL:
            return watermark_before
        observed = {
            doc_type: value
            for (candidate, doc_type), value in self._max_type_seen.items()
            if candidate == entity
        }
        if not observed:
            return watermark_before
        merged = _parse_type_watermark(watermark_before, entity)
        for doc_type, value in observed.items():
            merged[doc_type] = _max_document(merged.get(doc_type), value)
        return _format_type_watermark(merged)

    # ------------------------------------------------------------------
    # BisTrack scan plumbing (per-type scans, keyset paging, quarantine)
    # ------------------------------------------------------------------

    def _scan_plan(self, entity: str, spec: DocumentEntitySpec) -> list[tuple[str, str | None]]:
        """(watermark key, type predicate value) per configured document type.

        Orders MUST be type-scoped (spec §7.1) — an empty ``order_document_types``
        refuses to scan rather than silently ingesting quotes. Invoices scan
        unscoped (key ``"*"``) when no type list is configured.
        """
        raw = self._configured_types(spec.types_setting)
        if raw:
            return [(doc_type, doc_type) for doc_type in sorted(set(raw))]
        if spec.types_required:
            raise ConnectorNotConfigured(
                f"bistrack {spec.types_setting} is empty — refusing to scan '{entity}' "
                "without a document-type filter (spec §7.1: separate numbering per "
                "document type; quotes must never be ingested as orders)"
            )
        return [(UNSCOPED, None)]

    def _configured_types(self, setting: str) -> list[str]:
        raw = self.source.settings.get(setting, "") or ""
        return [value.strip() for value in raw.split(",") if value.strip()]

    def _document_page_sql(
        self,
        spec: DocumentEntitySpec,
        doc_type: str | None,
        type_watermark: str | None,
        last_doc: str | None,
        last_line: object,
    ) -> tuple[str, tuple[object, ...]]:
        """One keyset page of the document scan: predicate-cursed, never OFFSET."""
        select_list = ", ".join(
            f"{validate_identifier(source, 'source column')} AS "
            f"{validate_identifier(canonical, 'alias')}"
            for canonical, source in spec.columns
        )
        select_list += f", {validate_identifier(spec.type_column, 'document-type column')} AS {spec.type_field}"
        sql = (
            f"SELECT {select_list} FROM {validate_identifier(spec.line_table, 'table')} l "
            f"JOIN {validate_identifier(spec.header_table, 'table')} h ON {spec.join_on}"
        )
        params: list[object] = []
        wheres: list[str] = []
        if doc_type is not None:
            wheres.append(f"{spec.type_column} = {self.param_placeholder}")
            params.append(doc_type)
        if type_watermark:
            wheres.append(f"{spec.doc_column} > {self.param_placeholder}")
            params.append(type_watermark)
        if last_doc is not None:
            # Keyset cursor (spec §6.4): the page starts strictly after the last
            # row's (document number, line number). OFFSET 0 is present only
            # because T-SQL requires it before FETCH NEXT.
            wheres.append(
                f"({spec.doc_column} > {self.param_placeholder} OR "
                f"({spec.doc_column} = {self.param_placeholder} AND "
                f"{spec.line_no_column} > {self.param_placeholder}))"
            )
            params.extend([last_doc, last_doc, last_line])
        if wheres:
            sql += " WHERE " + " AND ".join(wheres)
        sql += (
            f" ORDER BY {spec.doc_column}, {spec.line_no_column} "
            f"OFFSET 0 ROWS FETCH NEXT {self.PAGE_SIZE} ROWS ONLY"
        )
        return sql, tuple(params)

    def _fetch_page(self, sql: str, params: tuple[object, ...]) -> list[tuple]:
        cursor = self._connect().cursor()
        try:
            cursor.execute(sql, params or None)
            return cursor.fetchall()
        finally:
            cursor.close()

    def _quarantine_row(self, entity: str, names: list[str], row: tuple, detail: str) -> None:
        """Persist an unreadable source row with a machine-readable reason
        (csv_sftp/NetSuite/P21 parity): the run fails, but the offending row is
        preserved for inspection instead of being dropped silently."""
        file_name = f"{entity}-{uuid.uuid4().hex}.json"
        target = self.config.quarantine_root / self.source.source_id / "odbc_rows" / file_name
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps({"columns": names, "row": [str(value) for value in row]}, default=str)
        target.write_text(payload, encoding="utf-8")
        self.store.record_quarantine(
            QuarantineRecord(
                source_id=self.source.source_id,
                batch_id=None,
                file_name=file_name,
                reason_code=RC_MALFORMED_ROW,
                detail=detail,
                quarantine_path=str(target),
                quarantined_at=datetime.now(UTC),
            )
        )

    def _observe_type_max(self, entity: str, scan_key: str, doc_number: object) -> None:
        text = str(doc_number)
        key = (entity, scan_key)
        self._max_type_seen[key] = _max_document(self._max_type_seen.get(key), text)

    def _require_config(self) -> None:
        """Fail closed BEFORE any connection attempt — inert without credentials."""
        problems = self.validate_config()
        if problems:
            raise ConnectorNotConfigured(
                f"source {self.source.source_id} ({self.erp_id}) is not configurable: "
                + "; ".join(problems)
            )

    # ------------------------------------------------------------------
    # Driver plumbing (tests inject a factory; production builds lazily)
    # ------------------------------------------------------------------

    @staticmethod
    def _build_default_connection_factory():
        """Factory that imports pyodbc lazily — the driver is an optional install."""

        def factory(settings):
            try:
                import pyodbc
            except ImportError as exc:  # pragma: no cover - depends on site image
                raise ConnectorNotConfigured(
                    "pyodbc is not installed; install the ODBC extra to extract from "
                    "BisTrack's SQL Server"
                ) from exc
            try:
                return pyodbc.connect(
                    f"DSN={settings['odbc_dsn']};"
                    f"UID={settings['db_user']};PWD={settings['db_password']}",
                    Encrypt="yes",  # spec §2.2: encrypted connections are mandatory
                )
            except Exception as exc:  # pyodbc.Error and driver-specific bases vary
                raise ConnectorError(f"BisTrack ODBC connection failed: {exc}") from exc

        return factory

    def _connect(self):
        try:
            return self._connection_factory(self.source.settings)
        except ConnectorError:
            raise
        except Exception as exc:
            # Wrap driver-level failures (refused connections, bad DSNs) so every
            # extraction error is a ConnectorError — never a silent swallow.
            raise ConnectorError(
                f"BisTrack ODBC connection failed for {self.source.source_id}: {exc}"
            ) from exc
