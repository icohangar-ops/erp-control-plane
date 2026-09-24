"""netsuite — SuiteQL/SuiteAnalytics extraction adapter (SuiteTalk REST).

STATUS: implemented against the documented SuiteTalk REST surface (SuiteQL via
``POST https://{account}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql``,
OAuth1 HMAC-SHA256 token auth, LIMIT/OFFSET paging with pages of 200 rows,
Retry-After backoff on 429/503 per the account-level concurrency governance)
but NOT exercised against a live tenant — per the locked decisions, no
fabricated API behavior ships as tested. Enable only after sandbox-tenant
discovery; validate with ``python -m connectors.cli plan --source
netsuite_template`` (dry-run, no network) before any live call.

Extraction notes (research doc art_5rlAUYBI / art_NKUrngnG; blueprint §4's
extraction-pattern defaults are this connector's contract — no dedicated
NetSuite integration spec exists):

- SuiteQL returns flat rows over the transaction/transaction_line model.
  Every query is ORDERed by its entity's unique key so LIMIT/OFFSET windows
  are deterministic; OFFSET paging on an active table can still shift rows
  between pages mid-scan, so re-runs stay idempotent by natural key and long
  histories partition into watermark windows at onboarding.
- SuiteQL through REST returns a maximum of 100,000 results per query (when
  the SuiteAnalytics Connect feature is disabled) — the 100k-row ceiling.
  Extraction beyond it partitions by watermark windows; the SuiteAnalytics
  Connect workbook channel is the alternative for bulk history.
- The account concurrency limit — 5/15/20 base concurrent requests by
  service tier (Standard/Premium/Enterprise-Ultimate), shared across SOAP,
  REST, and RESTlet calls, +10 per SuiteCloud Plus license — governs the
  tenant, not this adapter: extraction runs strictly sequentially, one
  in-flight page at any time, honoring ``Retry-After`` on 429/503.
- Incremental watermarks ride ``lastmodifieddate`` (system audit stamp, UTC)
  — never ``trandate``, a business date with no time component. Watermarks
  are delete-blind (spec §6): hard deletes reconcile through the scheduled
  anti-join (``reconcile_deletes``), part of the contract.
- Subsidiary scoping: OneWorld accounts repeat document numbers across
  subsidiaries, so the ``subsidiaries`` setting scopes every query
  server-side and natural ids carry the row's subsidiary id — two
  subsidiaries' shared document numbers never collide in staging. Non-OneWorld
  accounts leave the setting empty; rows then carry a null subsidiary id.
- Multi-book accounting duplicates every transaction line per accounting
  book: ``accounting_book_id`` scopes the GL entries query; unscoped runs
  stage every book's lines (the book id rides the natural id).

TODO(per-tenant): confirm SuiteAnalytics column names for the inventory
snapshot table, subsidiary/book internal ids, and timestamp precision before
the first live run (every query below is reviewed at onboarding, per the
same convention the original two-entity skeleton established).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
import urllib.parse
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import ClassVar

import httpx
import pyarrow as pa

from connectors.base import (
    BaseConnector,
    ConnectorError,
    ConnectorMaturity,
    ConnectorNotConfigured,
    DeleteSemantics,
    ExtractionMode,
    ExtractionPlan,
    natural_id_for,
)
from control_plane.config import ControlPlaneConfig
from control_plane.models import QuarantineRecord, SourceConfig
from control_plane.store import ControlPlaneStore

SUITEQL_PATH = "/services/rest/query/v1/suiteql"

#: Reason codes recorded on quarantined API pages (csv_sftp quarantine parity).
RC_MALFORMED_PAGE = "MALFORMED_PAGE"
RC_UNPARSEABLE_JSON = "UNPARSEABLE_JSON"

#: The epoch literal for a first incremental run with no stored checkpoint.
_EPOCH_WATERMARK = "1900-01-01"


@dataclass(frozen=True)
class NsEntity:
    """How one canonical entity maps onto the SuiteQL surface."""

    #: Display name for the extraction plan's surface string.
    table: str
    #: SuiteQL with ``{select}`` and ``{predicate}`` slots (both built below).
    sql: str
    #: Source expression carrying the system audit stamp, or None (full
    #: snapshot each run — no incremental filter).
    incremental_field: str | None
    #: Unique-key ORDER BY — keeps OFFSET windows deterministic.
    order_by: str
    #: Source expression scoping subsidiaries, or None (global table).
    subsidiary_column: str | None
    #: Source expression scoping the accounting book, or None.
    accounting_book_column: str | None = None


_ENTITY_SQL: dict[str, str] = {
    "items": NsEntity(
        table="item",
        sql="SELECT {select} FROM item WHERE 1=1{predicate}",
        incremental_field="item.lastmodifieddate",
        order_by="item.itemid",
        subsidiary_column=None,
    ),
    "customers": NsEntity(
        table="customer",
        sql="SELECT {select} FROM customer WHERE 1=1{predicate}",
        incremental_field="customer.lastmodifieddate",
        order_by="customer.entityid",
        subsidiary_column="customer.subsidiary",
    ),
    "vendors": NsEntity(
        table="vendor",
        sql="SELECT {select} FROM vendor WHERE 1=1{predicate}",
        incremental_field="vendor.lastmodifieddate",
        order_by="vendor.entityid",
        subsidiary_column="vendor.subsidiary",
    ),
    "sales_order_lines": NsEntity(
        table="transaction_line ⋈ transaction (type='SalesOrd')",
        sql=(
            "SELECT {select} FROM transaction_line tl "
            "JOIN transaction t ON t.id = tl.transaction "
            "WHERE t.type = 'SalesOrd'{predicate}"
        ),
        incremental_field="t.lastmodifieddate",
        order_by="tl.id",
        subsidiary_column="t.subsidiary",
    ),
    "invoice_lines": NsEntity(
        table="transaction_line ⋈ transaction (type='CustInvc')",
        sql=(
            "SELECT {select} FROM transaction_line tl "
            "JOIN transaction t ON t.id = tl.transaction "
            "WHERE t.type = 'CustInvc'{predicate}"
        ),
        incremental_field="t.lastmodifieddate",
        order_by="tl.id",
        subsidiary_column="t.subsidiary",
    ),
    "inventory_snapshots": NsEntity(
        table="inventorybalance (SuiteAnalytics; per-tenant workbook alternative)",
        sql="SELECT {select} FROM inventorybalance ib WHERE 1=1{predicate}",
        incremental_field=None,
        order_by="ib.date, ib.locationid, ib.itemid",
        subsidiary_column=None,
    ),
    "gl_entries": NsEntity(
        table="transaction_accounting_line ⋈ transaction",
        sql=(
            "SELECT {select} FROM transaction_accounting_line tal "
            "JOIN transaction t ON t.id = tal.transaction "
            "WHERE 1=1{predicate}"
        ),
        incremental_field="t.lastmodifieddate",
        order_by="tal.id",
        subsidiary_column="t.subsidiary",
        accounting_book_column="tal.accountingbookid",
    ),
}

#: canonical field <- SuiteQL source expression per entity. The SELECT list is
#: built from this map (aliases are the canonical names, so raw rows ARE the
#: staging shape); natural-key scans select the subset under natural_key_fields.
#: Column names follow the documented SuiteAnalytics model and are reviewed at
#: onboarding (see module TODO) — the same convention the original two-entity
#: skeleton established.
_ENTITY_COLUMNS: dict[str, dict[str, str]] = {
    "items": {
        "item_no": "item.itemid",
        "description": "item.displayname",
        "item_type": "item.itemtype",
        "list_price": "item.baseprice",
        "item_status": "item.isinactive",
        "last_modified": "item.lastmodifieddate",
    },
    "customers": {
        "customer_no": "customer.entityid",
        "customer_name": "customer.companyname",
        "subsidiary_id": "customer.subsidiary",
        "customer_status": "customer.isinactive",
        "last_modified": "customer.lastmodifieddate",
    },
    "vendors": {
        "vendor_no": "vendor.entityid",
        "vendor_name": "vendor.companyname",
        "subsidiary_id": "vendor.subsidiary",
        "vendor_status": "vendor.isinactive",
        "last_modified": "vendor.lastmodifieddate",
    },
    "sales_order_lines": {
        "order_no": "t.tranid",
        "line_no": "tl.id",
        "order_date": "t.trandate",
        "customer_no": "t.entityid",
        "branch_code": "tl.locationid",
        "item_no": "tl.itemid",
        "ordered_qty": "tl.quantityordered",
        "filled_qty": "tl.quantitybilled",
        "unit_price": "tl.rate",
        "memo": "tl.memo",
        "subsidiary_id": "t.subsidiary",
        "last_modified": "t.lastmodifieddate",
    },
    "invoice_lines": {
        "invoice_no": "t.tranid",
        "line_no": "tl.id",
        "invoice_date": "t.trandate",
        "customer_no": "t.entityid",
        "branch_code": "tl.locationid",
        "item_no": "tl.itemid",
        "invoiced_qty": "tl.quantity",
        "unit_price": "tl.rate",
        "memo": "tl.memo",
        "subsidiary_id": "t.subsidiary",
        "last_modified": "t.lastmodifieddate",
    },
    "inventory_snapshots": {
        "snapshot_date": "ib.date",
        "branch_code": "ib.locationid",
        "item_no": "ib.itemid",
        "qty_available": "ib.quantityavailable",
        "reorder_point": "ib.reorderpoint",
    },
    "gl_entries": {
        "journal_no": "tal.transaction",
        "line_no": "tal.id",
        "entry_date": "t.trandate",
        "account": "tal.accountid",
        "debit_amt": "tal.debitamount",
        "credit_amt": "tal.creditamount",
        "subsidiary_id": "t.subsidiary",
        "accounting_book_id": "tal.accountingbookid",
        "last_modified": "t.lastmodifieddate",
    },
}

#: Per-entity extraction-plan caveats (blueprint §4 + module notes). These ride
#: describe_extraction() so onboarding sees them before wiring a tenant.
_ENTITY_NOTES: dict[str, str] = {
    "items": (
        "item is a global (non-subsidiary-scoped) table — natural ids carry no "
        "subsidiary prefix. Price levels/quantity-price breaks are not flat "
        "columns on the item row; confirm pricing extraction at onboarding."
    ),
    "customers": (
        "subsidiary_id is null on non-OneWorld accounts (natural ids then carry "
        "a 'None' prefix); on OneWorld accounts the subsidiaries setting scopes "
        "the query server-side. Confirm at onboarding."
    ),
    "vendors": (
        "subsidiary_id is null on non-OneWorld accounts (natural ids then carry "
        "a 'None' prefix); on OneWorld accounts the subsidiaries setting scopes "
        "the query server-side. Confirm at onboarding."
    ),
    "sales_order_lines": (
        "SuiteQL pages LIMIT/OFFSET — on an active transaction table an OFFSET "
        "window can shift rows mid-scan; re-runs are idempotent by natural key "
        "and long histories partition into watermark windows (SuiteQL via REST "
        "returns a maximum of 100,000 results per query)."
    ),
    "invoice_lines": (
        "SuiteQL pages LIMIT/OFFSET — on an active transaction table an OFFSET "
        "window can shift rows mid-scan; re-runs are idempotent by natural key "
        "and long histories partition into watermark windows (SuiteQL via REST "
        "returns a maximum of 100,000 results per query)."
    ),
    "inventory_snapshots": (
        "Full snapshot each run (no incremental field): every run re-stages the "
        "current balances by location. SuiteAnalytics column names for the "
        "balance table are confirmed at onboarding (per-tenant workbook "
        "alternative per the research doc); location→subsidiary rollup likewise."
    ),
    "gl_entries": (
        "Multi-book accounting duplicates every transaction line per accounting "
        "book: the accounting_book_id setting scopes the query, and unscoped "
        "runs stage every book's lines (the book id rides the natural id). "
        "Watermarked on the transaction's lastmodifieddate."
    ),
}

#: Explicit Arrow types for canonical fields that are not strings. NetSuite
#: quantities/amounts arrive as JSON numbers — float64 accepts int or float.
_ARROW_FIELD_TYPES: dict[str, pa.DataType] = {
    "line_no": pa.int64(),
    "ordered_qty": pa.float64(),
    "filled_qty": pa.float64(),
    "invoiced_qty": pa.float64(),
    "unit_price": pa.float64(),
    "list_price": pa.float64(),
    "qty_available": pa.float64(),
    "reorder_point": pa.float64(),
    "debit_amt": pa.float64(),
    "credit_amt": pa.float64(),
}


def _ns_timestamp(raw: str) -> datetime:
    """Parse a NetSuite audit stamp for watermark comparison.

    SuiteQL returns mixed-precision stamps (``.6Z`` vs ``.603Z``) where a
    lexical string compare misorders instants — the checkpoint compares parsed
    values and keeps the original text for the next query's literal.
    """
    try:
        value = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ConnectorError(
            f"unparseable NetSuite timestamp {raw!r} — refusing to advance a "
            "watermark on it (storing one would silently skip records)"
        ) from exc
    if value.tzinfo is None:
        # NetSuite audit stamps are UTC; naive text is treated as UTC.
        value = value.replace(tzinfo=UTC)
    return value


def _sql_literal(value: str) -> str:
    """Single-quoted SQL literal for a config-sourced value (escape quotes)."""
    return "'" + value.replace("'", "''") + "'"


class NetsuiteConnector(BaseConnector):
    """SuiteQL adapter. Credential-gated; ``dry_run()`` needs no network."""

    erp_id = "netsuite"
    maturity = ConnectorMaturity.IMPLEMENTED  # coded — but see UNEXERCISED note above
    #: NetSuite extracts are watermark-incremental per entity (the inventory
    #: snapshot is a per-run re-stage, not a wholesale diff) — deletes surface
    #: only through the scheduled anti-join.
    full_snapshot = False
    delete_handling = DeleteSemantics.ANTI_JOIN
    extraction_notes = (
        "SuiteQL via POST /services/rest/query/v1/suiteql with OAuth1 "
        "HMAC-SHA256 token auth; LIMIT/OFFSET paging at 200 rows per page, "
        "ORDERed by each entity's unique key. SuiteQL via REST returns a "
        "maximum of 100,000 results per query (100k-row ceiling when "
        "SuiteAnalytics Connect is disabled) — partition long histories into "
        "watermark windows at onboarding. The account concurrency limit (5/15/20 "
        "base concurrent requests by service tier, shared across SOAP/REST/"
        "RESTlet calls, +10 per SuiteCloud Plus license) governs the tenant; "
        "this adapter runs strictly sequentially — one in-flight page at any "
        "time — and honors Retry-After on 429/503/504. Watermarks ride "
        "lastmodifieddate (UTC audit stamp) and are delete-blind (spec §6): the "
        "scheduled anti-join reconciliation tombstones vanished keys. The "
        "subsidiaries setting scopes OneWorld queries server-side and natural "
        "ids carry the row's subsidiary id; accounting_book_id scopes the GL. "
        "Malformed pages quarantine with a machine-readable reason and fail the "
        "run. Not yet exercised against a live tenant; confirm subsidiary/book "
        "internal ids, SuiteAnalytics column names, and timestamp precision at "
        "onboarding."
    )
    natural_key_fields: ClassVar[dict[str, tuple[str, ...]]] = {
        "items": ("item_no",),
        "customers": ("subsidiary_id", "customer_no"),
        "vendors": ("subsidiary_id", "vendor_no"),
        "sales_order_lines": ("subsidiary_id", "order_no", "line_no"),
        "invoice_lines": ("subsidiary_id", "invoice_no", "line_no"),
        "inventory_snapshots": ("snapshot_date", "branch_code", "item_no"),
        "gl_entries": ("subsidiary_id", "accounting_book_id", "journal_no", "line_no"),
    }

    REQUIRED_SETTINGS: ClassVar[tuple[str, ...]] = (
        "account_id",
        "consumer_key",
        "consumer_secret",
        "token_id",
        "token_secret",
    )

    PAGE_SIZE = 200  # SuiteQL REST page guidance keeps pages modest
    MAX_RETRIES = 5
    BACKOFF_SECONDS = 2.0

    def __init__(
        self, source: SourceConfig, store: ControlPlaneStore, config: ControlPlaneConfig
    ) -> None:
        super().__init__(source, store, config)
        self._http_client: httpx.Client | None = None
        self._max_incremental_seen: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Contract surface
    # ------------------------------------------------------------------

    def entities(self) -> list[str]:
        return list(self.natural_key_fields)

    def validate_config(self) -> list[str]:
        missing = [f for f in self.REQUIRED_SETTINGS if not self.source.settings.get(f)]
        if missing:
            return [
                f"missing required NetSuite credential settings: {', '.join(missing)} "
                "(see .env.example; source stays disabled until configured)"
            ]
        if not self.source.settings.get("account_id", "").strip().replace("-", "").isalnum():
            return [
                "setting 'account_id' must be the NetSuite account id (the host "
                "prefix of {account}.suitetalk.api.netsuite.com)"
            ]
        return []

    def describe_extraction(self, entity: str) -> ExtractionPlan:
        spec = self._entity_spec(entity)
        surface = f"SuiteQL POST {SUITEQL_PATH} over {spec.table}"
        scope_bits: list[str] = []
        subsidiaries = self._subsidiaries()
        if spec.subsidiary_column is not None:
            scope_bits.append(
                f"subsidiaries {', '.join(subsidiaries)}"
                if subsidiaries
                else "all subsidiaries (no restriction configured)"
            )
        if spec.accounting_book_column is not None:
            book = self._accounting_book_id()
            scope_bits.append(
                f"accounting book {book}" if book else "all accounting books (no id configured)"
            )
        if scope_bits:
            surface += f" — scoped: {'; '.join(scope_bits)}"
        notes = "\n".join(
            part for part in (_ENTITY_NOTES.get(entity, ""), self.extraction_notes) if part
        )
        return ExtractionPlan(
            entity=entity,
            surface=surface,
            incremental_key=spec.incremental_field,
            notes=notes,
        )

    def arrow_schema(self, entity: str) -> pa.Schema:
        """Declared staging schema — never infer types from the first batch."""
        fields = [
            pa.field(name, _ARROW_FIELD_TYPES.get(name, pa.string()))
            for name in sorted(_ENTITY_COLUMNS[entity])
        ]
        fields += [
            pa.field("source_system", pa.string()),
            pa.field("source_id", pa.string()),
            pa.field("loaded_at", pa.string()),
        ]
        return pa.schema(fields)

    # ------------------------------------------------------------------
    # Extraction
    # ------------------------------------------------------------------

    def _iter_records(
        self, entity: str, mode: ExtractionMode, watermark: str | None
    ) -> Iterator[dict[str, object]]:
        self._require_config()
        spec = self._entity_spec(entity)
        sql = self._build_sql(entity, spec, mode, watermark)
        offset = 0
        while True:
            paged = f"{sql} LIMIT {self.PAGE_SIZE} OFFSET {offset}"
            rows = self._suiteql(entity, paged)
            self._observe_incremental(entity, spec, rows)
            if not rows:
                return
            yield from rows
            if len(rows) < self.PAGE_SIZE:
                return
            offset += self.PAGE_SIZE

    def source_key_inventory(self, entity: str) -> set[str]:
        """Key-only SuiteQL scan — the anti-join's source side.

        Natural ids are subsidiary-prefixed (plus the accounting book on GL
        entries) so OneWorld tenants' repeated document numbers and multi-book
        duplicates never collapse into one staging key.
        """
        self._require_config()
        spec = self._entity_spec(entity)
        key_fields = self.natural_key_fields[entity]
        select = ", ".join(
            f"{expr} AS {canon}"
            for canon, expr in _ENTITY_COLUMNS[entity].items()
            if canon in key_fields
        )
        sql = self._build_sql(entity, spec, None, None, select=select)
        keys: set[str] = set()
        offset = 0
        while True:
            paged = f"{sql} LIMIT {self.PAGE_SIZE} OFFSET {offset}"
            rows = self._suiteql(entity, paged)
            if not rows:
                return keys
            keys.update(natural_id_for(key_fields, row) for row in rows)
            if len(rows) < self.PAGE_SIZE:
                return keys
            offset += self.PAGE_SIZE

    def current_watermark(
        self, entity: str, mode: ExtractionMode, watermark_before: str | None
    ) -> str | None:
        spec = self._entity_spec(entity)
        if mode is not ExtractionMode.INCREMENTAL or spec.incremental_field is None:
            return watermark_before
        # Checkpoint = the max lastmodifieddate observed across ALL pages of the
        # run — never a per-page max. The base persists it only after success.
        return self._max_incremental_seen.get(entity, watermark_before)

    # ------------------------------------------------------------------
    # SuiteQL plumbing (query assembly, paging, backoff, auth, quarantine)
    # ------------------------------------------------------------------

    def _build_sql(
        self,
        entity: str,
        spec: NsEntity,
        mode: ExtractionMode | None,
        watermark: str | None,
        select: str | None = None,
    ) -> str:
        """Assemble the query: SELECT list, watermark filter, scoping, ORDER BY."""
        predicate = ""
        if mode is ExtractionMode.INCREMENTAL and spec.incremental_field is not None:
            literal = self._watermark_literal(mode, watermark)
            predicate += f" AND {spec.incremental_field} > {literal}"
        if spec.subsidiary_column is not None and self._subsidiaries():
            ids = ", ".join(_sql_literal(s) for s in self._subsidiaries())
            predicate += f" AND {spec.subsidiary_column} IN ({ids})"
        if spec.accounting_book_column is not None and self._accounting_book_id():
            predicate += (
                f" AND {spec.accounting_book_column} = {_sql_literal(self._accounting_book_id())}"
            )
        columns = select if select is not None else self._select(entity)
        sql = spec.sql.format(select=columns, predicate=predicate)
        sql += f" ORDER BY {spec.order_by}"
        return " ".join(sql.split())

    def _watermark_literal(self, mode: ExtractionMode, watermark: str | None) -> str:
        """Validated SQL literal for the watermark (fail closed on garbage).

        The stored checkpoint is interpolated into SuiteQL, so it is parsed as
        an ISO timestamp first — an unparseable value can neither filter
        correctly nor inject, and raising beats storing a checkpoint that
        silently skips records.
        """
        if mode is ExtractionMode.INCREMENTAL and watermark:
            _ns_timestamp(watermark)  # validation — raises on garbage
            return _sql_literal(watermark)
        return _sql_literal(_EPOCH_WATERMARK)

    def _observe_incremental(
        self, entity: str, spec: NsEntity, rows: list[dict[str, object]]
    ) -> None:
        """Track the max lastmodifieddate seen across ALL pages of the run."""
        if spec.incremental_field is None:
            return
        for row in rows:
            observed = row.get("last_modified")
            if observed is None:
                continue
            text = str(observed)
            current = self._max_incremental_seen.get(entity)
            if current is None or _ns_timestamp(text) > _ns_timestamp(current):
                self._max_incremental_seen[entity] = text

    def _suiteql(self, entity: str, sql: str) -> list[dict[str, object]]:
        """POST one SuiteQL page with concurrency-governance backoff.

        429/503/504 back off honoring ``Retry-After`` (the account concurrency
        limit is shared across every integration, so throttling is expected
        under load). A 504 retries the same LIMIT/OFFSET window — unlike OData
        nextLink paging the window cannot shrink mid-scan without re-reading.
        A 200 whose body is not ``{"items": [object, ...]}`` is quarantined
        with a machine-readable reason and fails the run — never a silent
        drop, never a partial promote.
        """
        url = self._suiteql_url()
        delay = self.BACKOFF_SECONDS
        for attempt in range(1, self.MAX_RETRIES + 1):
            response = self._client().post(
                url,
                content=json.dumps({"q": sql}),
                headers=self._headers(url),
            )
            if response.status_code in (429, 503, 504):
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else delay
                if attempt == self.MAX_RETRIES:
                    raise ConnectorError(
                        f"NetSuite SuiteQL returned {response.status_code} on {url} "
                        f"after {self.MAX_RETRIES} backoff attempts"
                    )
                time.sleep(delay)
                delay *= 2
                continue
            try:
                response.raise_for_status()
            except httpx.HTTPError as exc:
                raise ConnectorError(f"NetSuite SuiteQL call failed: {exc}") from exc
            try:
                payload: object = response.json()
            except ValueError as exc:
                self._quarantine_page(entity, url, response.content, RC_UNPARSEABLE_JSON, str(exc))
                raise ConnectorError(
                    f"malformed NetSuite response on {url}: body is not JSON"
                ) from exc
            return self._validate_page(entity, url, payload)
        raise ConnectorError("unreachable: retry loop must return or raise")  # pragma: no cover

    def _validate_page(self, entity: str, url: str, payload: object) -> list[dict[str, object]]:
        rows = payload.get("items") if isinstance(payload, dict) else None
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            self._quarantine_page(
                entity,
                url,
                json.dumps(payload, default=str).encode("utf-8"),
                RC_MALFORMED_PAGE,
                "'items' must be a list of objects",
            )
            raise ConnectorError(
                f"malformed NetSuite page for {entity} at {url}: 'items' must be a list of objects"
            )
        return rows

    def _quarantine_page(
        self, entity: str, url: str, body: bytes, reason_code: str, detail: str
    ) -> None:
        file_name = f"{entity}-{uuid.uuid4().hex}.json"
        target = self.config.quarantine_root / self.source.source_id / "api_pages" / file_name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
        self.store.record_quarantine(
            QuarantineRecord(
                source_id=self.source.source_id,
                batch_id=None,
                file_name=file_name,
                reason_code=reason_code,
                detail=f"{detail} (from {url})",
                quarantine_path=str(target),
                quarantined_at=datetime.now(UTC),
            )
        )

    def _headers(self, url: str) -> dict[str, str]:
        return {
            "Authorization": self._oauth1_header(url, "POST"),
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Prefer": "transient",
        }

    def _oauth1_header(self, url: str, method: str) -> str:
        settings = self.source.settings
        realm = settings.get("realm") or settings["account_id"].upper()
        params = {
            "oauth_consumer_key": settings["consumer_key"],
            "oauth_nonce": uuid.uuid4().hex,
            "oauth_signature_method": "HMAC-SHA256",
            "oauth_timestamp": str(int(time.time())),
            "oauth_token": settings["token_id"],
            "oauth_version": "1.0",
        }
        base_url, _, query = url.partition("?")
        encoded = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        base_string = "&".join(
            (method.upper(), urllib.parse.quote(base_url, safe=""), urllib.parse.quote(encoded))
        )
        if query:  # SuiteQL posts carry no query string; kept for correctness
            base_string += "&" + urllib.parse.quote(query, safe="")
        signing_key = "&".join(
            (
                urllib.parse.quote(settings["consumer_secret"], safe=""),
                urllib.parse.quote(settings["token_secret"], safe=""),
            )
        )
        signature = base64.b64encode(
            hmac.new(signing_key.encode(), base_string.encode(), hashlib.sha256).digest()
        ).decode()
        params["oauth_signature"] = signature
        header = ", ".join(f'{k}="{urllib.parse.quote(v, safe="")}"' for k, v in params.items())
        return f'OAuth {header}, realm="{realm}"'

    def _suiteql_url(self) -> str:
        return (
            f"https://{self.source.settings['account_id'].strip()}"
            f".suitetalk.api.netsuite.com{SUITEQL_PATH}"
        )

    def _select(self, entity: str) -> str:
        return ", ".join(f"{expr} AS {canon}" for canon, expr in _ENTITY_COLUMNS[entity].items())

    def _subsidiaries(self) -> list[str]:
        """Configured subsidiary ids, in order, deduplicated (empty = unscoped)."""
        subsidiaries: list[str] = []
        for token in self.source.settings.get("subsidiaries", "").split(","):
            subsidiary_id = token.strip()
            if subsidiary_id and subsidiary_id not in subsidiaries:
                subsidiaries.append(subsidiary_id)
        return subsidiaries

    def _accounting_book_id(self) -> str | None:
        book = self.source.settings.get("accounting_book_id", "").strip()
        return book or None

    def _entity_spec(self, entity: str) -> NsEntity:
        spec = _ENTITY_SQL.get(entity)
        if spec is None:
            raise ConnectorError(
                f"entity '{entity}' has no SuiteQL mapping yet; known: "
                f"{', '.join(sorted(_ENTITY_SQL))}"
            )
        return spec

    def _require_config(self) -> None:
        """Fail closed BEFORE any network attempt — inert without credentials."""
        problems = self.validate_config()
        if problems:
            raise ConnectorNotConfigured(
                f"source {self.source.source_id} ({self.erp_id}) is not configurable: "
                + "; ".join(problems)
            )

    def _client(self) -> httpx.Client:
        if self._http_client is None:
            self._http_client = httpx.Client(timeout=60.0)
        return self._http_client
