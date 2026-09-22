"""d365_bc — Dynamics 365 Business Central extraction (API v2.0 / OData v4).

STATUS: implemented against the documented API v2.0 surface (OAuth2 client
credentials, per-company iteration over the configured companies list,
server-driven OData paging via ``@odata.nextLink`` with ``$top`` set
explicitly, ``Data-Access-Intent=ReadOnly`` to keep reads off the tenant
primary, HTTP 429/503 backoff and 504 page-shrink per the documented service
limits) but NOT exercised against a live tenant — per the locked decisions, no
fabricated API behavior ships as tested. Validate field maps against the
tenant's ``$metadata`` and run ``python -m connectors.cli plan --source <id>``
(dry-run) before any live call.

Extraction notes (integration spec "D365 BC — Connector Mechanisms",
art_3iTLa6aV; rollout posture art_TQ2kx5ZN):
- API v2.0 is company-scoped: the only non-company-scoped call is
  ``GET /companies``; every entity URL is ``/companies({id})/{entitySet}``
  (spec pitfall 3). The connector loops the configured companies list and
  checkpoints the max ``lastModifiedDateTime`` observed across ALL of them —
  never a per-company max.
- ``salesInvoices`` is the invoice document aggregate, not a posted-invoice
  archive (spec pitfall 6) — the caveat rides the ``invoice_lines`` plan.
- Price lists, item attributes, and ship-to/order-address masters have no
  standard v2.0 entity (spec §3) — custom AL API pages per tenant; noted on
  the affected plans.
- Historical backfill is restore-side: BACPAC via the admin center, restored
  into Azure SQL/SQL Server (10 exports/environment/month) — never an
  in-connector path.
- Watermarks are delete-blind (spec pitfall 7): hard deletes reconcile
  through the scheduled anti-join (``reconcile_deletes``), part of the
  contract.
- Documented limits: 20,000 entities/page platform cap; this pack defaults
  far lower (see ``PAGE_SIZE``), honors ``Retry-After`` on 429/503, and
  shrinks its page on 504 (spec §5 handling).
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, ClassVar

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

TOKEN_URL_TEMPLATE = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
BC_API_BASE = "https://api.businesscentral.dynamics.com/v2.0"

#: Reason codes recorded on quarantined API pages (csv_sftp quarantine parity).
RC_MALFORMED_PAGE = "MALFORMED_PAGE"
RC_UNPARSEABLE_JSON = "UNPARSEABLE_JSON"


@dataclass(frozen=True)
class BcEntity:
    """How one canonical entity maps onto the API v2.0 surface."""

    #: Entity set under /companies({company_id})/.
    entity_set: str
    #: $select list — source field names kept verbatim.
    select: tuple[str, ...]
    #: Incremental filter field, or None (full snapshot each run).
    incremental_field: str | None
    #: Nested lines array to flatten (header-driven line entities), with the
    #: canonical prefix for its fields.
    lines_field: str | None = None
    lines_prefix: str | None = None


_ENTITY_MAPS: dict[str, BcEntity] = {
    "items": BcEntity(
        entity_set="items",
        select=("number", "displayName", "itemCategoryCode", "unitCost", "unitPrice", "blocked"),
        incremental_field="lastModifiedDateTime",
    ),
    "customers": BcEntity(
        entity_set="customers",
        select=(
            "number",
            "displayName",
            "paymentTermsCode",
            "creditLimit",
            "addressLine1",
            "city",
            "state",
            "postalCode",
        ),
        incremental_field="lastModifiedDateTime",
    ),
    "vendors": BcEntity(
        entity_set="vendors",
        select=("number", "displayName", "paymentTermsCode"),
        incremental_field="lastModifiedDateTime",
    ),
    "sales_order_lines": BcEntity(
        entity_set="salesOrders",
        select=("number", "orderDate", "customerNumber"),
        incremental_field="lastModifiedDateTime",
        lines_field="SalesOrderLines",
        lines_prefix="sales_order_line",
    ),
    "invoice_lines": BcEntity(
        entity_set="salesInvoices",
        select=("number", "invoiceDate", "customerNumber"),
        incremental_field="lastModifiedDateTime",
        lines_field="SalesInvoiceLines",
        lines_prefix="invoice_line",
    ),
    "purchase_order_lines": BcEntity(
        entity_set="purchaseOrders",
        select=("number", "orderDate", "vendorNumber"),
        incremental_field="lastModifiedDateTime",
        lines_field="PurchaseOrderLines",
        lines_prefix="purchase_order_line",
    ),
    "gl_entries": BcEntity(
        entity_set="generalLedgerEntries",
        select=(
            "id",
            "postingDate",
            "accountNumber",
            "description",
            "debitAmount",
            "creditAmount",
        ),
        incremental_field="postingDate",
    ),
}

#: canonical field <- source field per entity (header context for line rows).
_FIELD_MAPS: dict[str, dict[str, str]] = {
    "items": {
        "item_no": "number",
        "description": "displayName",
        "category": "itemCategoryCode",
        "unit_cost": "unitCost",
        "list_price": "unitPrice",
        "item_status": "blocked",
    },
    "customers": {
        "customer_no": "number",
        "customer_name": "displayName",
        "terms": "paymentTermsCode",
        "credit_limit": "creditLimit",
        "address1": "addressLine1",
        "city": "city",
        "state": "state",
        "postal_code": "postalCode",
    },
    "vendors": {"vendor_no": "number", "vendor_name": "displayName", "terms": "paymentTermsCode"},
    "sales_order_lines": {
        "order_no": "number",
        "order_date": "orderDate",
        "customer_no": "customerNumber",
        "line_no": "lineNo",
        "item_no": "itemNumber",
        "uom": "unitOfMeasure",
        "ordered_qty": "quantity",
        "filled_qty": "quantityShipped",
        "cancelled_qty": "quantityCancelled",
        "unit_price": "unitPrice",
        "promised_date": "promisedDeliveryDate",
        "shipped_date": "shipmentDate",
        "order_status": "lineStatus",
    },
    "invoice_lines": {
        "invoice_no": "number",
        "invoice_date": "invoiceDate",
        "customer_no": "customerNumber",
        "line_no": "lineNo",
        "item_no": "itemNumber",
        "uom": "unitOfMeasure",
        "invoiced_qty": "quantity",
        "unit_price": "unitPrice",
    },
    "purchase_order_lines": {
        "po_no": "number",
        "po_date": "orderDate",
        "vendor_no": "vendorNumber",
        "line_no": "lineNo",
        "item_no": "itemNumber",
        "uom": "unitOfMeasure",
        "ordered_qty": "quantity",
        "received_qty": "quantityReceived",
        "unit_cost_actual": "directUnitCost",
        "promised_date": "expectedReceiptDate",
    },
    "gl_entries": {
        "journal_no": "id",
        "entry_date": "postingDate",
        "account": "accountNumber",
        "description": "description",
        "debit_amt": "debitAmount",
        "credit_amt": "creditAmount",
    },
}

#: line-array fields used when flattening header-driven entities (canonical
#: field <- lines-array field, for the entities whose map has no explicit lines
#: entry — see _LINE_FIELD_MAPS).
_LINE_FIELD_MAPS: dict[str, dict[str, str]] = {
    "sales_order_lines": {
        "line_no": "lineNumber",
        "item_no": "itemNumber",
        "uom": "unitOfMeasure",
        "ordered_qty": "quantity",
        "filled_qty": "quantityShipped",
        "cancelled_qty": "quantityCancelled",
        "unit_price": "unitPrice",
        "promised_date": "promisedDeliveryDate",
        "shipped_date": "shipmentDate",
        "order_status": "lineStatus",
    },
    "invoice_lines": {
        "line_no": "lineNumber",
        "item_no": "itemNumber",
        "uom": "unitOfMeasure",
        "invoiced_qty": "quantity",
        "unit_price": "unitPrice",
    },
    "purchase_order_lines": {
        "line_no": "lineNumber",
        "item_no": "itemNumber",
        "uom": "unitOfMeasure",
        "ordered_qty": "quantity",
        "received_qty": "quantityReceived",
        "unit_cost_actual": "directUnitCost",
        "promised_date": "expectedReceiptDate",
    },
}

#: Per-entity extraction-plan caveats from the integration spec (art_3iTLa6aV).
#: These ride describe_extraction() so onboarding sees them before wiring a
#: tenant — they document surfaces this connector deliberately does not fake.
_ENTITY_NOTES: dict[str, str] = {
    "items": (
        "No price lists or item attributes in standard v2.0 (spec §3 rows 2, 11): "
        "unitPrice/unitCost ride the item; price-list extraction (Price List "
        "Header/Line, tables 7002/7003) and item attributes (tables 7500-7502) "
        "need a custom AL API page per tenant."
    ),
    "customers": (
        "Ship-to addresses ride documents and posted shipments; the ship-to "
        "address book (Ship-to Address, table 222) has no v2.0 entity — custom "
        "AL API page per tenant (spec §3 row 4)."
    ),
    "vendors": (
        "Vendor order addresses (Order Address, table 260) have no v2.0 entity — "
        "custom AL API page per tenant (spec §3 row 4)."
    ),
    "sales_order_lines": (
        "salesOrders is the open-document aggregate; line rows derive from the "
        "expanded SalesOrderLines array. Posted order history: BACPAC restore "
        "or a custom AL API over posted tables."
    ),
    "invoice_lines": (
        "salesInvoices is the invoice DOCUMENT AGGREGATE (status Draft / In "
        "Review / Open / Paid / Canceled / Corrective), NOT a posted-invoice "
        "archive (spec pitfall 6). Strict posted history: the "
        "microsoft/automate v1.0 postedSalesInvoices systemId route, a BACPAC "
        "restore, or a custom AL API over posted tables."
    ),
    "purchase_order_lines": (
        "No posted purchase-invoice entity exists at all (spec §3 row 8): open "
        "purchaseInvoices plus GET-only purchaseReceipts; posted history via "
        "BACPAC restore or a custom AL API."
    ),
    "gl_entries": (
        "generalLedgerEntries is GET-only and append-only; there is no API write "
        "path into the ledger (spec §3 row 10). Watermarked on postingDate — a "
        "plain business date with no time component (spec pitfall 5)."
    ),
}

#: Explicit Arrow types for canonical fields that are not strings. BC decimals
#: arrive as JSON numbers (int or float) — float64 accepts both; item_status
#: is the BC ``blocked`` boolean, staged typed and cast in dbt staging.
_ARROW_FIELD_TYPES: dict[str, pa.DataType] = {
    "line_no": pa.int64(),
    "credit_limit": pa.float64(),
    "unit_cost": pa.float64(),
    "list_price": pa.float64(),
    "ordered_qty": pa.float64(),
    "filled_qty": pa.float64(),
    "cancelled_qty": pa.float64(),
    "invoiced_qty": pa.float64(),
    "received_qty": pa.float64(),
    "unit_price": pa.float64(),
    "unit_cost_actual": pa.float64(),
    "debit_amt": pa.float64(),
    "credit_amt": pa.float64(),
    "item_status": pa.bool_(),
}


def _bc_timestamp(raw: str) -> datetime:
    """Parse a BC API timestamp for watermark comparison (UTC — spec pitfall 5).

    BC stores every DateTime as UTC and OData transfers carry the offset; a
    lexical string compare misorders mixed-precision stamps (``.6Z`` vs
    ``.603Z``), so the checkpoint compares parsed instants and keeps the
    original text for the next ``$filter``.
    """
    try:
        value = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ConnectorError(
            f"unparseable Business Central timestamp {raw!r} — refusing to advance a "
            "watermark on it (storing one would silently skip records)"
        ) from exc
    if value.tzinfo is None:
        # Business dates (postingDate) carry no offset; BC web services run UTC.
        value = value.replace(tzinfo=UTC)
    return value


class DynamicsBcConnector(BaseConnector):
    """Business Central API v2.0 adapter. Credential-gated; dry-runs need no network."""

    erp_id = "d365_bc"
    maturity = ConnectorMaturity.IMPLEMENTED  # coded — but see UNEXERCISED note above
    #: BC extracts are watermark-incremental per entity — never a wholesale
    #: replacement — so deletes surface only through the scheduled anti-join.
    full_snapshot = False
    delete_handling = DeleteSemantics.ANTI_JOIN
    extraction_notes = (
        "API v2.0 / OData v4 per company: $top + @odata.nextLink continuation "
        "paging, Data-Access-Intent=ReadOnly, Retry-After backoff on 429/503 and "
        "page-shrink on 504 per the documented limits (spec §5). The checkpoint "
        "is the max lastModifiedDateTime observed across ALL configured "
        "companies — never a per-company max (spec pitfall 3). Watermarks are "
        "delete-blind (spec pitfall 7): the scheduled anti-join reconciliation "
        "tombstones vanished keys. Historical backfill is restore-side: "
        "admin-center BACPAC export restored into Azure SQL/SQL Server (10 "
        "exports/environment/month) — never an in-connector path. Malformed "
        "pages quarantine with a machine-readable reason and fail the run. Not "
        "yet exercised against a live tenant; validate field maps against "
        "$metadata and confirm Entra app registration, permission sets, and "
        "company scoping at onboarding."
    )
    natural_key_fields: ClassVar[dict[str, tuple[str, ...]]] = {
        "items": ("company_id", "item_no"),
        "customers": ("company_id", "customer_no"),
        "vendors": ("company_id", "vendor_no"),
        "sales_order_lines": ("company_id", "order_no", "line_no"),
        "invoice_lines": ("company_id", "invoice_no", "line_no"),
        "purchase_order_lines": ("company_id", "po_no", "line_no"),
        "gl_entries": ("company_id", "journal_no"),
    }
    required_settings: ClassVar[tuple[str, ...]] = (
        "tenant_id",
        "client_id",
        "client_secret",
        "environment",
        "companies",
    )
    PAGE_SIZE = 1000  # documented $top cap is 20,000 — stay conservative
    MIN_PAGE_SIZE = 100
    MAX_RETRIES = 5
    BACKOFF_SECONDS = 2.0

    def __init__(
        self, source: SourceConfig, store: ControlPlaneStore, config: ControlPlaneConfig
    ) -> None:
        super().__init__(source, store, config)
        self._http_client: httpx.Client | None = None
        self._cached_token: str | None = None
        self._token_expiry = 0.0
        self._max_incremental_seen: dict[str, str] = {}
        self._page_size: int | None = None  # halved on 504 (spec §5)

    # ------------------------------------------------------------------
    # Contract surface
    # ------------------------------------------------------------------

    def entities(self) -> list[str]:
        return list(self.natural_key_fields)

    def validate_config(self) -> list[str]:
        missing = [f for f in self.required_settings if not self.source.settings.get(f)]
        if missing:
            return [
                f"missing required Business Central settings: {', '.join(missing)} "
                "(see .env.example; source stays disabled until configured)"
            ]
        if not self._companies():
            return [
                "setting 'companies' resolves to no usable company ids — provide a "
                "comma-separated list of BC company ids in D365BC_COMPANIES "
                "(enumerate with GET /companies)"
            ]
        return []

    def describe_extraction(self, entity: str) -> ExtractionPlan:
        mapping = self._entity_map(entity)
        api_version = self.source.settings.get("api_version") or "v2.0"
        surface = f"API {api_version}: GET /companies({{id}})/{mapping.entity_set}"
        if mapping.lines_field:
            surface += f"?$expand={mapping.lines_field}"
        count = len(self._companies())
        surface += f" across {count} configured compan{'y' if count == 1 else 'ies'}"
        notes = "\n".join(
            part for part in (_ENTITY_NOTES.get(entity, ""), self.extraction_notes) if part
        )
        return ExtractionPlan(
            entity=entity,
            surface=surface,
            incremental_key=mapping.incremental_field,
            notes=notes,
        )

    def arrow_schema(self, entity: str) -> pa.Schema:
        """Declared staging schema — never infer types from the first batch.

        Multi-company runs mix null patterns per company; first-batch inference
        would mis-type a column that happens to be all-null in company 1.
        """
        canonical = set(_FIELD_MAPS[entity]) | set(_LINE_FIELD_MAPS.get(entity, {}))
        canonical.add("company_id")
        fields = [
            pa.field(name, _ARROW_FIELD_TYPES.get(name, pa.string())) for name in sorted(canonical)
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
        mapping = self._entity_map(entity)
        for company_id in self._companies():
            for row in self._paged(mapping, entity, mode, watermark, company_id):
                yield from self._flatten(entity, row, company_id)

    def _flatten(
        self, entity: str, row: dict[str, object], company_id: str
    ) -> Iterator[dict[str, object]]:
        """Fold one API row into canonical staging records, company-scoped."""
        mapping = self._entity_map(entity)
        field_map = _FIELD_MAPS[entity]
        if mapping.lines_field is None:
            record = {canon: row.get(source) for canon, source in field_map.items()}
            record["company_id"] = company_id
            yield record
            return
        header_context = {
            canon: row.get(source)
            for canon, source in field_map.items()
            if canon not in _LINE_FIELD_MAPS[entity]
        }
        for line in row.get(mapping.lines_field) or []:
            record: dict[str, object] = dict(header_context)
            record.update(
                {canon: line.get(source) for canon, source in _LINE_FIELD_MAPS[entity].items()}
            )
            if record.get("line_no") is None:
                continue  # header without lines data — not a line row
            record["company_id"] = company_id
            yield record

    def source_key_inventory(self, entity: str) -> set[str]:
        """Key scan across every configured company — the anti-join's source side.

        Flat entities scan with a key-only ``$select`` per company; header-driven
        line entities derive keys from the full line stream (line keys live
        inside expanded header rows). Natural ids are company-prefixed so two
        companies' shared document numbers never collapse into one key.
        """
        self._require_config()
        mapping = self._entity_map(entity)
        key_fields = self.natural_key_fields[entity]
        keys: set[str] = set()
        if mapping.lines_field is not None:
            for record in self._iter_records(entity, ExtractionMode.BACKFILL, None):
                keys.add(natural_id_for(key_fields, record))
            return keys
        field_map = _FIELD_MAPS[entity]
        select = self._key_select(entity, mapping)
        for company_id in self._companies():
            for row in self._paged(mapping, entity, None, None, company_id, select=select):
                record = {c: row.get(s) for c, s in field_map.items() if c in key_fields}
                record["company_id"] = company_id
                keys.add(natural_id_for(key_fields, record))
        return keys

    def current_watermark(
        self, entity: str, mode: ExtractionMode, watermark_before: str | None
    ) -> str | None:
        mapping = self._entity_map(entity)
        if mode is not ExtractionMode.INCREMENTAL or mapping.incremental_field is None:
            return watermark_before
        # Cross-company checkpoint: the max lastModifiedDateTime observed across
        # every configured company's pages — never a per-company max (spec
        # pitfall 3). The base persists it only after the run fully succeeds.
        return self._max_incremental_seen.get(entity, watermark_before)

    # ------------------------------------------------------------------
    # OData plumbing (paging, backoff, auth, quarantine)
    # ------------------------------------------------------------------

    def _paged(
        self,
        mapping: BcEntity,
        entity: str,
        mode: ExtractionMode | None,
        watermark: str | None,
        company_id: str,
        select: str | None = None,
    ) -> Iterator[dict[str, object]]:
        """Yield one company's rows across @odata.nextLink continuation pages."""
        params: dict[str, str] = {
            "$top": str(self._current_page_size()),
            "$select": select or ",".join(mapping.select),
            "Data-Access-Intent": "ReadOnly",
        }
        if mapping.lines_field:
            params["$expand"] = mapping.lines_field
        if (
            mode is ExtractionMode.INCREMENTAL
            and mapping.incremental_field is not None
            and watermark
        ):
            params["$filter"] = f"{mapping.incremental_field} gt {watermark}"
        url = self._entity_url(mapping.entity_set, company_id)
        while url:
            payload = self._get_json(entity, url, params)
            rows: list[dict[str, object]] = payload["value"]  # shape-validated in _get_json
            self._observe_incremental(entity, mapping, rows)
            yield from rows
            next_link = payload.get("@odata.nextLink")
            url = next_link if isinstance(next_link, str) else None
            params = {}  # nextLink carries the query with it

    def _observe_incremental(
        self, entity: str, mapping: BcEntity, rows: list[dict[str, object]]
    ) -> None:
        """Track the max incremental value seen — across ALL pages and companies."""
        field = mapping.incremental_field
        if field is None:
            return
        for row in rows:
            observed = row.get(field)
            if observed is None:
                continue
            text = str(observed)
            current = self._max_incremental_seen.get(entity)
            if current is None or _bc_timestamp(text) > _bc_timestamp(current):
                self._max_incremental_seen[entity] = text

    def _get_json(self, entity: str, url: str, params: dict[str, str]) -> dict[str, object]:
        """GET with bearer auth, documented-limits backoff, and page validation.

        429/503 back off honoring ``Retry-After``; a 504 on a self-constructed
        URL halves ``$top`` first (spec §5: split the request into smaller
        ones). A 200 whose body is not ``{"value": [object, ...]}`` is
        quarantined with a machine-readable reason and fails the run — never a
        silent drop, never a partial promote.
        """
        delay = self.BACKOFF_SECONDS
        for attempt in range(1, self.MAX_RETRIES + 1):
            response = self._client().get(url, params=params, headers=self._headers())
            if response.status_code in (429, 503, 504):
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else delay
                if attempt == self.MAX_RETRIES:
                    raise ConnectorError(
                        f"Business Central returned {response.status_code} on {url} "
                        f"after {self.MAX_RETRIES} backoff attempts"
                    )
                if response.status_code == 504 and params:
                    # Our own URL: shrink the window. nextLink pages carry the
                    # server's query — backoff is all that applies there.
                    self._shrink_page_size()
                    params = {**params, "$top": str(self._current_page_size())}
                time.sleep(delay)
                delay *= 2
                continue
            try:
                response.raise_for_status()
            except httpx.HTTPError as exc:
                raise ConnectorError(f"Business Central call failed: {exc}") from exc
            try:
                payload: object = response.json()
            except ValueError as exc:
                self._quarantine_page(entity, url, response.content, RC_UNPARSEABLE_JSON, str(exc))
                raise ConnectorError(
                    f"malformed Business Central response on {url}: body is not JSON"
                ) from exc
            self._validate_page(entity, url, payload)
            return payload  # type: ignore[no-any-return]
        raise ConnectorError("unreachable: retry loop must return or raise")  # pragma: no cover

    def _validate_page(self, entity: str, url: str, payload: object) -> None:
        rows = payload.get("value") if isinstance(payload, dict) else None
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            self._quarantine_page(
                entity,
                url,
                json.dumps(payload, default=str).encode("utf-8"),
                RC_MALFORMED_PAGE,
                "'value' must be a list of objects",
            )
            raise ConnectorError(
                f"malformed Business Central page for {entity} at {url}: "
                "'value' must be a list of objects"
            )

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

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token()}", "Accept": "application/json"}

    def _token(self) -> str:
        """Entra ID client-credentials token, cached until near expiry."""
        if self._cached_token and self._token_expiry > time.time() + 60:
            return self._cached_token
        settings = self.source.settings
        response = self._client().post(
            TOKEN_URL_TEMPLATE.format(tenant_id=settings["tenant_id"]),
            data={
                "grant_type": "client_credentials",
                "client_id": settings["client_id"],
                "client_secret": settings["client_secret"],
                "scope": f"{BC_API_BASE}/.default",
            },
            timeout=60.0,
        )
        try:
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise ConnectorError(f"Entra token request failed: {exc}") from exc
        payload: dict[str, Any] = response.json()
        self._cached_token = payload["access_token"]
        self._token_expiry = time.time() + float(payload.get("expires_in", 3600))
        return self._cached_token

    def _entity_url(self, entity_set: str, company_id: str) -> str:
        settings = self.source.settings
        api_version = settings.get("api_version") or "v2.0"
        return (
            f"{BC_API_BASE}/{settings['environment']}/api/{api_version}"
            f"/companies({company_id})/{entity_set}"
        )

    def _companies(self) -> list[str]:
        """Configured company ids, in order, deduplicated — spec pitfall 3's loop."""
        companies: list[str] = []
        for token in self.source.settings.get("companies", "").split(","):
            company_id = token.strip()
            if company_id and company_id not in companies:
                companies.append(company_id)
        return companies

    def _require_config(self) -> None:
        """Fail closed BEFORE any network attempt — inert without credentials."""
        problems = self.validate_config()
        if problems:
            raise ConnectorNotConfigured(
                f"source {self.source.source_id} ({self.erp_id}) is not configurable: "
                + "; ".join(problems)
            )

    def _current_page_size(self) -> int:
        return self._page_size or self.PAGE_SIZE

    def _shrink_page_size(self) -> None:
        """Spec §5 504 handling: split the request into smaller ones."""
        self._page_size = max(self._current_page_size() // 2, self.MIN_PAGE_SIZE)

    def _key_select(self, entity: str, mapping: BcEntity) -> str:
        """$select limited to the natural-key source fields (plus line keys)."""
        key_sources = set()
        field_map = _FIELD_MAPS[entity]
        line_map = _LINE_FIELD_MAPS.get(entity, {})
        for canon in self.natural_key_fields[entity]:
            source = line_map.get(canon) or field_map.get(canon)
            if source:
                key_sources.add(source)
        return ",".join(sorted(key_sources)) or ",".join(mapping.select)

    def _entity_map(self, entity: str) -> BcEntity:
        mapping = _ENTITY_MAPS.get(entity)
        if mapping is None:
            raise ConnectorError(
                f"entity '{entity}' has no Business Central mapping yet; known: "
                f"{', '.join(sorted(_ENTITY_MAPS))}"
            )
        return mapping

    def _client(self) -> httpx.Client:
        if self._http_client is None:
            self._http_client = httpx.Client(timeout=60.0)
        return self._http_client
