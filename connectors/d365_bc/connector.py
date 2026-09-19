"""d365_bc — Dynamics 365 Business Central extraction (API v2.0 / OData v4).

STATUS: implemented against the documented API v2.0 surface (OAuth2 client
credentials, server-driven OData paging via ``@odata.nextLink`` with ``$top``
set explicitly, ``Data-Access-Intent=ReadOnly`` to keep reads off the tenant
primary, HTTP 429/503 backoff per the documented service limits) but NOT
exercised against a live tenant — per the locked decisions, no fabricated API
behavior ships as tested. Validate field maps against the tenant's
``$metadata`` and run ``python -m connectors.cli plan --source <id>`` (dry-run)
before any live call.

Extraction notes (ERP landscape research, art_NKUrngnG; posture art_7DIRx9Nu):
- D365 BC exposes API v2.0 (Microsoft Entra ID OAuth2, per-company endpoints)
  covering salesOrders, salesInvoices, purchaseOrders, items, customers,
  vendors, and generalLedgerEntries.
- Backfill for multi-year history: restore a BACPAC export of the tenant DB
  into a scratch SQL DB and bulk-read, then switch the connector to API v2
  for the delta. Never point production extraction at the tenant primary.
- Company id is part of every API URL; multi-company tenants need one
  extraction stream per company (or loop with company_id).
- Line entities are read header-driven: page ``salesOrders``/``salesInvoices``/
  ``purchaseOrders`` (incremental on ``lastModifiedDateTime``) with the lines
  expanded, then flatten each lines array into canonical staging rows. Expanded
  arrays ride along with the header page — no nested paging.
- Documented limits: default page size 20,000 per page for $top; this pack
  defaults far lower (see PAGE_SIZE) and honors Retry-After on 429/503.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, ClassVar

import httpx

from connectors.base import (
    BaseConnector,
    ConnectorError,
    ConnectorMaturity,
    ExtractionMode,
    ExtractionPlan,
    natural_id_for,
)

TOKEN_URL_TEMPLATE = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
BC_API_BASE = "https://api.businesscentral.dynamics.com/v2.0"


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


class DynamicsBcConnector(BaseConnector):
    """Business Central API v2.0 adapter. Credential-gated; dry-runs need no network."""

    erp_id = "d365_bc"
    maturity = ConnectorMaturity.IMPLEMENTED  # coded — but see UNEXERCISED note above
    extraction_notes = (
        "API v2.0 / OData v4 with $top + @odata.nextLink continuation paging, "
        "Data-Access-Intent=ReadOnly, and HTTP 429 backoff per the documented "
        "limits (spec §5). Not yet exercised against a live tenant; validate "
        "field maps against $metadata and confirm Entra app registration and "
        "company scoping at onboarding."
    )
    natural_key_fields: ClassVar[dict[str, tuple[str, ...]]] = {
        "items": ("item_no",),
        "customers": ("customer_no",),
        "vendors": ("vendor_no",),
        "sales_order_lines": ("order_no", "line_no"),
        "invoice_lines": ("invoice_no", "line_no"),
        "purchase_order_lines": ("po_no", "line_no"),
        "gl_entries": ("journal_no",),
    }
    required_settings: ClassVar[tuple[str, ...]] = (
        "tenant_id",
        "client_id",
        "client_secret",
        "environment",
        "company_id",
    )
    PAGE_SIZE = 1000  # documented $top cap is 20,000 — stay conservative
    MAX_RETRIES = 5
    BACKOFF_SECONDS = 2.0

    def __init__(self, source, store, config):
        super().__init__(source, store, config)
        self._http_client: httpx.Client | None = None
        self._cached_token: str | None = None
        self._token_expiry = 0.0
        self._max_incremental_seen: dict[str, str] = {}

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
        return []

    def describe_extraction(self, entity: str) -> ExtractionPlan:
        mapping = self._entity_map(entity)
        surface = f"API v2.0: GET /companies({{id}})/{mapping.entity_set}"
        if mapping.lines_field:
            surface += f"?$expand={mapping.lines_field}"
        return ExtractionPlan(
            entity=entity,
            surface=surface,
            incremental_key=mapping.incremental_field,
            notes=self.extraction_notes,
        )

    # ------------------------------------------------------------------
    # Extraction
    # ------------------------------------------------------------------

    def _iter_records(
        self, entity: str, mode: ExtractionMode, watermark: str | None
    ) -> Iterator[dict[str, object]]:
        mapping = self._entity_map(entity)
        field_map = _FIELD_MAPS[entity]
        for row in self._paged(mapping.entity_set, entity, mapping, mode, watermark):
            if mapping.lines_field is None:
                yield {canon: row.get(source) for canon, source in field_map.items()}
                continue
            header_context = {
                canon: row.get(source)
                for canon, source in field_map.items()
                if canon not in _LINE_FIELD_MAPS[entity]
            }
            for line in row.get(mapping.lines_field) or []:
                line_fields = _LINE_FIELD_MAPS[entity]
                record: dict[str, object] = dict(header_context)
                record.update({canon: line.get(source) for canon, source in line_fields.items()})
                if record.get("line_no") is None:
                    continue  # header without lines data — not a line row
                yield record

    def source_key_inventory(self, entity: str) -> set[str]:
        """Key scan of the entity set — the anti-join's source side.

        Flat entities scan with a key-only ``$select``; header-driven line
        entities derive keys from the full line stream (line keys live inside
        expanded header rows).
        """
        mapping = self._entity_map(entity)
        key_fields = self.natural_key_fields[entity]
        if mapping.lines_field is not None:
            return {
                natural_id_for(key_fields, record)
                for record in self._iter_records(entity, ExtractionMode.BACKFILL, None)
            }
        field_map = _FIELD_MAPS[entity]
        select = self._key_select(entity, mapping)
        keys: set[str] = set()
        for row in self._paged(mapping.entity_set, entity, mapping, None, None, select=select):
            keys.add(
                natural_id_for(
                    key_fields,
                    {c: row.get(s) for c, s in field_map.items() if c in key_fields},
                )
            )
        return keys

    def current_watermark(
        self, entity: str, mode: ExtractionMode, watermark_before: str | None
    ) -> str | None:
        mapping = self._entity_map(entity)
        if mode is not ExtractionMode.INCREMENTAL or mapping.incremental_field is None:
            return watermark_before
        return self._max_incremental_seen.get(entity, watermark_before)

    # ------------------------------------------------------------------
    # OData plumbing (paging, backoff, auth)
    # ------------------------------------------------------------------

    def _paged(self, entity_set, entity, mapping, mode, watermark, select=None):
        """Yield rows across @odata.nextLink continuation pages."""
        params: dict[str, str] = {
            "$top": str(self.PAGE_SIZE),
            "Data-Access-Intent": "ReadOnly",
        }
        params["$select"] = select or ",".join(mapping.select)
        if mapping.lines_field:
            params["$expand"] = mapping.lines_field
        if (
            mode is ExtractionMode.INCREMENTAL
            and mapping.incremental_field is not None
            and watermark
        ):
            params["$filter"] = f"{mapping.incremental_field} gt {watermark}"
        url = self._entity_url(entity_set)
        while url:
            payload = self._get_json(url, params)
            self._observe_incremental(entity, mapping, payload.get("value", []))
            yield from payload.get("value", [])
            next_link = payload.get("@odata.nextLink")
            url = next_link if isinstance(next_link, str) else None
            params = {}  # nextLink carries the query with it

    def _observe_incremental(
        self, entity: str, mapping: BcEntity, rows: list[dict[str, object]]
    ) -> None:
        field = mapping.incremental_field
        if field is None:
            return
        for row in rows:
            observed = row.get(field)
            if observed is None:
                continue
            text = str(observed)
            current = self._max_incremental_seen.get(entity)
            if current is None or text > current:
                self._max_incremental_seen[entity] = text

    def _get_json(self, url: str, params: dict[str, str]) -> dict[str, object]:
        """GET with bearer auth and documented-limits backoff (429/503)."""
        delay = self.BACKOFF_SECONDS
        for attempt in range(1, self.MAX_RETRIES + 1):
            response = self._client().get(url, params=params, headers=self._headers())
            if response.status_code in (429, 503):
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else delay
                if attempt == self.MAX_RETRIES:
                    raise ConnectorError(
                        f"Business Central returned {response.status_code} on {url} "
                        f"after {self.MAX_RETRIES} backoff attempts"
                    )
                time.sleep(delay)
                delay *= 2
                continue
            try:
                response.raise_for_status()
            except httpx.HTTPError as exc:
                raise ConnectorError(f"Business Central call failed: {exc}") from exc
            data: dict[str, object] = response.json()
            return data
        raise ConnectorError("unreachable: retry loop must return or raise")  # pragma: no cover

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

    def _entity_url(self, entity_set: str) -> str:
        settings = self.source.settings
        return (
            f"{BC_API_BASE}/{settings['environment']}/api/v2.0/"
            f"companies({settings['company_id']})/{entity_set}"
        )

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
