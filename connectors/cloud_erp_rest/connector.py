"""cloud_erp_rest — cloud API-led ERP extraction (Plex / Dynamics 365).

STATUS: implemented per the dlt rest_api verified-source approach — per-tenant
auth resolution (API-key header or OAuth2 client credentials), explicit
pagination strategies (page-number paging and OData ``@odata.nextLink``
continuation), modified-timestamp watermarks, and full-key anti-join delete
reconciliation (these APIs expose no delete feeds) — but NOT exercised against
a live tenant: no fabricated API behavior ships as tested. Resource paths and
field maps below are canonical-shaped defaults that must be validated against
the tenant's API metadata at onboarding (they were written without a
per-tenant research artifact); per-tenant overrides ride settings
(``auth_mode``, ``watermark_field``, ``incremental_query``, ``page_size``).
Run ``python -m connectors.cli plan --source cloud_erp_rest_template``
(dry-run) before any live call.

Extraction notes (spec §5/§6; posture per the verified-inventory reconciliation):
- One connector class serves both cloud ERP families behind per-tenant
  settings: ``provider: plex`` (API-key header by default, OAuth2 bearer
  optional) and ``provider: d365`` (OAuth2 client credentials via Microsoft
  Entra ID) — the dlt rest_api declarative auth options.
- Pagination is explicit per profile: page-number paging (``page``/``pageSize``
  query params, stop on a short page) for Plex-style surfaces and
  ``@odata.nextLink`` continuation for OData/Dynamics 365 — the two dlt
  rest_api paginator shapes implemented here.
- Incremental requests filter server-side on a modified-timestamp field
  (per-profile default, settings-overridable); the watermark is observed from
  raw payloads and persisted by the shared machinery.
- These cloud APIs expose no delete feeds: the scheduled
  ``delete_reconciliation`` asset runs the inherited full-key anti-join (§6).
  Key scans use the key-only projection where the profile declares one
  ($select-style); otherwise they page full payloads and derive keys.
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
    ConnectorNotConfigured,
    ExtractionMode,
    ExtractionPlan,
    natural_id_for,
)

TOKEN_URL_TEMPLATE = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

AUTH_MODES = ("api_key", "oauth2")


@dataclass(frozen=True)
class CloudErpProfile:
    """Per-provider extraction profile: auth defaults + explicit pagination.

    Resource paths and field maps (``ENTITY_PATHS``/``ENTITY_MAPS``) are
    canonical-shaped defaults validated against tenant API metadata at
    onboarding; the per-tenant knobs (auth mode, watermark field, incremental
    query template, page size) ride source settings.
    """

    provider: str
    label: str
    #: "next_link" (OData continuation) or "page_number" (page/pageSize params).
    pagination: str
    #: Envelope key holding the record list (None = bare JSON array pages).
    list_root: str | None
    #: Continuation field (next_link profiles), e.g. "@odata.nextLink".
    next_link_field: str | None
    #: Page-number query param (page_number profiles).
    page_param: str | None
    #: Page-size query param ($top for OData, pageSize for Plex-style).
    page_size_param: str | None
    #: Key-only projection param ($select-style), or None (full-payload scans).
    select_param: str | None
    #: Incremental query template: "name={value expression}" with {field}/{value}.
    incremental_param: str
    #: Modified-timestamp field driving watermarks (settings-overridable).
    watermark_field: str
    default_auth_mode: str
    default_api_key_header: str


PROFILES: dict[str, CloudErpProfile] = {
    "plex": CloudErpProfile(
        provider="plex",
        label="Plex Manufacturing Cloud (per-tenant REST)",
        pagination="page_number",
        list_root=None,
        next_link_field=None,
        page_param="page",
        page_size_param="pageSize",
        select_param=None,
        incremental_param="modified_since={value}",
        watermark_field="last_modified",
        default_auth_mode="api_key",
        default_api_key_header="X-API-Key",
    ),
    "d365": CloudErpProfile(
        provider="d365",
        label="Dynamics 365 (per-tenant OData REST)",
        pagination="next_link",
        list_root="value",
        next_link_field="@odata.nextLink",
        page_param=None,
        page_size_param="$top",
        select_param="$select",
        incremental_param="$filter={field} gt {value}",
        watermark_field="ModifiedDateTime",
        default_auth_mode="oauth2",
        default_api_key_header="Authorization",
    ),
}

#: Canonical entity -> per-tenant REST resource path (onboarding-validated).
ENTITY_PATHS: dict[str, dict[str, str]] = {
    "plex": {
        "items": "items",
        "customers": "customers",
        "vendors": "vendors",
        "sales_order_lines": "sales-order-lines",
        "invoice_lines": "invoice-lines",
        "purchase_order_lines": "purchase-order-lines",
    },
    "d365": {
        "items": "data/ReleasedProductsV2",
        "customers": "data/CustomersV3",
        "vendors": "data/VendorsV2",
        "sales_order_lines": "data/SalesOrderLinesV2",
        "invoice_lines": "data/SalesInvoiceLinesV2",
        "purchase_order_lines": "data/PurchaseOrderLinesV2",
    },
}

#: canonical field <- source field per entity, per provider. Illustrative
#: canonical-shaped defaults (written without a per-tenant research artifact):
#: validate against the tenant's API metadata at onboarding before enabling.
ENTITY_MAPS: dict[str, dict[str, dict[str, str]]] = {
    "plex": {
        "items": {
            "item_no": "item_id",
            "description": "item_description",
            "category": "product_class",
            "subcategory": "product_subclass",
            "uom": "uom",
            "unit_cost": "avg_cost",
            "list_price": "list_price",
            "item_status": "item_status",
        },
        "customers": {
            "customer_no": "customer_id",
            "customer_name": "customer_name",
            "customer_class": "customer_class",
            "terms": "terms_code",
            "credit_limit": "credit_limit",
            "address1": "address_line1",
            "city": "city",
            "state": "state",
            "postal_code": "postal_code",
        },
        "vendors": {
            "vendor_no": "vendor_id",
            "vendor_name": "vendor_name",
            "terms": "terms_code",
            "lead_time_days": "lead_time_days",
        },
        "sales_order_lines": {
            "order_no": "order_id",
            "line_no": "line_number",
            "order_date": "order_date",
            "customer_no": "customer_id",
            "branch_code": "facility_id",
            "salesperson_code": "salesperson_id",
            "item_no": "item_id",
            "uom": "uom",
            "ordered_qty": "quantity_ordered",
            "filled_qty": "quantity_shipped",
            "cancelled_qty": "quantity_cancelled",
            "unit_price": "unit_price",
            "unit_cost": "unit_cost",
            "promised_date": "promised_date",
            "shipped_date": "ship_date",
            "order_status": "line_status",
        },
        "invoice_lines": {
            "invoice_no": "invoice_id",
            "line_no": "line_number",
            "invoice_date": "invoice_date",
            "order_no": "order_id",
            "customer_no": "customer_id",
            "branch_code": "facility_id",
            "item_no": "item_id",
            "uom": "uom",
            "invoiced_qty": "quantity_invoiced",
            "unit_price": "unit_price",
            "unit_cost": "unit_cost",
            "freight_amt": "freight_amount",
            "tax_amt": "tax_amount",
        },
        "purchase_order_lines": {
            "po_no": "po_id",
            "line_no": "line_number",
            "po_date": "po_date",
            "vendor_no": "vendor_id",
            "branch_code": "facility_id",
            "item_no": "item_id",
            "uom": "uom",
            "ordered_qty": "quantity_ordered",
            "received_qty": "quantity_received",
            "unit_cost_actual": "unit_cost_actual",
            "unit_cost_standard": "unit_cost_standard",
            "received_date": "received_date",
        },
    },
    "d365": {
        "items": {
            "item_no": "ItemNumber",
            "description": "ItemName",
            "category": "ItemGroupId",
            "subcategory": "ItemSubGroupId",
            "uom": "UnitOfMeasureSymbol",
            "unit_cost": "StandardCost",
            "list_price": "SalesPrice",
            "item_status": "Stopped",
        },
        "customers": {
            "customer_no": "CustomerAccount",
            "customer_name": "OrganizationName",
            "customer_class": "CustomerGroupId",
            "terms": "PaymentTerms",
            "credit_limit": "CreditMax",
            "address1": "AddressLine",
            "city": "City",
            "state": "State",
            "postal_code": "ZipCode",
        },
        "vendors": {
            "vendor_no": "VendorAccountNumber",
            "vendor_name": "OrganizationName",
            "terms": "PaymentTerms",
            "lead_time_days": "PurchaseLeadTime",
        },
        "sales_order_lines": {
            "order_no": "SalesId",
            "line_no": "LineNumber",
            "order_date": "OrderDate",
            "customer_no": "OrderingCustomerAccountNumber",
            "branch_code": "InventLocationId",
            "salesperson_code": "SalesResponsibleId",
            "item_no": "ItemNumber",
            "uom": "SalesUnitSymbol",
            "ordered_qty": "OrderedSalesQuantity",
            "filled_qty": "ShippedSalesQuantity",
            "cancelled_qty": "CanceledSalesQuantity",
            "unit_price": "SalesPrice",
            "unit_cost": "CostPrice",
            "promised_date": "RequestedShipDate",
            "shipped_date": "ConfirmedShipDate",
            "order_status": "LineStatus",
        },
        "invoice_lines": {
            "invoice_no": "InvoiceId",
            "line_no": "LineNumber",
            "invoice_date": "InvoiceDate",
            "order_no": "SalesId",
            "customer_no": "InvoiceAccount",
            "branch_code": "InventLocationId",
            "item_no": "ItemNumber",
            "uom": "SalesUnitSymbol",
            "invoiced_qty": "InvoicedSalesQuantity",
            "unit_price": "SalesPrice",
            "unit_cost": "CostPrice",
            "freight_amt": "FreightCost",
            "tax_amt": "TaxAmount",
        },
        "purchase_order_lines": {
            "po_no": "PurchaseOrderNumber",
            "line_no": "LineNumber",
            "po_date": "OrderDate",
            "vendor_no": "OrderingVendorAccountNumber",
            "branch_code": "InventLocationId",
            "item_no": "ItemNumber",
            "uom": "PurchaseUnitSymbol",
            "ordered_qty": "OrderedPurchQuantity",
            "received_qty": "ReceivedPurchQuantity",
            "unit_cost_actual": "PurchPrice",
            "unit_cost_standard": "StandardCost",
            "received_date": "ConfirmedDlvDate",
        },
    },
}


class CloudErpRestConnector(BaseConnector):
    """Cloud ERP REST adapter (Plex / Dynamics 365) — per-tenant profiles.

    The extraction machinery (paging, backoff, watermark mirroring, key-only
    scans) is shared and fixture-tested against recorded MockTransport
    responses; what varies per tenant is settings, never code.
    """

    erp_id = "cloud_erp_rest"
    maturity = ConnectorMaturity.IMPLEMENTED  # coded — but see UNEXERCISED note above
    extraction_notes = (
        "Cloud ERP REST via per-tenant profiles: Plex (API-key header or OAuth2 "
        "bearer; page/pageSize paging) and Dynamics 365 (Entra ID OAuth2 client "
        "credentials; @odata.nextLink continuation). Incremental reads filter "
        "server-side on a modified-timestamp field. These APIs expose no delete "
        "feeds — the scheduled full-key anti-join reconciles deletes (spec §6). "
        "Resource paths and field maps validate against tenant API metadata at "
        "onboarding; credentials ride environment variables only."
    )
    natural_key_fields: ClassVar[dict[str, tuple[str, ...]]] = {
        "items": ("item_no",),
        "customers": ("customer_no",),
        "vendors": ("vendor_no",),
        "sales_order_lines": ("order_no", "line_no"),
        "invoice_lines": ("invoice_no", "line_no"),
        "purchase_order_lines": ("po_no", "line_no"),
    }
    required_settings: ClassVar[tuple[str, ...]] = ("provider", "base_url")
    PAGE_SIZE = 500
    PAGE_LIMIT = 10_000
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
        settings = self.source.settings
        provider = (settings.get("provider") or "").strip()
        profile = PROFILES.get(provider)
        if profile is None:
            return [
                f"provider '{provider or '<unset>'}' has no cloud-ERP profile — "
                f"known providers: {', '.join(sorted(PROFILES))}"
            ]
        problems: list[str] = []
        if not (settings.get("base_url") or "").strip():
            problems.append("base_url is required (per-tenant REST endpoint)")
        auth_mode = self._auth_mode(profile)
        if auth_mode not in AUTH_MODES:
            problems.append(f"auth_mode '{auth_mode}' is not one of {', '.join(AUTH_MODES)}")
        elif auth_mode == "api_key":
            if not (settings.get("api_key") or "").strip():
                problems.append("api_key is required when auth_mode=api_key")
        else:
            client_id, client_secret = self._client_credentials()
            if not client_id or not client_secret:
                problems.append("client_id and client_secret are required when auth_mode=oauth2")
            if not (settings.get("token_url") or "").strip() and not (
                provider == "d365" and (settings.get("tenant_id") or "").strip()
            ):
                problems.append("token_url is required for OAuth2 (d365 derives it from tenant_id)")
        return problems

    def describe_extraction(self, entity: str) -> ExtractionPlan:
        provider = (self.source.settings.get("provider") or "").strip()
        profile = PROFILES.get(provider)
        if profile is None:
            return ExtractionPlan(
                entity=entity,
                surface="cloud ERP REST (per-tenant provider unset)",
                incremental_key=None,
                notes=(
                    "set 'provider' to one of "
                    f"{', '.join(sorted(PROFILES))} plus base_url; incremental reads "
                    "then filter server-side on the tenant's modified-timestamp field"
                ),
            )
        field_map = self._field_map(entity, profile)
        paging = f"{profile.pagination} paging"
        if profile.page_size_param:
            paging += f" ({profile.page_size_param}={self._page_size()})"
        return ExtractionPlan(
            entity=entity,
            surface=(
                f"{profile.label}: GET {{base_url}}/{ENTITY_PATHS[profile.provider][entity]} — "
                f"{paging}, {self._auth_mode(profile)} auth, "
                f"{len(field_map)} mapped columns"
            ),
            incremental_key=self._watermark_field(profile),
            notes=self.extraction_notes,
        )

    # ------------------------------------------------------------------
    # Extraction
    # ------------------------------------------------------------------

    def _iter_records(
        self, entity: str, mode: ExtractionMode, watermark: str | None
    ) -> Iterator[dict[str, object]]:
        profile = self._require_profile()
        field_map = self._field_map(entity, profile)
        params: dict[str, str] = {}
        if mode is ExtractionMode.INCREMENTAL and watermark:
            params.update(
                self._incremental_params(profile, self._watermark_field(profile), watermark)
            )
        for record in self._paged(entity, profile, params):
            self._observe_incremental(entity, profile, record)
            yield {canon: record.get(source) for canon, source in field_map.items()}

    def source_key_inventory(self, entity: str) -> set[str]:
        """Full key scan — the anti-join's source side.

        Profiles with a key-only projection ($select-style) scan keys only;
        otherwise full payloads are paged and keys derived from them.
        """
        profile = self._require_profile()
        key_fields = self.natural_key_fields[entity]
        field_map = self._field_map(entity, profile)
        params: dict[str, str] = {}
        if profile.select_param:
            params[profile.select_param] = ",".join(sorted(field_map[c] for c in key_fields))
        keys: set[str] = set()
        for record in self._paged(entity, profile, params):
            keys.add(natural_id_for(key_fields, {c: record.get(field_map[c]) for c in key_fields}))
        return keys

    def current_watermark(
        self, entity: str, mode: ExtractionMode, watermark_before: str | None
    ) -> str | None:
        if mode is not ExtractionMode.INCREMENTAL:
            return watermark_before
        return self._max_incremental_seen.get(entity, watermark_before)

    # ------------------------------------------------------------------
    # Profile plumbing (paging, backoff, auth)
    # ------------------------------------------------------------------

    def _paged(
        self, entity: str, profile: CloudErpProfile, params: dict[str, str]
    ) -> Iterator[dict[str, object]]:
        """Yield payload records across explicitly-paginated pages."""
        if profile.page_size_param and profile.page_size_param not in params:
            params[profile.page_size_param] = str(self._page_size())
        if profile.page_param:
            params.setdefault(profile.page_param, "1")
        url: str | None = self._entity_url(entity, profile)
        page_number = 1
        while url:
            payload = self._get_json(url, params)
            records, next_url = self._unpack_page(payload, profile)
            yield from records
            if profile.next_link_field:
                url = next_url
                params = {}  # continuation links carry their own query
                continue
            page_number += 1
            if len(records) < self._page_size() or page_number > self.PAGE_LIMIT:
                url = None  # short page (or guardrail) — the source is exhausted
            else:
                assert profile.page_param is not None
                params = {**params, profile.page_param: str(page_number)}

    def _unpack_page(
        self, payload: Any, profile: CloudErpProfile
    ) -> tuple[list[dict[str, object]], str | None]:
        if profile.next_link_field is None:
            if not isinstance(payload, list):
                raise ConnectorError(
                    f"provider '{profile.provider}' pages are bare JSON arrays; got "
                    f"{type(payload).__name__} — base_url or profile is wrong"
                )
            return payload, None
        if not isinstance(payload, dict) or not isinstance(payload.get(profile.list_root), list):
            raise ConnectorError(
                f"provider '{profile.provider}' pages carry a '{profile.list_root}' "
                f"envelope; got {type(payload).__name__} — base_url or profile is wrong"
            )
        raw_next = payload.get(profile.next_link_field)
        next_url = raw_next if isinstance(raw_next, str) else None
        return payload[profile.list_root], next_url

    def _observe_incremental(
        self, entity: str, profile: CloudErpProfile, record: dict[str, object]
    ) -> None:
        observed = record.get(self._watermark_field(profile))
        if observed is None:
            return
        text = str(observed)
        current = self._max_incremental_seen.get(entity)
        if current is None or text > current:
            self._max_incremental_seen[entity] = text

    def _incremental_params(
        self, profile: CloudErpProfile, field: str, watermark: str
    ) -> dict[str, str]:
        """Expand the profile's incremental template into query params.

        Templates look like ``$filter={field} gt {value}`` (OData) or
        ``modified_since={value}`` (plain query param): a param name, an
        ``=``, and a value expression over {field}/{value}.
        """
        template = (
            self.source.settings.get("incremental_query", "").strip() or profile.incremental_param
        )
        name, separator, value_expr = template.partition("=")
        if not name or not separator or not value_expr:
            raise ConnectorError(
                f"connector '{self.erp_id}': incremental query template {template!r} must "
                "look like 'name={value expression}' over {field}/{value}"
            )
        return {name: value_expr.format(field=field, value=watermark)}

    def _get_json(self, url: str, params: dict[str, str]) -> Any:
        """GET with profile auth and documented-limits backoff (429/503)."""
        delay = self.BACKOFF_SECONDS
        for attempt in range(1, self.MAX_RETRIES + 1):
            # params={} would strip a continuation URL's own query string (httpx
            # re-encodes the URL); pass params only when there are any.
            if params:
                response = self._client().get(url, params=params, headers=self._headers())
            else:
                response = self._client().get(url, headers=self._headers())
            if response.status_code in (429, 503):
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else delay
                if attempt == self.MAX_RETRIES:
                    raise ConnectorError(
                        f"cloud ERP REST returned {response.status_code} on {url} "
                        f"after {self.MAX_RETRIES} backoff attempts"
                    )
                time.sleep(delay)
                delay *= 2
                continue
            try:
                response.raise_for_status()
            except httpx.HTTPError as exc:
                raise ConnectorError(f"cloud ERP REST call failed: {exc}") from exc
            return response.json()
        raise ConnectorError("unreachable: retry loop must return or raise")  # pragma: no cover

    def _headers(self) -> dict[str, str]:
        profile = self._require_profile()
        auth_mode = self._auth_mode(profile)
        if auth_mode == "api_key":
            api_key = (self.source.settings.get("api_key") or "").strip()
            if not api_key:
                raise ConnectorNotConfigured(
                    f"source {self.source.source_id} auth_mode=api_key but api_key is unset"
                )
            return {
                self.source.settings.get("api_key_header", "").strip()
                or profile.default_api_key_header: api_key,
                "Accept": "application/json",
            }
        return {"Authorization": f"Bearer {self._token()}", "Accept": "application/json"}

    def _token(self) -> str:
        """OAuth2 client-credentials token, cached until near expiry."""
        if self._cached_token and self._token_expiry > time.time() + 60:
            return self._cached_token
        settings = self.source.settings
        token_url = (settings.get("token_url") or "").strip()
        if not token_url:
            if (settings.get("provider") or "").strip() != "d365" or not (
                settings.get("tenant_id") or ""
            ).strip():
                raise ConnectorNotConfigured(
                    f"source {self.source.source_id} needs token_url (or d365 tenant_id)"
                )
            token_url = TOKEN_URL_TEMPLATE.format(tenant_id=settings["tenant_id"])
        client_id, client_secret = self._client_credentials()
        if not client_id or not client_secret:
            raise ConnectorNotConfigured(
                f"source {self.source.source_id} needs client_id and client_secret"
            )
        data: dict[str, str] = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }
        scope = (settings.get("scope") or "").strip()
        if scope:
            data["scope"] = scope
        response = self._client().post(token_url, data=data, timeout=60.0)
        try:
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise ConnectorError(f"OAuth2 token request failed: {exc}") from exc
        payload: dict[str, Any] = response.json()
        self._cached_token = payload["access_token"]
        self._token_expiry = time.time() + float(payload.get("expires_in", 3600))
        return self._cached_token

    # ------------------------------------------------------------------
    # Settings resolution
    # ------------------------------------------------------------------

    def _require_profile(self) -> CloudErpProfile:
        provider = (self.source.settings.get("provider") or "").strip()
        profile = PROFILES.get(provider)
        if profile is None:
            raise ConnectorError(
                f"source {self.source.source_id} has no cloud-ERP profile for provider "
                f"'{provider or '<unset>'}'; known: {', '.join(sorted(PROFILES))}"
            )
        return profile

    def _auth_mode(self, profile: CloudErpProfile) -> str:
        return (self.source.settings.get("auth_mode") or "").strip() or profile.default_auth_mode

    def _client_credentials(self) -> tuple[str, str]:
        settings = self.source.settings
        return (settings.get("client_id") or "").strip(), (
            settings.get("client_secret") or ""
        ).strip()

    def _watermark_field(self, profile: CloudErpProfile) -> str:
        return self.source.settings.get("watermark_field", "").strip() or profile.watermark_field

    def _page_size(self) -> int:
        raw = (self.source.settings.get("page_size") or "").strip()
        if not raw:
            return self.PAGE_SIZE
        try:
            size = int(raw)
        except ValueError as exc:
            raise ConnectorError(
                f"connector '{self.erp_id}': page_size {raw!r} is not an integer"
            ) from exc
        if size <= 0:
            raise ConnectorError(f"connector '{self.erp_id}': page_size must be positive")
        return size

    def _field_map(self, entity: str, profile: CloudErpProfile) -> dict[str, str]:
        provider_maps = ENTITY_MAPS.get(profile.provider, {})
        field_map = provider_maps.get(entity)
        if field_map is None:
            raise ConnectorError(
                f"entity '{entity}' has no {profile.provider} mapping yet; known: "
                f"{', '.join(sorted(provider_maps))}"
            )
        return field_map

    def _entity_url(self, entity: str, profile: CloudErpProfile) -> str:
        base_url = (self.source.settings.get("base_url") or "").strip().rstrip("/")
        path = ENTITY_PATHS.get(profile.provider, {}).get(entity)
        if path is None:
            raise ConnectorError(
                f"entity '{entity}' has no {profile.provider} resource path yet; known: "
                f"{', '.join(sorted(ENTITY_PATHS.get(profile.provider, {})))}"
            )
        return f"{base_url}/{path}"

    def _client(self) -> httpx.Client:
        if self._http_client is None:
            self._http_client = httpx.Client(timeout=60.0)
        return self._http_client
