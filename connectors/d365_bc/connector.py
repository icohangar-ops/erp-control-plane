"""d365_bc — Dynamics 365 Business Central extraction (SKELETON).

Extraction notes (ERP landscape research, art_NKUrngnG):
- D365 BC exposes API v2.0 (Microsoft Entra ID OAuth2, per-company endpoints)
  covering salesOrders, salesInvoices, purchaseOrders, items, customers,
  vendors, and generalLedgerEntries.
- Backfill for multi-year history: restore a BACPAC export of the tenant DB
  into a scratch SQL DB and bulk-read, then switch the connector to API v2
  for the delta. Never point production extraction at the tenant primary.
- Company id is part of every API URL; multi-company tenants need one
  extraction stream per company (or loop with company_id).

TODO(per-tenant): register an Entra app with Dynamics CRM/BC permissions,
consent the tenant, choose company(ies), and confirm delegation vs app-only
auth with the customer's Microsoft admin.
"""

from typing import ClassVar

from connectors.stubs.skeleton import SkeletonConnector


class DynamicsBcConnector(SkeletonConnector):
    erp_id = "d365_bc"
    extraction_notes = (
        "Dynamics 365 Business Central API v2.0 (OAuth2) with BACPAC backfill "
        "for history. Skeleton pending Entra app registration; per-company "
        "endpoint loop needed for multi-company tenants."
    )
    natural_key_fields: ClassVar[dict[str, tuple[str, ...]]] = {
        "items": ("item_no",),
        "customers": ("customer_no",),
        "vendors": ("vendor_no",),
        "sales_order_lines": ("order_no", "line_no"),
        "invoice_lines": ("invoice_no", "line_no"),
        "purchase_order_lines": ("po_no", "line_no"),
        "gl_entries": ("journal_no", "line_no"),
    }
    extraction_surface: ClassVar[dict[str, str]] = {
        "items": "API v2.0: GET /companies({id})/items (paged JSON)",
        "customers": "API v2.0: GET /companies({id})/customers",
        "vendors": "API v2.0: GET /companies({id})/vendors",
        "sales_order_lines": "API v2.0: salesOrders + expand lines (or salesOrderLines)",
        "invoice_lines": "API v2.0: salesInvoices + lines",
        "purchase_order_lines": "API v2.0: purchaseOrders + lines",
        "gl_entries": "API v2.0: generalLedgerEntries (multi-company loop)",
    }
    incremental_keys: ClassVar[dict[str, str]] = {
        "items": "lastModifiedDateTime",
        "customers": "lastModifiedDateTime",
        "vendors": "lastModifiedDateTime",
        "sales_order_lines": "orderDate + lastModifiedDateTime",
        "invoice_lines": "invoiceDate + lastModifiedDateTime",
        "purchase_order_lines": "expectedReceiptDate / lastModifiedDateTime",
        "gl_entries": "postingDate",
    }
    required_settings: ClassVar[tuple[str, ...]] = (
        "tenant_id",
        "client_id",
        "client_secret",
        "environment",
        "company_id",
    )
