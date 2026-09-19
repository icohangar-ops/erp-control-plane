"""dmsi_agility — DMSi Agility REST extraction (SKELETON).

Extraction notes (ERP landscape research, art_NKUrngnG):
- DMSi Agility is a cloud-hosted dealer ERP for building-materials and
  lumber yards; the documented integration surface is its REST API.
- Core entities live behind the point-of-sale / order-management APIs:
  customers, items/products, quotes-orders, invoices, POs, inventory.

TODO(per-tenant): obtain an Agility sandbox tenant + API credentials, then
inventory the exact endpoints (orders, ar invoices, ap, inventory by branch)
and rate limits before implementing _iter_records.
"""

from typing import ClassVar

from connectors.stubs.skeleton import SkeletonConnector


class DmsiAgilityConnector(SkeletonConnector):
    erp_id = "dmsi_agility"
    extraction_notes = (
        "DMSi Agility REST API extraction. Skeleton pending tenant sandbox "
        "access and endpoint inventory; confirm pagination style and field "
        "names against the tenant's Agility version."
    )
    natural_key_fields: ClassVar[dict[str, tuple[str, ...]]] = {
        "items": ("item_no",),
        "customers": ("customer_no",),
        "vendors": ("vendor_no",),
        "sales_order_lines": ("order_no", "line_no"),
        "invoice_lines": ("invoice_no", "line_no"),
        "purchase_order_lines": ("po_no", "line_no"),
        "inventory_snapshots": ("snapshot_date", "branch_code", "item_no"),
    }
    extraction_surface: ClassVar[dict[str, str]] = {
        "items": "REST API: product/item catalog endpoints (paged JSON)",
        "customers": "REST API: customer/accounts endpoints",
        "vendors": "REST API: vendor endpoints",
        "sales_order_lines": "REST API: order header + line endpoints (per order)",
        "invoice_lines": "REST API: AR invoice line endpoints",
        "purchase_order_lines": "REST API: PO endpoints",
        "inventory_snapshots": "REST API: inventory-by-branch endpoints (daily snapshot job)",
    }
    incremental_keys: ClassVar[dict[str, str]] = {
        "items": "last-modified timestamp",
        "customers": "last-modified timestamp",
        "vendors": "last-modified timestamp",
        "sales_order_lines": "order last-modified or status-change timestamp",
        "invoice_lines": "invoice date",
        "purchase_order_lines": "PO last-modified timestamp",
        "inventory_snapshots": "snapshot date",
    }
    required_settings: ClassVar[tuple[str, ...]] = ("base_url", "client_id", "client_secret")
