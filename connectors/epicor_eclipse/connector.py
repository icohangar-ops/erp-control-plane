"""epicor_eclipse — Epicor Eclipse REST extraction (SKELETON).

Extraction notes (ERP landscape research, art_NKUrngnG):
- Eclipse is REST-only (no direct SQL access contract) on an InterSystems
  Caché backbone; endpoints follow the Eclipse Business/Browse API style.
- Ordering/inventory data is exposed through browse-style endpoints;
  reporting often pages by internal record id.

TODO(per-tenant): provision an API user with least-privilege roles, confirm
API version/endpoint surface (product, customer, purchase-order, sales-order
browses), and establish rate-limit budget with the tenant admin.
"""

from typing import ClassVar

from connectors.stubs.skeleton import SkeletonConnector


class EpicorEclipseConnector(SkeletonConnector):
    erp_id = "epicor_eclipse"
    extraction_notes = (
        "Epicor Eclipse REST-only API over InterSystems Caché. Skeleton "
        "pending API user provisioning; browse endpoints page by internal id "
        "and need mapping to document numbers at onboarding."
    )
    natural_key_fields: ClassVar[dict[str, tuple[str, ...]]] = {
        "items": ("product_code",),
        "customers": ("customer_id",),
        "sales_order_lines": ("order_no", "line_no"),
        "invoice_lines": ("invoice_no", "line_no"),
        "purchase_order_lines": ("po_no", "line_no"),
        "inventory_snapshots": ("snapshot_date", "branch", "product_code"),
    }
    extraction_surface: ClassVar[dict[str, str]] = {
        "items": "REST: product browse endpoints (Caché-backed JSON)",
        "customers": "REST: customer endpoints",
        "sales_order_lines": "REST: sales-order browse + lines",
        "invoice_lines": "REST: invoice browse + lines",
        "purchase_order_lines": "REST: PO browse + lines",
        "inventory_snapshots": "REST: inventory/stock browse by branch (snapshot job)",
    }
    incremental_keys: ClassVar[dict[str, str]] = {
        "items": "record id / last-change marker",
        "customers": "record id / last-change marker",
        "sales_order_lines": "order date",
        "invoice_lines": "invoice date",
        "purchase_order_lines": "PO date",
        "inventory_snapshots": "snapshot date",
    }
    required_settings: ClassVar[tuple[str, ...]] = ("base_url", "username", "password")
