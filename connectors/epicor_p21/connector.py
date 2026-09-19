"""epicor_p21 — Epicor Prophet 21 extraction (SKELETON).

Extraction notes (ERP landscape research, art_NKUrngnG):
- P21 ships SQL Server backends exposed through documented views
  (SMO/CSV-style stage views) and an OData REST API for cloud deployments.
- Common sources: ORDER_HEADER/ORDER_DETAIL, INVOICE_HEADER/INVOICE_DETAIL,
  INVENTORY_LOCATION, PRODUCT, CUSTOMER, SUPPLIER.
- Deployment mode decides the surface: direct read-only SQL against a replica
  for on-prem; OData for P21 Cloud.

TODO(per-tenant): determine on-prem vs cloud, request a read-only SQL login
(or OData account), and map the tenant's custom UDFs/fields.
"""

from typing import ClassVar

from connectors.stubs.skeleton import SkeletonConnector


class EpicorP21Connector(SkeletonConnector):
    erp_id = "epicor_p21"
    extraction_notes = (
        "Epicor Prophet 21 via SQL Server views (on-prem) or OData (cloud). "
        "Skeleton pending the customer's deployment architecture; prefer a "
        "read-only replica to avoid touching the transactional primary."
    )
    natural_key_fields: ClassVar[dict[str, tuple[str, ...]]] = {
        "items": ("item_id",),
        "customers": ("customer_id",),
        "vendors": ("supplier_id",),
        "sales_order_lines": ("order_no", "line_no"),
        "invoice_lines": ("invoice_no", "line_no"),
        "purchase_order_lines": ("po_no", "line_no"),
        "inventory_snapshots": ("snapshot_date", "location_id", "item_id"),
        "gl_entries": ("je_no", "line_no"),
    }
    extraction_surface: ClassVar[dict[str, str]] = {
        "items": "SQL Server view PRODUCT (read-only) or OData products entity",
        "customers": "SQL Server view CUSTOMER or OData customers entity",
        "vendors": "SQL Server view SUPPLIER or OData suppliers entity",
        "sales_order_lines": "SQL Server ORDER_HEADER+ORDER_DETAIL or OData orders",
        "invoice_lines": "SQL Server INVOICE_HEADER+INVOICE_DETAIL or OData invoices",
        "purchase_order_lines": "SQL Server PO views or OData purchase-orders",
        "inventory_snapshots": "SQL Server INVENTORY_LOCATION (snapshot job) or OData inventory",
        "gl_entries": "SQL Server GL transaction views or OData gl-journal-lines",
    }
    incremental_keys: ClassVar[dict[str, str]] = {
        "items": "last_maint_ts",
        "customers": "last_maint_ts",
        "vendors": "last_maint_ts",
        "sales_order_lines": "order_date",
        "invoice_lines": "invoice_date",
        "purchase_order_lines": "po_date",
        "inventory_snapshots": "snapshot date",
        "gl_entries": "gl entry date",
    }
    required_settings: ClassVar[tuple[str, ...]] = ("sql_dsn", "odata_base_url")
