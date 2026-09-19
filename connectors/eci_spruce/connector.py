"""eci_spruce — ECI Spruce / RockSolid MAX extraction (SKELETON).

Extraction notes (ERP landscape research, art_NKUrngnG):
- Two products in scope: ECI Spruce (web/cloud dealer ERP with CSV export
  routines) and RockSolid MAX (SOAP web services API).
- Spruce: nightly CSV exports per entity are the pragmatic first surface —
  reuse the csv_sftp validation pattern against the Spruce export layout.
- RockSolid MAX: SOAP endpoints for customers, items, orders, invoices,
  inventory; XML payload mapping.

TODO(per-tenant): identify which ECI product the dealer runs, request API
access (RockSolid) or scheduled exports (Spruce), and map the WSDL/CSV
layouts; a RockSolid implementation wraps a SOAP client here.
"""

from typing import ClassVar

from connectors.stubs.skeleton import SkeletonConnector


class EciSpruceConnector(SkeletonConnector):
    erp_id = "eci_spruce"
    extraction_notes = (
        "ECI Spruce CSV exports or RockSolid MAX SOAP services. Skeleton "
        "pending product identification and vendor API access; Spruce CSV "
        "fallback should reuse the csv_sftp gates (manifest/checksum/quarantine)."
    )
    natural_key_fields: ClassVar[dict[str, tuple[str, ...]]] = {
        "items": ("item_id",),
        "customers": ("customer_id",),
        "vendors": ("vendor_id",),
        "sales_order_lines": ("order_no", "line_no"),
        "invoice_lines": ("invoice_no", "line_no"),
        "inventory_snapshots": ("snapshot_date", "branch", "item_id"),
    }
    extraction_surface: ClassVar[dict[str, str]] = {
        "items": "Spruce: scheduled CSV item export; RockSolid: SOAP item service",
        "customers": "Spruce: scheduled CSV customer export; RockSolid: SOAP customer service",
        "vendors": "Spruce: CSV vendor export; RockSolid: SOAP vendor service",
        "sales_order_lines": "Spruce: CSV order export; RockSolid: SOAP order service",
        "invoice_lines": "Spruce: CSV invoice export; RockSolid: SOAP invoice service",
        "inventory_snapshots": "Spruce: CSV inventory export; RockSolid: SOAP inventory service",
    }
    incremental_keys: ClassVar[dict[str, str]] = {
        "items": "export batch date",
        "customers": "export batch date",
        "vendors": "export batch date",
        "sales_order_lines": "order date within batch",
        "invoice_lines": "invoice date within batch",
        "inventory_snapshots": "snapshot date",
    }
    required_settings: ClassVar[tuple[str, ...]] = ("product", "wsdl_url", "csv_drop_root")
