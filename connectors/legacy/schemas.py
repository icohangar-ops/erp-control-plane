"""Canonical staging entity schemas shared by the legacy SQL connector pack.

Column names and kinds mirror the approved CSV/SFTP schema registry
(``connectors/csv_sftp/schemas.yml``) so an entity lands in staging with the
same shape regardless of which source class produced it — the canonical Kimball
model (dbt staging) must not care whether ``invoice_lines`` came from Informix,
Business Central, or a dealer's CSV drop. A parity test in
``tests/test_legacy_sql_connectors.py`` keeps the two registries in lockstep.

Value kinds: ``string | integer | decimal | date`` (the CSV registry's kinds).
"""

from __future__ import annotations

import pyarrow as pa

#: (column, kind) per canonical entity — kind ∈ string|integer|decimal|date.
CANONICAL_ENTITY_COLUMNS: dict[str, tuple[tuple[str, str], ...]] = {
    "items": (
        ("item_no", "string"),
        ("description", "string"),
        ("category", "string"),
        ("subcategory", "string"),
        ("uom", "string"),
        ("unit_cost", "decimal"),
        ("list_price", "decimal"),
        ("item_status", "string"),
    ),
    "salespeople": (
        ("salesperson_code", "string"),
        ("salesperson_name", "string"),
        ("home_branch", "string"),
    ),
    "customers": (
        ("customer_no", "string"),
        ("customer_name", "string"),
        ("customer_class", "string"),
        ("terms", "string"),
        ("credit_limit", "decimal"),
        ("address1", "string"),
        ("city", "string"),
        ("state", "string"),
        ("postal_code", "string"),
    ),
    "vendors": (
        ("vendor_no", "string"),
        ("vendor_name", "string"),
        ("terms", "string"),
        ("lead_time_days", "integer"),
    ),
    "sales_order_lines": (
        ("order_no", "string"),
        ("line_no", "integer"),
        ("order_date", "date"),
        ("customer_no", "string"),
        ("branch_code", "string"),
        ("salesperson_code", "string"),
        ("item_no", "string"),
        ("uom", "string"),
        ("ordered_qty", "integer"),
        ("filled_qty", "integer"),
        ("cancelled_qty", "integer"),
        ("unit_price", "decimal"),
        ("unit_cost", "decimal"),
        ("promised_date", "date"),
        ("shipped_date", "date"),
        ("order_status", "string"),
    ),
    "purchase_order_lines": (
        ("po_no", "string"),
        ("line_no", "integer"),
        ("po_date", "date"),
        ("vendor_no", "string"),
        ("branch_code", "string"),
        ("item_no", "string"),
        ("uom", "string"),
        ("ordered_qty", "integer"),
        ("received_qty", "integer"),
        ("unit_cost_actual", "decimal"),
        ("unit_cost_standard", "decimal"),
        ("received_date", "date"),
    ),
    "invoice_lines": (
        ("invoice_no", "string"),
        ("line_no", "integer"),
        ("invoice_date", "date"),
        ("order_no", "string"),
        ("customer_no", "string"),
        ("branch_code", "string"),
        ("item_no", "string"),
        ("uom", "string"),
        ("invoiced_qty", "integer"),
        ("unit_price", "decimal"),
        ("unit_cost", "decimal"),
        ("freight_amt", "decimal"),
        ("tax_amt", "decimal"),
    ),
    "inventory_snapshots": (
        ("snapshot_date", "date"),
        ("branch_code", "string"),
        ("item_no", "string"),
        ("on_hand_qty", "integer"),
        ("allocated_qty", "integer"),
        ("on_order_qty", "integer"),
        ("backorder_qty", "integer"),
        ("unit_cost", "decimal"),
        ("inventory_value", "decimal"),
    ),
    "gl_entries": (
        ("journal_no", "string"),
        ("line_no", "integer"),
        ("entry_date", "date"),
        ("branch_code", "string"),
        ("account", "string"),
        ("description", "string"),
        ("debit_amt", "decimal"),
        ("credit_amt", "decimal"),
    ),
}

#: Natural source key per entity — the anti-join reconciliation and provenance
#: stamping must agree on this (see connectors.base.natural_id_for).
NATURAL_KEY_FIELDS: dict[str, tuple[str, ...]] = {
    "items": ("item_no",),
    "salespeople": ("salesperson_code",),
    "customers": ("customer_no",),
    "vendors": ("vendor_no",),
    "sales_order_lines": ("order_no", "line_no"),
    "purchase_order_lines": ("po_no", "line_no"),
    "invoice_lines": ("invoice_no", "line_no"),
    "inventory_snapshots": ("snapshot_date", "branch_code", "item_no"),
    "gl_entries": ("journal_no", "line_no"),
}


class LegacySchemaError(Exception):
    """An entity is missing from the canonical registry — a code bug, not config."""


_ARROW_TYPE_BY_KIND: dict[str, pa.DataType] = {
    "string": pa.string(),
    "integer": pa.int64(),
    "decimal": pa.decimal128(38, 9),  # wide enough for any money/qty value
    "date": pa.date32(),
}


def canonical_arrow_schema(entity: str) -> pa.Schema:
    """Explicit Arrow schema for a canonical entity's staging Parquet.

    Includes the provenance columns the base writer appends after the entity
    columns (mirrors connectors.csv_sftp's declared writer schema; loaded_at
    is an ISO string).
    """
    spec = CANONICAL_ENTITY_COLUMNS.get(entity)
    if spec is None:
        raise LegacySchemaError(
            f"entity '{entity}' is not in the canonical registry; "
            f"known: {', '.join(sorted(CANONICAL_ENTITY_COLUMNS))}"
        )
    fields = [pa.field(name, _ARROW_TYPE_BY_KIND[kind]) for name, kind in spec]
    fields += [
        pa.field("source_system", pa.string()),
        pa.field("source_id", pa.string()),
        pa.field("source_file", pa.string()),
        pa.field("source_row_no", pa.int64()),
        pa.field("batch_id", pa.string()),
        pa.field("loaded_at", pa.string()),
    ]
    return pa.schema(fields)
