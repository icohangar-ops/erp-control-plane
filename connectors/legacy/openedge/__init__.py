"""Progress OpenEdge batch connector — spec §5 row 9, posture per inventory §3 (C2).

OpenEdge exposes SQL through ODBC; there is no SQLAlchemy dialect, so dlt's
``sql_database`` source cannot connect (C2) — this connector is the custom
pyodbc/DataDirect ODBC dlt resource the inventory prescribes.
"""

from __future__ import annotations

from typing import ClassVar

from connectors.base import ConnectorMaturity, ConnectorNotConfigured
from connectors.legacy.sql_source import DbApiBatchConnector, EntitySource


class OpenEdgeConnector(DbApiBatchConnector):
    """Progress OpenEdge via the DataDirect OpenEdge ODBC driver (pyodbc)."""

    erp_id = "openedge"
    maturity = ConnectorMaturity.IMPLEMENTED
    transport_label = "ODBC (DataDirect OpenEdge driver) via pyodbc — custom dlt resource"
    param_placeholder = "?"
    required_settings = ("odbc_dsn", "db_user", "db_password")
    extraction_notes = (
        "Custom pyodbc/DataDirect ODBC dlt resource (spec §5 row 9; C2: no "
        "SQLAlchemy dialect exists, so dlt sql_database cannot connect). Batch "
        "only per spec §6; native OpenEdge CDC / Pro2 is the site-licensed CDC "
        "pilot option — see connectors.legacy.openedge.cdc."
    )

    entity_sources: ClassVar[dict[str, EntitySource]] = {
        "items": EntitySource(table="oe_items", incremental_column="last_modified"),
        "salespeople": EntitySource(table="oe_salespeople", incremental_column="last_modified"),
        "customers": EntitySource(table="oe_customers", incremental_column="last_modified"),
        "vendors": EntitySource(table="oe_vendors", incremental_column="last_modified"),
        "sales_order_lines": EntitySource(table="oe_so_lines", incremental_column="last_modified"),
        "purchase_order_lines": EntitySource(
            table="oe_po_lines", incremental_column="last_modified"
        ),
        "invoice_lines": EntitySource(table="oe_invoice_lines", incremental_column="last_modified"),
        "inventory_snapshots": EntitySource(
            table="oe_inventory_snap", incremental_column="snapshot_date"
        ),
        "gl_entries": EntitySource(table="oe_gl_entries", incremental_column="entry_date"),
    }

    @staticmethod
    def _build_default_connection_factory():
        """pyodbc connection built lazily — the driver is an optional install."""

        def factory(settings):
            try:
                import pyodbc
            except ImportError as exc:  # pragma: no cover - depends on site image
                raise ConnectorNotConfigured(
                    "pyodbc is not installed; install the legacy-drivers extra to "
                    "extract from OpenEdge"
                ) from exc
            return pyodbc.connect(
                f"DSN={settings['odbc_dsn']};"
                f"UID={settings['db_user']};PWD={settings['db_password']}"
            )

        return factory
