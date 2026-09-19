"""Db2 LUW batch connector — spec §5 row 2, posture per inventory §3 (C1)."""

from __future__ import annotations

from typing import ClassVar

from connectors.base import ConnectorMaturity, ConnectorNotConfigured
from connectors.legacy.sql_source import DbApiBatchConnector, EntitySource


class Db2LuwConnector(DbApiBatchConnector):
    """IBM Db2 LUW via the IBM Db2 ODBC/CLI driver (pyodbc)."""

    erp_id = "db2_luw"
    maturity = ConnectorMaturity.IMPLEMENTED
    transport_label = "ODBC (IBM Db2 ODBC/CLI driver) via pyodbc"
    param_placeholder = "?"
    required_settings = ("odbc_dsn", "db_user", "db_password")
    extraction_notes = (
        "JDBC/ODBC batch via watermark (spec §5 row 2). CDC posture gated on the "
        "IIDR license — see connectors.legacy.db2_luw.cdc; batch needs no license."
    )

    entity_sources: ClassVar[dict[str, EntitySource]] = {
        "items": EntitySource(table="db2_items", incremental_column="last_modified"),
        "salespeople": EntitySource(table="db2_salespeople", incremental_column="last_modified"),
        "customers": EntitySource(table="db2_customers", incremental_column="last_modified"),
        "vendors": EntitySource(table="db2_vendors", incremental_column="last_modified"),
        "sales_order_lines": EntitySource(table="db2_so_lines", incremental_column="last_modified"),
        "purchase_order_lines": EntitySource(
            table="db2_po_lines", incremental_column="last_modified"
        ),
        "invoice_lines": EntitySource(
            table="db2_invoice_lines", incremental_column="last_modified"
        ),
        "inventory_snapshots": EntitySource(
            table="db2_inventory_snap", incremental_column="snapshot_date"
        ),
        "gl_entries": EntitySource(table="db2_gl_entries", incremental_column="entry_date"),
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
                    "extract from Db2 LUW"
                ) from exc
            return pyodbc.connect(
                f"DSN={settings['odbc_dsn']};"
                f"UID={settings['db_user']};PWD={settings['db_password']}"
            )

        return factory
