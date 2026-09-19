"""Informix batch connector — spec §5 row 1, posture per inventory §3 (C4).

Primary path: JDBC/ODBC batch extraction via watermark (the shared
:class:`DbApiBatchConnector` machinery). The optional Debezium change-streams
CDC module is documented in :mod:`connectors.legacy.informix.cdc` but not
wired into CI loads.
"""

from __future__ import annotations

from typing import ClassVar

from connectors.base import ConnectorMaturity, ConnectorNotConfigured
from connectors.legacy.sql_source import DbApiBatchConnector, EntitySource


class InformixConnector(DbApiBatchConnector):
    """IBM Informix via the Informix Client SDK ODBC driver (pyodbc)."""

    erp_id = "informix"
    maturity = ConnectorMaturity.IMPLEMENTED
    transport_label = "ODBC (IBM Informix Client SDK) via pyodbc — dlt custom resource"
    param_placeholder = "?"
    required_settings = ("odbc_dsn", "db_user", "db_password")
    extraction_notes = (
        "JDBC/ODBC batch primary via watermark (spec §5 row 1). Optional Debezium "
        "change-streams CDC module documented, not CI-required (spec §6)."
    )

    entity_sources: ClassVar[dict[str, EntitySource]] = {
        "items": EntitySource(table="ifx_items", incremental_column="last_modified"),
        "salespeople": EntitySource(table="ifx_salespeople", incremental_column="last_modified"),
        "customers": EntitySource(table="ifx_customers", incremental_column="last_modified"),
        "vendors": EntitySource(table="ifx_vendors", incremental_column="last_modified"),
        "sales_order_lines": EntitySource(table="ifx_so_lines", incremental_column="last_modified"),
        "purchase_order_lines": EntitySource(
            table="ifx_po_lines", incremental_column="last_modified"
        ),
        "invoice_lines": EntitySource(
            table="ifx_invoice_lines", incremental_column="last_modified"
        ),
        "inventory_snapshots": EntitySource(
            table="ifx_inventory_snap", incremental_column="snapshot_date"
        ),
        "gl_entries": EntitySource(table="ifx_gl_entries", incremental_column="entry_date"),
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
                    "extract from Informix"
                ) from exc
            return pyodbc.connect(
                f"DSN={settings['odbc_dsn']};"
                f"UID={settings['db_user']};PWD={settings['db_password']}"
            )

        return factory
