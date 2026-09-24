"""Oracle batch connector — spec §5 row 4, posture per inventory §3 (confirmed).

ODBC batch is the primary path. CDC runs through LogMiner (no extra license)
with the XStream/GoldenGate gate documented in connectors.legacy.oracle.cdc.
"""

from __future__ import annotations

from typing import ClassVar

from connectors.base import ConnectorMaturity, ConnectorNotConfigured
from connectors.legacy.sql_source import DbApiBatchConnector, EntitySource


class OracleConnector(DbApiBatchConnector):
    """Oracle Database via the Oracle ODBC driver (pyodbc)."""

    erp_id = "oracle"
    maturity = ConnectorMaturity.IMPLEMENTED
    transport_label = "ODBC (Oracle ODBC driver) via pyodbc"
    param_placeholder = "?"
    required_settings = ("odbc_dsn", "db_user", "db_password")
    extraction_notes = (
        "ODBC batch via watermark (spec §5 row 4, confirmed). LogMiner CDC module "
        "documented with its license note (LogMiner: no extra license; XStream: "
        "GoldenGate commercial) — see connectors.legacy.oracle.cdc."
    )

    entity_sources: ClassVar[dict[str, EntitySource]] = {
        "items": EntitySource(table="ORA_ITEMS", incremental_column="LAST_MODIFIED"),
        "salespeople": EntitySource(table="ORA_SALESPEOPLE", incremental_column="LAST_MODIFIED"),
        "customers": EntitySource(table="ORA_CUSTOMERS", incremental_column="LAST_MODIFIED"),
        "vendors": EntitySource(table="ORA_VENDORS", incremental_column="LAST_MODIFIED"),
        "sales_order_lines": EntitySource(table="ORA_SO_LINES", incremental_column="LAST_MODIFIED"),
        "purchase_order_lines": EntitySource(
            table="ORA_PO_LINES", incremental_column="LAST_MODIFIED"
        ),
        "invoice_lines": EntitySource(
            table="ORA_INVOICE_LINES", incremental_column="LAST_MODIFIED"
        ),
        "inventory_snapshots": EntitySource(
            table="ORA_INVENTORY_SNAP", incremental_column="SNAPSHOT_DATE"
        ),
        "gl_entries": EntitySource(table="ORA_GL_ENTRIES", incremental_column="ENTRY_DATE"),
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
                    "extract from Oracle"
                ) from exc
            return pyodbc.connect(
                f"DSN={settings['odbc_dsn']};"
                f"UID={settings['db_user']};PWD={settings['db_password']}"
            )

        return factory
