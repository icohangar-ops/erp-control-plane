"""SQL Server batch connector — spec §5 row 5 (native CDC + ODBC batch)."""

from __future__ import annotations

from typing import ClassVar

from connectors.base import ConnectorMaturity, ConnectorNotConfigured
from connectors.legacy.sql_source import DbApiBatchConnector, EntitySource


class SqlServerConnector(DbApiBatchConnector):
    """Microsoft SQL Server via the Microsoft ODBC Driver (pyodbc)."""

    erp_id = "sqlserver"
    maturity = ConnectorMaturity.IMPLEMENTED
    transport_label = "ODBC (Microsoft ODBC Driver 18 for SQL Server) via pyodbc"
    param_placeholder = "?"
    required_settings = ("odbc_dsn", "db_user", "db_password")
    extraction_notes = (
        "Native CDC change tables plus ODBC batch (spec §5 row 5, confirmed). The "
        "Debezium SQL Server connector (stable) reads the same change tables; "
        "batch with the anti-join is the CI-tested default."
    )

    entity_sources: ClassVar[dict[str, EntitySource]] = {
        "items": EntitySource(table="dbo.Items", incremental_column="LastModified"),
        "salespeople": EntitySource(table="dbo.Salespeople", incremental_column="LastModified"),
        "customers": EntitySource(table="dbo.Customers", incremental_column="LastModified"),
        "vendors": EntitySource(table="dbo.Vendors", incremental_column="LastModified"),
        "sales_order_lines": EntitySource(
            table="dbo.SalesOrderLines", incremental_column="LastModified"
        ),
        "purchase_order_lines": EntitySource(
            table="dbo.PurchaseOrderLines", incremental_column="LastModified"
        ),
        "invoice_lines": EntitySource(table="dbo.InvoiceLines", incremental_column="LastModified"),
        "inventory_snapshots": EntitySource(
            table="dbo.InventorySnapshots", incremental_column="SnapshotDate"
        ),
        "gl_entries": EntitySource(table="dbo.GlEntries", incremental_column="EntryDate"),
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
                    "extract from SQL Server"
                ) from exc
            return pyodbc.connect(
                f"DSN={settings['odbc_dsn']};"
                f"UID={settings['db_user']};PWD={settings['db_password']}"
            )

        return factory
