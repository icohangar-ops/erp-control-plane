"""PostgreSQL batch connector — spec §5 row 6 (logical decoding CDC + DB-API batch)."""

from __future__ import annotations

from typing import ClassVar

from connectors.base import ConnectorMaturity, ConnectorNotConfigured
from connectors.legacy.sql_source import DbApiBatchConnector, EntitySource


class PostgresConnector(DbApiBatchConnector):
    """PostgreSQL via psycopg2 (DB-API)."""

    erp_id = "postgresql"
    maturity = ConnectorMaturity.IMPLEMENTED
    transport_label = "DB-API (psycopg2)"
    param_placeholder = "%s"
    required_settings = ("db_host", "db_port", "db_name", "db_user", "db_password")
    extraction_notes = (
        "DB-API batch via watermark (spec §5 row 6, confirmed). Debezium PostgreSQL "
        "connector (stable, pgoutput logical decoding) is the CDC path when a hot "
        "entity justifies Kafka Connect - see connectors.legacy.postgresql.cdc."
    )

    entity_sources: ClassVar[dict[str, EntitySource]] = {
        "items": EntitySource(table="pg_items", incremental_column="last_modified"),
        "salespeople": EntitySource(table="pg_salespeople", incremental_column="last_modified"),
        "customers": EntitySource(table="pg_customers", incremental_column="last_modified"),
        "vendors": EntitySource(table="pg_vendors", incremental_column="last_modified"),
        "sales_order_lines": EntitySource(table="pg_so_lines", incremental_column="last_modified"),
        "purchase_order_lines": EntitySource(
            table="pg_po_lines", incremental_column="last_modified"
        ),
        "invoice_lines": EntitySource(table="pg_invoice_lines", incremental_column="last_modified"),
        "inventory_snapshots": EntitySource(
            table="pg_inventory_snap", incremental_column="snapshot_date"
        ),
        "gl_entries": EntitySource(table="pg_gl_entries", incremental_column="entry_date"),
    }

    @staticmethod
    def _build_default_connection_factory():
        """psycopg2 connection built lazily — the driver is an optional install."""

        def factory(settings):
            try:
                import psycopg2
            except ImportError as exc:  # pragma: no cover - depends on site image
                raise ConnectorNotConfigured(
                    "psycopg2 is not installed; install the legacy-drivers extra to "
                    "extract from PostgreSQL"
                ) from exc
            return psycopg2.connect(
                host=settings["db_host"],
                port=settings["db_port"],
                dbname=settings["db_name"],
                user=settings["db_user"],
                password=settings["db_password"],
            )

        return factory
