"""MySQL/MariaDB batch connector — spec §5 row 7, posture per inventory §3 (C5).

One coverage row, two connector classes: MySQL and MariaDB share the binlog
CDC story (both non-incubating Debezium connectors per C5) while differing in
driver notes. The MySQL Connector/J GPLv2 + Universal FOSS Exception license
note binds (C5).
"""

from __future__ import annotations

from typing import ClassVar

from connectors.base import ConnectorMaturity, ConnectorNotConfigured
from connectors.legacy.sql_source import DbApiBatchConnector, EntitySource


class MySqlConnector(DbApiBatchConnector):
    """MySQL via PyMySQL (pure-Python DB-API)."""

    erp_id = "mysql"
    maturity = ConnectorMaturity.IMPLEMENTED
    transport_label = "DB-API (PyMySQL)"
    param_placeholder = "%s"
    required_settings = ("db_host", "db_port", "db_name", "db_user", "db_password")
    extraction_notes = (
        "DB-API batch via watermark (spec §5 row 7, confirmed). Binlog CDC via "
        "stable Debezium connectors for both MySQL and MariaDB (C5). License "
        "note (C5): MySQL Connector/J is GPLv2 with the Universal FOSS "
        "Exception - review before linking into licensed distributions."
    )

    entity_sources: ClassVar[dict[str, EntitySource]] = {
        "items": EntitySource(table="my_items", incremental_column="last_modified"),
        "salespeople": EntitySource(table="my_salespeople", incremental_column="last_modified"),
        "customers": EntitySource(table="my_customers", incremental_column="last_modified"),
        "vendors": EntitySource(table="my_vendors", incremental_column="last_modified"),
        "sales_order_lines": EntitySource(table="my_so_lines", incremental_column="last_modified"),
        "purchase_order_lines": EntitySource(
            table="my_po_lines", incremental_column="last_modified"
        ),
        "invoice_lines": EntitySource(table="my_invoice_lines", incremental_column="last_modified"),
        "inventory_snapshots": EntitySource(
            table="my_inventory_snap", incremental_column="snapshot_date"
        ),
        "gl_entries": EntitySource(table="my_gl_entries", incremental_column="entry_date"),
    }

    @staticmethod
    def _build_default_connection_factory():
        """PyMySQL connection built lazily — the driver is an optional install."""

        def factory(settings):
            try:
                import pymysql
            except ImportError as exc:  # pragma: no cover - depends on site image
                raise ConnectorNotConfigured(
                    "PyMySQL is not installed; install the legacy-drivers extra to "
                    "extract from MySQL/MariaDB"
                ) from exc
            return pymysql.connect(
                host=settings["db_host"],
                port=int(settings["db_port"]),
                database=settings["db_name"],
                user=settings["db_user"],
                password=settings["db_password"],
            )

        return factory


class MariaDbConnector(MySqlConnector):
    """MariaDB — same DB-API surface; license note does not apply."""

    erp_id = "mariadb"
    transport_label = "DB-API (PyMySQL against MariaDB)"
    extraction_notes = (
        "MariaDB variant of the MySQL row (spec §5 row 7). Debezium MariaDB "
        "connector is non-incubating (C5). No Connector/J GPLv2 note applies - "
        "MariaDB clients are GPL/LGPL with different terms; batch extraction "
        "over PyMySQL avoids the question entirely."
    )
