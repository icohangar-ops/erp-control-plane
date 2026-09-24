"""Sybase ASE batch connector — spec §5 row 8, posture per inventory §3 (C7).

Batch only: ASE has no Debezium connector, and ASE access requires an external
community SQLAlchemy dialect wired explicitly (C7) — this pack reaches ASE over
jConnect JDBC via the JayDeBeApi bridge instead, so no dialect wiring is
needed. Soft-delete handling is configured per site for the anti-join.
"""

from __future__ import annotations

from typing import ClassVar

from connectors.base import ConnectorMaturity, ConnectorNotConfigured
from connectors.legacy.sql_source import DbApiBatchConnector, EntitySource

_JCONNECT_CLASS = "com.sybase.jdbc4.jdbc.SybDriver"


class SybaseAseConnector(DbApiBatchConnector):
    """SAP Sybase ASE via jConnect JDBC (JayDeBeApi bridge)."""

    erp_id = "sybase_ase"
    maturity = ConnectorMaturity.IMPLEMENTED
    transport_label = "JDBC (jConnect) via JayDeBeApi bridge — dlt custom resource"
    param_placeholder = "?"
    required_settings = ("jdbc_url", "db_user", "db_password")
    extraction_notes = (
        "jConnect JDBC batch only (spec §5 row 8). No Debezium connector exists "
        "for ASE; the external community SQLAlchemy dialect (C7) is another "
        "option but this pack uses the JDBC bridge. Deletes are soft-flagged at "
        "most sites - set soft_delete_column/soft_delete_value settings and the "
        "anti-join inventory excludes them; hard deletes are reconciled by the "
        "anti-join like every batch path."
    )

    entity_sources: ClassVar[dict[str, EntitySource]] = {
        "items": EntitySource(table="ase_items", incremental_column="last_modified"),
        "salespeople": EntitySource(table="ase_salespeople", incremental_column="last_modified"),
        "customers": EntitySource(table="ase_customers", incremental_column="last_modified"),
        "vendors": EntitySource(table="ase_vendors", incremental_column="last_modified"),
        "sales_order_lines": EntitySource(table="ase_so_lines", incremental_column="last_modified"),
        "purchase_order_lines": EntitySource(
            table="ase_po_lines", incremental_column="last_modified"
        ),
        "invoice_lines": EntitySource(
            table="ase_invoice_lines", incremental_column="last_modified"
        ),
        "inventory_snapshots": EntitySource(
            table="ase_inventory_snap", incremental_column="snapshot_date"
        ),
        "gl_entries": EntitySource(table="ase_gl_entries", incremental_column="entry_date"),
    }

    @staticmethod
    def _build_default_connection_factory():
        """JayDeBeApi connection built lazily — JVM + jConnect jar are site-side."""

        def factory(settings):
            try:
                import jaydebeapi
            except ImportError as exc:  # pragma: no cover - depends on site image
                raise ConnectorNotConfigured(
                    "jaydebeapi is not installed; install the legacy-drivers extra "
                    "and provision the jConnect jar (jconnect_jars setting) to "
                    "extract from Sybase ASE"
                ) from exc
            jars = settings.get("jconnect_jars")
            return jaydebeapi.connect(
                _JCONNECT_CLASS,
                settings["jdbc_url"],
                [settings["db_user"], settings["db_password"]],
                jars=jars.split(",") if jars else None,
            )

        return factory
