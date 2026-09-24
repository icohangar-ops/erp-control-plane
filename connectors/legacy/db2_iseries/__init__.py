"""Db2 for i (AS/400) batch connector — spec §5 row 3, posture per inventory §3 (C3).

JT400 JDBC batch is the default path, reached through the JayDeBeApi bridge
(JVM required) — dlt's ``sql_database`` cannot connect (no SQLAlchemy dialect).
This connector also demonstrates the explicit canonical←source column map:
IBM i DDS files use short coded field names, so extraction aliases them to the
canonical staging columns in SQL rather than renaming in Python.
"""

from __future__ import annotations

from typing import ClassVar

from connectors.base import ConnectorMaturity, ConnectorNotConfigured
from connectors.legacy.sql_source import DbApiBatchConnector, EntitySource

_JT400_CLASS = "com.ibm.as400.access.AS400JDBCDriver"


class Db2ISeriesConnector(DbApiBatchConnector):
    """Db2 for i via the JT400 JDBC toolkit (JayDeBeApi bridge)."""

    erp_id = "db2_iseries"
    maturity = ConnectorMaturity.IMPLEMENTED
    transport_label = "JDBC (JT400 toolkit) via JayDeBeApi bridge — dlt custom resource"
    param_placeholder = "?"
    required_settings = ("jdbc_url", "db_user", "db_password")
    extraction_notes = (
        "JT400 JDBC batch default (spec §5 row 3). CDC posture per C3: the "
        "Debezium Db2 for i connector is incubating and undocumented in Debezium "
        "reference docs — any attempt must pin a Final artifact (see "
        "connectors.legacy.db2_iseries.cdc); batch stays the default."
    )

    entity_sources: ClassVar[dict[str, EntitySource]] = {
        "items": EntitySource(table="SHIPLIB.ITMMST", incremental_column="MMUPDT"),
        "salespeople": EntitySource(table="SHIPLIB.SLPMAST", incremental_column="SMUPDT"),
        "customers": EntitySource(table="SHIPLIB.CSTMST", incremental_column="CMUPDT"),
        "vendors": EntitySource(table="SHIPLIB.VNDMST", incremental_column="VMUPDT"),
        "sales_order_lines": EntitySource(
            table="SHIPLIB.OEORDL1",
            incremental_column="OLUPDT",
            columns=(
                ("order_no", "ORDNO"),
                ("line_no", "ORLIN"),
                ("order_date", "ORDAT"),
                ("customer_no", "ORCST"),
                ("branch_code", "ORBRN"),
                ("salesperson_code", "ORSLP"),
                ("item_no", "ORITM"),
                ("uom", "ORUOM"),
                ("ordered_qty", "OROQT"),
                ("filled_qty", "ORFQT"),
                ("cancelled_qty", "ORCQT"),
                ("unit_price", "ORPRC"),
                ("unit_cost", "ORCST"),
                ("promised_date", "ORPDD"),
                ("shipped_date", "ORSDD"),
                ("order_status", "ORSTS"),
            ),
        ),
        "purchase_order_lines": EntitySource(table="SHIPLIB.POLIN2", incremental_column="PLUPDT"),
        "invoice_lines": EntitySource(table="SHIPLIB.INVLIN", incremental_column="ILUPDT"),
        "inventory_snapshots": EntitySource(table="SHIPLIB.INVSNP", incremental_column="SNDAT"),
        "gl_entries": EntitySource(table="SHIPLIB.GLENTR", incremental_column="GEENTD"),
    }

    @staticmethod
    def _build_default_connection_factory():
        """JayDeBeApi connection built lazily — JVM + jt400 jar are site-side."""

        def factory(settings):
            try:
                import jaydebeapi
            except ImportError as exc:  # pragma: no cover - depends on site image
                raise ConnectorNotConfigured(
                    "jaydebeapi is not installed; install the legacy-drivers extra "
                    "and provision the jt400 jar (JT400_JARS setting) to extract "
                    "from Db2 for i"
                ) from exc
            jars = settings.get("jt400_jars")
            return jaydebeapi.connect(
                _JT400_CLASS,
                settings["jdbc_url"],
                [settings["db_user"], settings["db_password"]],
                jars=jars.split(",") if jars else None,
            )

        return factory
