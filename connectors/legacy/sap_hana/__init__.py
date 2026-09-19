"""SAP HANA batch connector — SAP Business One on HANA (spec §5, 15-class matrix).

STATUS: coded against the documented sqlalchemy-hana surface — the ``hana://``
SQLAlchemy dialect over the hdbcli client, i.e. the same engine type dlt's
``sql_database`` source uses — but NOT exercised against a live HANA instance
(fixture-tested over the shared fake DB-API only; no network access to any SAP
system). Extraction reads onboarding staging views (``b1_*``) exposed to a
dedicated read-only extraction user; validate the view columns and watermark
column against the tenant schema at onboarding, and dry-run
(``python -m connectors.cli plan --source sap_hana_template``) before any live
call.

Extraction notes (spec §5/§6; posture per the verified-inventory reconciliation):
- SAP Business One keeps site data in the tenant schema (OITM items, OCRD
  business partners, ORDR/RDR1 orders, OPOR/POR1 purchase orders, OINV/INV1
  invoices, OJDT/JDT1 journal entries, OITW inventory). The shared
  :class:`DbApiBatchConnector` expects one table per entity with canonical
  column names — exactly the staging-view contract: the site DBA exposes
  ``b1_*`` views over those tables (OCRD serves both customers and vendors,
  filtered by CardType, flattened into two views).
- Batch-only per spec §6: no CDC path verified this session; deletes
  reconcile through the scheduled full-key anti-join like every other pack
  member (no delete feed — hard deletes surface only as missing keys).
- Transport: SQLAlchemy engine on the ``hana://`` dialect (sqlalchemy-hana
  dialect, hdbcli underneath), raw DB-API connection so the shared machinery
  binds values qmark-style (hdbcli paramstyle).
- HANA instance ports follow the 3NN15 convention — port 3{instance:02d}15,
  e.g. instance 90 → 39015 (see docs/CONNECTOR_GUIDE.md).
"""

from __future__ import annotations

from dataclasses import replace
from typing import ClassVar
from urllib.parse import quote_plus

from connectors.base import ConnectorMaturity, ConnectorNotConfigured
from connectors.legacy.sql_source import DbApiBatchConnector, EntitySource, validate_identifier


class SapHanaConnector(DbApiBatchConnector):
    """SAP HANA (SAP Business One on HANA) via the sqlalchemy-hana dialect.

    Follows the dlt ``sql_database`` source shape — SQLAlchemy engine, batch
    reads with a watermark column — while emitting the same provenance-stamped
    canonical records as the rest of the legacy pack.
    """

    erp_id = "sap_hana"
    maturity = ConnectorMaturity.IMPLEMENTED
    transport_label = (
        "SQLAlchemy hana:// engine (sqlalchemy-hana, hdbcli) — dlt sql_database surface"
    )
    param_placeholder = "?"
    required_settings = ("db_host", "db_port", "db_user", "db_password")
    extraction_notes = (
        "SAP HANA batch extraction via SQLAlchemy (sqlalchemy-hana dialect, hdbcli "
        "client) — the dlt sql_database engine shape — over DBA-maintained staging "
        "views exposed to a read-only extraction user. Instance ports follow the "
        "3NN15 convention (instance 90 → 39015). Batch-only per spec §6: no CDC "
        "path verified; deletes reconcile via the scheduled full-key anti-join. "
        "UNEXERCISED against live HANA — validate view columns and watermarks at "
        "onboarding."
    )

    #: One staging view per canonical entity, exposed to the read-only
    #: extraction user with canonical column names (identity map — the
    #: onboarding view contract; see module docstring).
    entity_sources: ClassVar[dict[str, EntitySource]] = {
        "items": EntitySource(table="b1_items", incremental_column="last_modified"),
        "salespeople": EntitySource(table="b1_salespeople", incremental_column="last_modified"),
        "customers": EntitySource(table="b1_customers", incremental_column="last_modified"),
        "vendors": EntitySource(table="b1_vendors", incremental_column="last_modified"),
        "sales_order_lines": EntitySource(table="b1_so_lines", incremental_column="last_modified"),
        "purchase_order_lines": EntitySource(
            table="b1_po_lines", incremental_column="last_modified"
        ),
        "invoice_lines": EntitySource(table="b1_invoice_lines", incremental_column="last_modified"),
        "inventory_snapshots": EntitySource(
            table="b1_inventory_snap", incremental_column="snapshot_date"
        ),
        "gl_entries": EntitySource(table="b1_gl_entries", incremental_column="entry_date"),
    }

    def _entity_source(self, entity: str) -> EntitySource:
        """Entity source with the optional ``db_schema`` setting qualified on.

        Settings-declared schemas are identifier-validated then prefixed
        (``B1SCHEMA.b1_items``) so every shared SQL path — SELECT, key scan,
        dry-run plan — sees the same qualified name.
        """
        source = super()._entity_source(entity)
        schema = self.source.settings.get("db_schema", "").strip()
        if not schema:
            return source
        validate_identifier(schema, "schema")
        return replace(source, table=f"{schema}.{source.table}")

    @staticmethod
    def _build_default_connection_factory():
        """SQLAlchemy hana:// engine's raw DB-API connection — lazy imports.

        sqlalchemy-hana (and its hdbcli dependency) is a site-provided optional
        install, like pyodbc for the ODBC pack; tests and special sites inject
        ``connection_factory=`` instead (see tests/fixtures/fake_dbapi.py).
        """

        def factory(settings):
            try:
                import sqlalchemy
                import sqlalchemy_hana  # noqa: F401 — registers the hana:// dialect
            except ImportError as exc:  # pragma: no cover - depends on site image
                raise ConnectorNotConfigured(
                    "sqlalchemy-hana and hdbcli are not installed; install the "
                    "sap-hana extra to extract from SAP HANA"
                ) from exc
            engine = sqlalchemy.create_engine(
                "hana://{user}:{password}@{host}:{port}".format(
                    user=quote_plus(settings["db_user"]),
                    password=quote_plus(settings["db_password"]),
                    host=settings["db_host"],
                    port=settings["db_port"],
                )
            )
            return engine.raw_connection()

        return factory
