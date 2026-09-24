"""Shared machinery for the legacy SQL connector pack (spec §5 rows 1-9).

Every legacy database source class — Informix, Db2 LUW, Db2 for i, Oracle,
SQL Server, PostgreSQL, MySQL/MariaDB, Sybase ASE, Progress OpenEdge — feeds
the same canonical staging entities through the same DB-API batch extraction
path (:class:`connectors.legacy.sql_source.DbApiBatchConnector`). Per-ERP
connectors only declare *where* each entity lives (table, column map,
watermark column); the extraction, provenance, watermark, and delete
reconciliation behavior is shared and tested once.
"""

from connectors.legacy.schemas import (
    CANONICAL_ENTITY_COLUMNS,
    NATURAL_KEY_FIELDS,
    canonical_arrow_schema,
)
from connectors.legacy.sql_source import DbApiBatchConnector, EntitySource

__all__ = [
    "CANONICAL_ENTITY_COLUMNS",
    "NATURAL_KEY_FIELDS",
    "DbApiBatchConnector",
    "EntitySource",
    "canonical_arrow_schema",
]
