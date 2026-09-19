"""Expose any connector's extraction as a dlt resource.

The control plane's ingestion stack is dlt-based (v0.1 lineage): dlt ingests
from arbitrary Python sources, so every connector in this pack — including the
ones dlt's ``sql_database`` source cannot reach (OpenEdge has no SQLAlchemy
dialect at all; Sybase ASE only via an external community dialect; Db2 for i
via JT400 JDBC) — can feed a dlt pipeline through this one wrapper. It yields
the same provenance-stamped records the Parquet writer consumes, so a dlt
pipeline and a Dagster extraction cannot drift apart.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from connectors.base import ExtractionMode

if TYPE_CHECKING:  # pragma: no cover - import for typing only
    from connectors.base import BaseConnector


def as_dlt_resource(
    connector: BaseConnector,
    entity: str,
    mode: ExtractionMode = ExtractionMode.BACKFILL,
    watermark: str | None = None,
):
    """Return a ``dlt.resource`` streaming the connector's stamped records.

    ``primary_key="source_id"`` — the provenance-stamped natural key — so
    dlt merge dispositions and downstream lineage key rows consistently with
    the anti-join reconciliation. The connector owns watermark advancement;
    this wrapper does not duplicate it.
    """
    import dlt  # lazy: keeps non-dlt import paths (CLI plan, contract tests) fast

    @dlt.resource(name=entity, table_name=entity, primary_key="source_id")
    def _resource():
        yield from connector.stamped_records(entity, mode, watermark)

    return _resource
