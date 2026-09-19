"""PostgreSQL CDC module — logical decoding via pgoutput (spec §6)."""

from __future__ import annotations

from connectors.legacy.cdc import CdcModule

POSTGRES_CDC = CdcModule(
    connector="postgresql",
    engine="PostgreSQL (logical decoding, pgoutput)",
    debezium_artifact="io.debezium:debezium-connector-postgres",
    debezium_maturity="stable",
    prerequisites=(
        "wal_level=logical with a pgoutput replication slot",
        "database encoding UTF-8 (per Debezium docs, required)",
        "replica identity (default/full) set on tables without primary keys",
        "DDL events are not captured - schema changes go through migrations",
    ),
    recommended_mode="batch",
    notes=(
        "The Debezium PostgreSQL connector is stable and well documented. Batch "
        "with the anti-join remains the CI-tested default per spec §6."
    ),
    sources=(
        "artifact art_7DIRx9Nu section 4 - PostgreSQL row confirmed as specced",
        "artifact art_HFsCu22Y section 6 - PostgreSQL CDC/batch decision row",
    ),
)


def module() -> CdcModule:
    """The PostgreSQL CDC posture."""
    return POSTGRES_CDC
