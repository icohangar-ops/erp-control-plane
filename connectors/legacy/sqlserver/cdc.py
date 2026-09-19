"""SQL Server CDC module — native change tables, Debezium reader (spec §6)."""

from __future__ import annotations

from connectors.legacy.cdc import CdcModule

SQLSERVER_CDC = CdcModule(
    connector="sqlserver",
    engine="Microsoft SQL Server (native CDC change tables)",
    debezium_artifact="io.debezium:debezium-connector-sqlserver",
    debezium_maturity="stable",
    prerequisites=(
        "SQL Server CDC enabled per database/table (Standard or Enterprise; "
        "2016 SP1+ for Standard)",
        "SQL Server Agent running to populate change tables",
        "Kafka Connect runtime with offsets + snapshot management",
    ),
    vendor_alternative=(
        "native CDC change tables read by T-SQL/ODBC polling - usable without a "
        "Kafka Connect deployment for lighter deployments"
    ),
    recommended_mode="batch",
    notes=(
        "Both paths rely on the same native change tables. Batch with the "
        "anti-join reconciliation remains the CI-tested default (spec §6 row 5)."
    ),
    sources=(
        "artifact art_7DIRx9Nu section 4 - SQL Server row confirmed as specced",
        "artifact art_HFsCu22Y section 6 - SQL Server CDC/batch decision row",
    ),
)


def module() -> CdcModule:
    """The SQL Server CDC posture."""
    return SQLSERVER_CDC
