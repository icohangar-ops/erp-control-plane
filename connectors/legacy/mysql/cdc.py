"""MySQL/MariaDB CDC module — binlog, stable Debezium connectors (spec §6, C5)."""

from __future__ import annotations

from connectors.legacy.cdc import CdcModule

MYSQL_CDC = CdcModule(
    connector="mysql",
    engine="MySQL / MariaDB (binlog)",
    debezium_artifact="io.debezium:debezium-connector-mysql",
    debezium_maturity="stable",
    prerequisites=(
        "row-based binlog (binlog_format=ROW) with binlog_row_image=FULL",
        "binlog retention long enough for outage recovery windows",
        "a MySQL/MariaDB user with REPLICATION SLAVE and REPLICATION CLIENT",
        "Kafka Connect runtime with offsets + snapshot management",
    ),
    license_gate=(
        "MySQL Connector/J is GPLv2 with the Universal FOSS Exception (C5) - "
        "review before linking into licensed distributions. PyMySQL (batch path) "
        "is MIT and does not carry this question."
    ),
    recommended_mode="batch",
    notes=(
        "Both MySQL and MariaDB Debezium connectors are non-incubating (C5), so "
        "CDC is technically unencumbered; batch with the anti-join remains the "
        "CI-tested default per spec §6."
    ),
    sources=(
        "artifact art_7DIRx9Nu section 3 (C5) - both connectors non-incubating; "
        "Connector/J GPLv2/UFE license note",
        "artifact art_HFsCu22Y section 6 - MySQL/MariaDB CDC/batch decision row",
    ),
)


def module() -> CdcModule:
    """The MySQL/MariaDB CDC posture."""
    return MYSQL_CDC
