"""Db2 for i CDC module — incubating artifact, Final-version pin required (C3).

Verified inventory (C3): the Debezium Db2 for i connector is incubating and
absent from Debezium reference documentation. Final artifacts exist for
3.0.0-3.1.1 (newest line 3.2.0.Alpha1 is an alpha). JT400 JDBC batch stays the
default path; any CDC attempt must pin a Final artifact.
"""

from __future__ import annotations

from connectors.legacy.cdc import CdcModule

DB2_ISERIES_CDC = CdcModule(
    connector="db2_iseries",
    engine="IBM Db2 for i (journal-based via SQL replication services)",
    debezium_artifact="io.debezium:debezium-connector-ibmi",
    debezium_maturity="incubating",
    prerequisites=(
        "pin a Final release (3.0.0-3.1.1) - never an Alpha (newest line is "
        "3.2.0.Alpha1 and is not a candidate)",
        "journaling enabled on captured files; SQL replication services reachable",
        "Kafka Connect runtime with offsets + snapshot management",
    ),
    recommended_mode="batch",
    notes=(
        "The connector line is incubating and undocumented in Debezium reference "
        "docs - a pilot is possible, not a default. JT400 JDBC batch via this "
        "package's anti-join reconciliation is the supported path."
    ),
    sources=(
        "artifact art_7DIRx9Nu section 3 (C3) - incubating, no reference-docs page, "
        "Final artifacts 3.0.0-3.1.1, newest 3.2.0.Alpha1",
        "artifact art_HFsCu22Y section 6 - Db2 for i CDC/batch decision row",
    ),
)


def module() -> CdcModule:
    """The Db2 for i CDC posture."""
    return DB2_ISERIES_CDC
