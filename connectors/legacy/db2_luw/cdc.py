"""Db2 LUW CDC module — license-gated (spec §6, C1 correction binds).

The verified inventory's binding correction: Db2 LUW CDC via SQL Replication
requires a separate IBM InfoSphere Data Replication (IIDR) license. Document
the gate; do not assume CDC availability at any site.
"""

from __future__ import annotations

from connectors.legacy.cdc import CdcModule

DB2_LUW_CDC = CdcModule(
    connector="db2_luw",
    engine="IBM Db2 LUW (SQL Replication / ASN Capture)",
    debezium_artifact="io.debezium:debezium-connector-db2",
    debezium_maturity="stable",
    prerequisites=(
        "SQL Replication (ASN Capture/Apply) configured on the source database",
        "license gate below cleared and evidenced in the site runbook",
        "Kafka Connect runtime with offsets + snapshot management",
    ),
    license_gate=(
        "CDC requires a separate IBM InfoSphere Data Replication (IIDR) license; "
        "IIDR installation itself is not required, but the license must be "
        "verified before enabling the Debezium connector. Batch extraction "
        "(default path) needs no such license."
    ),
    recommended_mode="batch",
    notes=(
        "The Debezium Db2 connector is stable, but its operational availability "
        "is a licensing question, not a technical one. Default to batch; the "
        "license gate must clear with evidence before any site flips to CDC."
    ),
    sources=(
        "artifact art_7DIRx9Nu §3 (C1) — IIDR license correction binds",
        "artifact art_HFsCu22Y §6 — Db2 LUW CDC/batch decision row",
    ),
)


def module() -> CdcModule:
    """The Db2 LUW CDC posture."""
    return DB2_LUW_CDC
