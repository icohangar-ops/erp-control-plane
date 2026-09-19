"""Oracle CDC module — LogMiner default, XStream license gate (spec §6, C6)."""

from __future__ import annotations

from connectors.legacy.cdc import CdcModule

ORACLE_CDC = CdcModule(
    connector="oracle",
    engine="Oracle Database (LogMiner)",
    debezium_artifact="io.debezium:debezium-connector-oracle",
    debezium_maturity="stable",
    prerequisites=(
        "archive log mode enabled with supplemental logging on captured tables",
        "LogMiner retained for the capture window (SM/SCN retention)",
        "Kafka Connect runtime with offsets + snapshot management",
    ),
    license_gate=(
        "LogMiner-based capture needs no extra Oracle license. The XStream API "
        "alternative requires an Oracle GoldenGate license - verify the license "
        "position before selecting an XStream-based deployment."
    ),
    recommended_mode="batch",
    notes=(
        "Batch via watermark is the CI-tested default. LogMiner CDC is the "
        "license-safe CDC path when a hot entity justifies it."
    ),
    sources=(
        "artifact art_7DIRx9Nu section 3 (C6) - LogMiner/XStream license posture",
        "artifact art_HFsCu22Y section 6 - Oracle CDC/batch decision row",
    ),
)


def module() -> CdcModule:
    """The Oracle CDC posture."""
    return ORACLE_CDC
