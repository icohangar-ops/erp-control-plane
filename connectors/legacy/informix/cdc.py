"""Informix CDC module — optional change-streams path (spec §6, C4 posture).

Documented, not CI-required: spec §5 row 1 makes JDBC/ODBC batch via watermark
the primary path; Debezium change-streams are the optional CDC module.
"""

from __future__ import annotations

from connectors.legacy.cdc import CdcModule

INFORMIX_CDC = CdcModule(
    connector="informix",
    engine="IBM Informix (Change Streams API)",
    debezium_artifact="io.debezium:debezium-connector-informix",
    debezium_maturity="incubating",
    prerequisites=(
        "Debezium 3.6.x ships JDBC driver v15 — use it for any Informix 14/15 "
        "target (per C4, the spec's 12.x/driver-v15 flip condition is stale: "
        "Informix 12.x works with driver v15 in practice)",
        "full row logging enabled on captured tables",
        "Change Streams API available on the target engine",
        "Kafka Connect runtime with offsets + snapshot management",
    ),
    recommended_mode="batch",
    notes=(
        "Batch via watermark is the default and the CI-tested path. Enable the "
        "change-streams module only where a hot entity justifies a Kafka Connect "
        "deployment; monitor the incubating artifact's release notes before "
        "promoting it beyond a pilot."
    ),
    sources=(
        "artifact art_7DIRx9Nu §3 (C4) — Debezium Informix incubating; driver "
        "v15 posture; 12.x works in practice; stall bug fixed",
        "artifact art_HFsCu22Y §6 — Informix CDC/batch decision row",
    ),
)


def module() -> CdcModule:
    """The Informix CDC posture."""
    return INFORMIX_CDC
