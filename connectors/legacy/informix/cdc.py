"""Informix CDC module — optional change-streams path (spec §6, C4 posture).

Documented, not CI-required: spec §5 row 1 makes JDBC/ODBC batch via watermark
the primary path; Debezium change-streams are the optional CDC module.

Attribution (all-flavors spec §5): the Change Streams API for Java is
CLIENT-SIDE — it ships with the Informix JDBC installation (Maven Central
com.ibm.informix:ifx-changestream-client) and rides the Kafka Connect
classpath. The server-side prerequisites live in the engine: syscdcv1.sql,
full-row logging, and capture mode over the logical log.
"""

from __future__ import annotations

from connectors.legacy.cdc import CdcModule

INFORMIX_CDC = CdcModule(
    connector="informix",
    engine="IBM Informix (server-side Change Data Capture API on the logical log)",
    debezium_artifact="io.debezium:debezium-connector-informix",
    debezium_maturity="incubating",
    prerequisites=(
        "Debezium 3.6.x ships JDBC driver v15 — use it for any Informix 14/15 "
        "target (per C4, the spec's 12.x/driver-v15 flip condition is stale: "
        "Informix 12.x works with driver v15 in practice)",
        "SERVER-SIDE engine prerequisites: syscdcv1.sql installed as user "
        "informix from $INFORMIXDIR/etc, full row logging enabled on captured "
        "tables (cdc_set_fullrowlogging), and captured tables placed in "
        "capture mode",
        "CLIENT-SIDE library: the Informix Change Streams API for Java ships "
        "with the Informix JDBC installation (Maven Central "
        "com.ibm.informix:ifx-changestream-client) and must be added to the "
        "Kafka Connect plugin directory — it is not bundled with the "
        "connector archive (licensing) and is not an engine feature",
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
        "artifact art_wGXFbs3x §5 — Change Streams API is client-side (ships "
        "with the JDBC installation; ifx-changestream-client); syscdcv1.sql + "
        "full-row logging + capture mode are the server-side prerequisites",
    ),
)


def module() -> CdcModule:
    """The Informix CDC posture."""
    return INFORMIX_CDC
