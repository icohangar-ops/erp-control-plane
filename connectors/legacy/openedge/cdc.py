"""OpenEdge CDC module — native OpenEdge CDC / Pro2 pilot option (C2)."""

from __future__ import annotations

from connectors.legacy.cdc import CdcModule

OPENEDGE_CDC = CdcModule(
    connector="openedge",
    engine="Progress OpenEdge (native CDC / Pro2)",
    debezium_artifact=None,
    debezium_maturity="not applicable - no Debezium connector exists",
    prerequisites=(
        "native OpenEdge CDC (12.2+ captures create/update/delete) or Pro2 "
        "replication configured and site-licensed",
        "a CDC consumer target the control plane can read (SQL Server, Oracle, "
        "or another OpenEdge database per Pro2's supported targets)",
        "Kafka Connect runtime only if a bridge producer is added on top",
    ),
    vendor_alternative=(
        "native OpenEdge CDC / Pro2 replication (site-licensed) - the only CDC "
        "route; there is no Debezium connector for OpenEdge"
    ),
    recommended_mode="batch",
    notes=(
        "Batch only per spec §6. Pro2 replication lands changes in a second "
        "database, so CDC here means reading Pro2's target, not the ERP primary. "
        "Treat as a site-licensed pilot option, not a default."
    ),
    sources=(
        "artifact art_7DIRx9Nu section 3 (C2) - OpenEdge custom ODBC/dlt "
        "extraction and native CDC/Pro2 posture",
        "artifact art_HFsCu22Y section 6 - OpenEdge CDC/batch decision row",
    ),
)


def module() -> CdcModule:
    """The OpenEdge CDC posture."""
    return OPENEDGE_CDC
