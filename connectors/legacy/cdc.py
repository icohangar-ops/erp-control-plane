"""Shared machinery for the documented CDC modules (spec §5/§6).

Default posture across the pack is batch-first (spec §6): CDC earns its
operational weight (Kafka Connect runtime, offsets, snapshot management) only
for hot entities, and several legacy engines have no Debezium connector at all.
Each connector that *has* a CDC story ships a ``cdc.py`` module declaring a
:class:`CdcModule` — the verified posture (artifact art_7DIRx9Nu, September 19,
2026) surfaced to ops as data: artifact line and maturity, prerequisites, and
the license gate where one exists. CDC modules are documentation with a stable
surface — they are deliberately NOT wired into CI extraction loads.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CdcModule:
    """The verified CDC posture for one legacy source class."""

    connector: str
    engine: str
    #: Debezium source connector artifact, when one exists (e.g.
    #: "io.debezium:debezium-connector-db2"). None = no Debezium connector.
    debezium_artifact: str | None
    #: Verified maturity of that artifact line (e.g. "stable", "incubating").
    debezium_maturity: str
    #: Site prerequisites validated during the September 19, 2026 research.
    prerequisites: tuple[str, ...] = ()
    #: License gate that must clear before CDC goes live, if any.
    license_gate: str | None = None
    #: Non-Debezium vendor CDC alternative, when verified to exist.
    vendor_alternative: str | None = None
    #: Spec §6 default posture for this class.
    recommended_mode: str = "batch"
    notes: str = ""
    #: Artifacts/retrievals backing every claim (the inventory's source register).
    sources: tuple[str, ...] = field(default_factory=tuple)

    def plan(self) -> dict[str, object]:
        """Machine-readable posture for dry-runs, docs, and ops tooling."""
        return {
            "connector": self.connector,
            "engine": self.engine,
            "debezium_artifact": self.debezium_artifact,
            "debezium_maturity": self.debezium_maturity,
            "prerequisites": list(self.prerequisites),
            "license_gate": self.license_gate,
            "vendor_alternative": self.vendor_alternative,
            "recommended_mode": self.recommended_mode,
            "notes": self.notes,
            "sources": list(self.sources),
        }
