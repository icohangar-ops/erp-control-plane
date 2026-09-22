#!/usr/bin/env python3
"""Evidence check: the README connector-maturity table matches implementation state.

Backs the ``evidence/matrix.yaml`` row claiming the README maturity table does
not drift from the tree: a connector implemented as a real class (its module
never references ``SkeletonConnector``) must be marked working/coded, and a
documented skeleton must be marked as one. Stdlib-only, offline; exit 0 =
verified, exit 1 = refused.
"""

from __future__ import annotations

import sys
from pathlib import Path

ERP_TO_CONNECTOR_DIR = {
    "Generic CSV/SFTP": "csv_sftp",
    "NetSuite": "netsuite",
    "BisTrack": "bistrack",
    "DMSi Agility": "dmsi_agility",
    "Epicor Prophet 21": "epicor_p21",
    "Epicor Eclipse": "epicor_eclipse",
    "ECI Spruce / RockSolid MAX": "eci_spruce",
    "Dynamics 365 BC": "d365_bc",
}
SKELETON_MARKER = "SkeletonConnector"
SKELETON_STATUS = "📝"
CODED_STATUS = ("⚙️", "✅")


def parse_maturity_rows(readme: str) -> dict[str, str]:
    """ERP label → status cell for the '## Connector maturity' table."""
    rows: dict[str, str] = {}
    in_section = False
    for line in readme.splitlines():
        if line.startswith("## "):
            in_section = line.strip() == "## Connector maturity (first wave)"
            continue
        if in_section and line.startswith("| ") and not line.startswith("| ERP"):
            cells = [cell.strip() for cell in line.split("|")[1:-1]]
            if len(cells) == 3:
                rows[cells[0]] = cells[2]
    return rows


def connector_is_skeleton(connectors_dir: Path, connector: str) -> bool:
    """True when any Python module in the connector package is a skeleton."""
    package = connectors_dir / connector
    return any(SKELETON_MARKER in py.read_text(encoding="utf-8") for py in package.glob("*.py"))


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    readme_path = root / "README.md"
    if not readme_path.is_file():
        print(f"FAIL: README not found: {readme_path}")
        return 1
    rows = parse_maturity_rows(readme_path.read_text(encoding="utf-8"))
    expected_erp = set(ERP_TO_CONNECTOR_DIR)
    if set(rows) != expected_erp:
        print(f"FAIL: maturity table ERPs {sorted(rows)} != expected {sorted(expected_erp)}")
        return 1
    failures: list[str] = []
    for erp, status in sorted(rows.items()):
        connector = ERP_TO_CONNECTOR_DIR[erp]
        is_skeleton = connector_is_skeleton(root / "connectors", connector)
        if is_skeleton and SKELETON_STATUS not in status:
            failures.append(
                f"{erp}: implemented as {SKELETON_MARKER} but README status is {status!r}"
            )
        if not is_skeleton:
            if not any(mark in status for mark in CODED_STATUS):
                failures.append(f"{erp}: coded connector but README status is {status!r}")
            if "skeleton" in status.lower():
                failures.append(
                    f"{erp}: coded connector but README calls it a skeleton: {status!r}"
                )
    if failures:
        print("FAIL: README maturity table drifted from implementation state:")
        for failure in failures:
            print(f"  {failure}")
        return 1
    print(f"OK: all {len(rows)} maturity rows match implementation state (skeleton vs coded)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
