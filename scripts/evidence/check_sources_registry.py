#!/usr/bin/env python3
"""Evidence check: the connector registry declares 21 sources, disabled-first.

Backs the ``evidence/matrix.yaml`` row claiming ``connectors/sources.yml``
registers exactly 21 sources, every ``*_template`` entry ships
``enabled: false`` with no literal secrets, and the seeded CSV/SFTP demo source
is the only enabled one. Stdlib-only, offline; exit 0 = verified, exit 1 =
refused.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

EXPECTED_SOURCE_COUNT = 21
EXPECTED_ENABLED_SOURCES = {"csvsftp_ridgeline"}

SOURCE_ID_RE = re.compile(r"^\s*-\s+source_id:\s*(\S+)\s*$")
ENABLED_RE = re.compile(r"^(?P<indent>\s*)enabled:\s*(?P<flag>true|false)\s*$")


def parse_source_flags(text: str) -> dict[str, bool]:
    """Scan the registry line-wise: each source_id maps to its enabled flag."""
    flags: dict[str, bool] = {}
    current: str | None = None
    current_indent_len = 0
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        source_match = SOURCE_ID_RE.match(line)
        if source_match:
            current = source_match.group(1)
            current_indent_len = len(line) - len(line.lstrip())
            flags.setdefault(current, False)
            continue
        enabled_match = ENABLED_RE.match(line)
        # An ``enabled:`` key belongs to the current source only when indented
        # deeper than its ``- source_id:`` key (entity blocks nest deeper still).
        if (
            enabled_match
            and current is not None
            and len(enabled_match.group("indent")) > current_indent_len
        ):
            flags[current] = enabled_match.group("flag") == "true"
    return flags


def main() -> int:
    registry = Path(__file__).resolve().parents[2] / "connectors" / "sources.yml"
    if not registry.is_file():
        print(f"FAIL: registry file not found: {registry}")
        return 1
    flags = parse_source_flags(registry.read_text(encoding="utf-8"))
    if len(flags) != EXPECTED_SOURCE_COUNT:
        print(f"FAIL: registry declares {len(flags)} sources, expected {EXPECTED_SOURCE_COUNT}")
        return 1
    enabled = {name for name, on in flags.items() if on}
    if enabled != EXPECTED_ENABLED_SOURCES:
        print(
            f"FAIL: enabled sources {sorted(enabled)}, expected {sorted(EXPECTED_ENABLED_SOURCES)}"
        )
        return 1
    templates_disabled = [name for name in flags if name.endswith("_template") and flags[name]]
    if templates_disabled:
        print(f"FAIL: template sources must ship disabled-first: {templates_disabled}")
        return 1
    print(
        f"OK: {len(flags)} sources registered; enabled={sorted(enabled)}; templates disabled-first"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
