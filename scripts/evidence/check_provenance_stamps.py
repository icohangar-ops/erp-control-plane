#!/usr/bin/env python3
"""Evidence check: every canonical fact model stamps provenance columns.

Backs the ``evidence/matrix.yaml`` row claiming every canonical fact row is
provenance-stamped: ``source_system`` plus ``loaded_at``, and a document/line
or source-key locator, in each ``dbt/models/canonical/fact_*.sql``. Stdlib-only,
offline; exit 0 = verified, exit 1 = refused.
"""

from __future__ import annotations

import sys
from pathlib import Path

REQUIRED_COLUMNS = ("source_system", "loaded_at")
LOCATOR_COLUMNS = ("source_doc_no", "source_line_no", "source_key")


def main() -> int:
    canonical_dir = Path(__file__).resolve().parents[2] / "dbt" / "models" / "canonical"
    fact_models = sorted(canonical_dir.glob("fact_*.sql"))
    if not fact_models:
        print(f"FAIL: no canonical fact models found under {canonical_dir}")
        return 1
    failures: list[str] = []
    for model in fact_models:
        text = model.read_text(encoding="utf-8")
        missing = [name for name in REQUIRED_COLUMNS if name not in text]
        has_locator = any(name in text for name in LOCATOR_COLUMNS)
        if not has_locator:
            missing.append("source_doc_no|source_line_no|source_key")
        if missing:
            failures.append(f"{model.name}: missing {missing}")
    if failures:
        print("FAIL: provenance stamping incomplete:")
        for failure in failures:
            print(f"  {failure}")
        return 1
    print(f"OK: {len(fact_models)} canonical fact models carry the provenance stamp columns")
    return 0


if __name__ == "__main__":
    sys.exit(main())
