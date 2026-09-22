#!/usr/bin/env python3
"""Evidence check: the seeded dealer dataset is exactly 5,004 manifest-pinned rows.

Backs the ``evidence/matrix.yaml`` row claiming ``seed/dealer_export`` holds
5,004 data rows across nine domains, every file's SHA-256 matches the committed
manifest checksum, and every parsed row count matches the manifest's recorded
count. Stdlib-only, offline; exit 0 = verified, exit 1 = refused.
"""

from __future__ import annotations

import csv
import hashlib
import json
import sys
from pathlib import Path

EXPECTED_TOTAL_ROWS = 5004


def verify_seed_dir(seed_dir: Path) -> int:
    manifest_path = seed_dir / "manifest.json"
    if not manifest_path.is_file():
        print(f"FAIL: seed manifest not found: {manifest_path}")
        return 1
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    total = 0
    for entry in manifest["files"]:
        path = seed_dir / str(entry["name"])
        if not path.is_file():
            print(f"FAIL: manifest references a missing file: {entry['name']}")
            return 1
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != entry["sha256"]:
            print(f"FAIL: checksum mismatch for {entry['name']}")
            return 1
        with path.open(newline="", encoding="utf-8") as handle:
            rows = sum(1 for _ in csv.reader(handle)) - 1
        if rows != entry["rows"]:
            print(f"FAIL: {entry['name']} has {rows} data rows, manifest records {entry['rows']}")
            return 1
        total += rows
    if total != EXPECTED_TOTAL_ROWS:
        print(f"FAIL: {total} seed rows across domains, expected {EXPECTED_TOTAL_ROWS}")
        return 1
    print(f"OK: {total} seed rows verified against manifest checksums and row counts")
    return 0


def main() -> int:
    seed_dir = Path(__file__).resolve().parents[2] / "seed" / "dealer_export"
    return verify_seed_dir(seed_dir)


if __name__ == "__main__":
    sys.exit(main())
