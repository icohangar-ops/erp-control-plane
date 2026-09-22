#!/usr/bin/env python3
"""Evidence check: environment credentials stay out of the committed tree.

Backs the ``evidence/matrix.yaml`` row claiming no credentials are committed:
``.env`` is gitignored (``!.env.example`` re-includes the placeholder file),
and ``.env.example`` exists in the tree. Registry-level secret hygiene (templates
carry no literal secrets) is pinned separately by the registry test suite.
Stdlib-only, offline; exit 0 = verified, exit 1 = refused.
"""

from __future__ import annotations

import sys
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    gitignore = root / ".gitignore"
    env_example = root / ".env.example"
    if not gitignore.is_file():
        print(f"FAIL: .gitignore not found: {gitignore}")
        return 1
    lines = [line.strip() for line in gitignore.read_text(encoding="utf-8").splitlines()]
    if ".env" not in lines:
        print("FAIL: .gitignore does not ignore .env")
        return 1
    if "!.env.example" not in lines:
        print("FAIL: .gitignore does not re-include .env.example")
        return 1
    if not env_example.is_file():
        print(f"FAIL: placeholder env file missing: {env_example}")
        return 1
    print("OK: .env gitignored, .env.example placeholder file present")
    return 0


if __name__ == "__main__":
    sys.exit(main())
