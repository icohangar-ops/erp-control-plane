"""Evidence-matrix gate tests: the matrix must verify, and must refuse tampering.

These tests are the verifying evidence for ``evidence/matrix.yaml`` rows C009-C011:
the vendored verifier runs clean over the real matrix, refuses every refusal rule
(in the test path, per the task brief — not via red CI runs), is byte-identical to
the canonical kit copy, and is stdlib-only with no skip flags.

The verifier resolves evidence refs against the tampered matrix's grandparent
directory (``tree_root``: ``matrix_path.parent.parent`` — the canonical
``evidence/matrix.yaml`` layout). Tamper cases therefore write beside the real
matrix so refs still resolve against this repo root; a fixture guarantees cleanup.
"""

from __future__ import annotations

import ast
import hashlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
VERIFIER = ROOT / "tools" / "verify_evidence_matrix.py"
MATRIX = ROOT / "evidence" / "matrix.yaml"
TAMPER_MATRIX = ROOT / "evidence" / ".tamper_tmp_matrix.yaml"

# Canonical: icohangar-ops/consensus-hardening-protocol tools/verify_evidence_matrix.py
# at kit commit 88067e4 (v1.0.0).
CANONICAL_SHA256 = "238e02ab19d59e8b4dc2f6cc5f7ddf099f17a32079b1bc62aa9fcbafbfd297e8"

# Every module the vendored verifier may import (yaml optional at import time).
ALLOWED_VERIFIER_IMPORTS = {
    "argparse",
    "hashlib",
    "json",
    "os",
    "subprocess",
    "sys",
    "dataclasses",
    "pathlib",
    "typing",
    "yaml",
}


def run_verifier(*extra_args: str, cwd: Path = ROOT) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(VERIFIER), *extra_args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )


@pytest.fixture
def tampered_matrix() -> Any:
    """Apply one textual replacement to the matrix and run the verifier on it."""

    def _tamper(old: str, new: str) -> subprocess.CompletedProcess[str]:
        text = MATRIX.read_text(encoding="utf-8")
        assert old in text, f"tamper anchor not found in matrix: {old!r}"
        TAMPER_MATRIX.write_text(text.replace(old, new, 1), encoding="utf-8")
        return run_verifier("--matrix", str(TAMPER_MATRIX))

    try:
        yield _tamper
    finally:
        TAMPER_MATRIX.unlink(missing_ok=True)


def test_matrix_verifies_clean() -> None:
    result = run_verifier()
    assert result.returncode == 0, result.stdout + result.stderr
    assert "EVIDENCE MATRIX: VERIFIED (23/23)" in result.stdout


def test_missing_manifest_refuses() -> None:
    result = run_verifier("--matrix", "/tmp/no-such-matrix.yaml")
    assert result.returncode == 2
    # Unusable-path verdicts print to stderr (verified output, not assertion text).
    assert "EVIDENCE MATRIX: UNUSABLE" in result.stdout + result.stderr
    assert "no evidence matrix found" in result.stdout + result.stderr


def test_tampered_hash_refuses(tampered_matrix: Any) -> None:
    result = tampered_matrix(CANONICAL_SHA256, "0" * 64)
    assert result.returncode == 1
    assert "artifact hash mismatch" in result.stdout


def test_unknown_evidence_type_refuses(tampered_matrix: Any) -> None:
    result = tampered_matrix("type: script", "type: oracle")
    assert result.returncode == 1
    assert "unknown evidence type" in result.stdout


def test_zero_evidence_row_refuses(tampered_matrix: Any) -> None:
    empty_evidence = (
        "    evidence:\n"
        "      - type: test\n"
        '        ref: "tests/test_csv_sftp.py::test_reextract_is_idempotent_by_content_hash"\n'
    )
    result = tampered_matrix(empty_evidence, "    evidence: []\n")
    assert result.returncode == 1
    assert "zero-evidence row" in result.stdout


def test_duplicate_claim_id_refuses(tampered_matrix: Any) -> None:
    result = tampered_matrix("- id: C023", "- id: C020")
    assert result.returncode == 1
    assert "duplicate claim id" in result.stdout


def test_missing_source_locator_refuses(tampered_matrix: Any) -> None:
    result = tampered_matrix(
        'source: "README.md#security-notes"', 'source: "docs/DOES_NOT_EXIST.md"'
    )
    assert result.returncode == 1
    assert "source locator points at a file that does not exist" in result.stdout


def test_schema_version_bump_refuses(tampered_matrix: Any) -> None:
    result = tampered_matrix("schema_version: 1", "schema_version: 2")
    assert result.returncode == 2
    assert "unsupported schema_version 2" in result.stdout + result.stderr


def test_verifier_byte_identical_to_canonical() -> None:
    digest = hashlib.sha256(VERIFIER.read_bytes()).hexdigest()
    assert digest == CANONICAL_SHA256, (
        f"vendored verifier drifted from canonical kit v1.0.0: {digest} != {CANONICAL_SHA256}"
    )


def test_verifier_is_stdlib_only() -> None:
    tree = ast.parse(VERIFIER.read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            imported.add(node.module.split(".")[0])
    unexpected = imported - ALLOWED_VERIFIER_IMPORTS
    assert not unexpected, f"verifier imports outside the stdlib allowlist: {sorted(unexpected)}"


def test_verifier_has_no_skip_or_quiet_flags() -> None:
    result = run_verifier("--help")
    assert result.returncode == 0
    # The description sentence mentions the no-skip policy by name; only the
    # generated options list may not contain skip/quiet modes.
    options_section = result.stdout.split("options:", 1)[-1]
    lowered = options_section.lower()
    assert "skip" not in lowered and "quiet" not in lowered, "verifier grew a skip/quiet mode"


def test_matrix_parses_identically_under_both_parsers() -> None:
    yaml = pytest.importorskip("yaml")
    spec = importlib.util.spec_from_file_location("vendored_verifier", VERIFIER)
    assert spec is not None and spec.loader is not None
    module: Any = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    text = MATRIX.read_text(encoding="utf-8")
    pyyaml_parse: Any = yaml.safe_load(text)
    fallback_parse: Any = module.parse_yaml_subset(text)
    assert json.dumps(pyyaml_parse, sort_keys=True) == json.dumps(fallback_parse, sort_keys=True), (
        "PyYAML and the stdlib fallback parser disagree on evidence/matrix.yaml"
    )
