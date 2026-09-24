"""Agent-packet work partitioning for connector onboarding.

A packet is a hand-off contract: an independent agent gets a self-contained
brief with machine-run acceptance checks and returns a report in a fixed
schema. These tests pin the two dishonesty-proofing rules — the report is
bound to its packet, and a reported ``passed`` is only believed when the
verifier's own re-run of the check agrees.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

from connectors.packets import (
    CHECK_PYTEST_NODE,
    CHECK_PYTHON_IMPORT,
    CHECK_SOURCES_YML,
    PACKET_SCHEMA,
    REPORT_SCHEMA,
    PacketError,
    build_packet,
    run_check,
    validate_report,
    verify_packet,
    write_packet_directory,
)

REAL_SOURCE = "csvsftp_ridgeline"  # the seeded demo dealer, enabled: true


# --- packet construction -------------------------------------------------------


def test_build_packet_is_self_contained() -> None:
    packet = build_packet(REAL_SOURCE)
    assert packet["schema"] == PACKET_SCHEMA
    assert packet["packet_id"] == f"onboard-{REAL_SOURCE}"
    assert packet["source_id"] == REAL_SOURCE
    assert "task" in packet and "brief" in packet
    # Nothing is implied: the agent gets the guide, the inputs, the constraints,
    # and the exact acceptance checks verification will run.
    assert packet["inputs"]["connector_guide"] == "docs/CONNECTOR_GUIDE.md"
    assert packet["inputs"]["entities"]
    assert packet["report_schema"] == REPORT_SCHEMA
    assert packet["constraints"], "repo-wide constraints must be spelled out"
    assert [c["check_id"] for c in packet["acceptance_checks"]] == [
        "connector_imports",
        "registered_in_sources_yml",
        "extraction_plan_offline",
        "contract_suite_passes",
    ]
    # Credential discipline is a stated constraint, not an accident.
    assert any("No secrets" in c for c in packet["constraints"])


def test_build_packet_unknown_source_raises() -> None:
    with pytest.raises(PacketError):
        build_packet("not_in_sources_yml")


def test_packet_directory_carries_schema_doc_and_verifier(tmp_path: Path) -> None:
    target = write_packet_directory(build_packet(REAL_SOURCE), tmp_path)
    assert (target / "packet.json").exists()
    assert REPORT_SCHEMA in (target / "REPORT_SCHEMA.md").read_text(encoding="utf-8")
    verifier = target / "verify_report.py"
    assert verifier.exists()
    assert "verify_packet" in verifier.read_text(encoding="utf-8")
    round_tripped = json.loads((target / "packet.json").read_text(encoding="utf-8"))
    assert round_tripped["packet_id"] == f"onboard-{REAL_SOURCE}"


# --- report validation (the fixed schema) -----------------------------------------


def mini_packet() -> dict[str, Any]:
    """A hand-built packet with deterministic checks: two that pass, one that
    cannot (pytest on a test file that does not exist)."""
    return {
        "schema": PACKET_SCHEMA,
        "packet_id": "onboard-mini",
        "source_id": REAL_SOURCE,
        "acceptance_checks": [
            {
                "check_id": "import_ok",
                "kind": CHECK_PYTHON_IMPORT,
                "module": "connectors.base",
                "symbol": "BaseConnector",
            },
            {
                "check_id": "declared",
                "kind": CHECK_SOURCES_YML,
                "source_id": REAL_SOURCE,
                "expected_enabled": True,
            },
            {
                "check_id": "red_check",
                "kind": CHECK_PYTEST_NODE,
                "node": "tests/test__does_not_exist_zz.py",
            },
        ],
    }


def report_from(
    packet: dict[str, Any],
    status: str,
    overrides: dict[str, bool] | None = None,
) -> dict[str, Any]:
    checks = []
    for acceptance in packet["acceptance_checks"]:
        observation = run_check(acceptance)
        passed = observation.passed
        if overrides and observation.check_id in overrides:
            passed = overrides[observation.check_id]
        checks.append(
            {"check_id": observation.check_id, "passed": passed, "detail": observation.detail}
        )
    return {
        "schema": REPORT_SCHEMA,
        "packet_id": packet["packet_id"],
        "source_id": packet["source_id"],
        "status": status,
        "checks": checks,
        "artifacts": ["connectors/mini_impl.py"],
        "notes": "evidence-backed run",
        "agent": "test-agent",
        "completed_at": "2026-09-20T00:00:00Z",
    }


def test_valid_report_passes_structure() -> None:
    packet = mini_packet()
    assert validate_report(packet, report_from(packet, "blocked")) == []


@pytest.mark.parametrize(
    "mutation, expected_fragment",
    [
        ({"schema": "other/v1"}, "schema must be exactly"),
        ({"packet_id": "onboard-other"}, "does not bind to this packet"),
        ({"source_id": "other_source"}, "does not match"),
        ({"status": "shipped"}, "status must be one of"),
    ],
)
def test_report_schema_rejects_drift(mutation: dict[str, Any], expected_fragment: str) -> None:
    packet = mini_packet()
    report = report_from(packet, "blocked")
    report.update(mutation)
    problems = validate_report(packet, report)
    assert problems, f"expected a problem mentioning {expected_fragment!r}"
    assert any(expected_fragment in p for p in problems)


def test_missing_required_field_is_rejected() -> None:
    packet = mini_packet()
    report = report_from(packet, "blocked")
    del report["agent"]
    problems = validate_report(packet, report)
    assert any("missing required field" in p for p in problems)


def test_report_may_not_invent_or_drop_checks() -> None:
    packet = mini_packet()
    report = report_from(packet, "blocked")
    report["checks"] = report["checks"][:2]  # dropped red_check
    assert any("missing acceptance check" in p for p in validate_report(packet, report))

    invented = dict(
        report,
        checks=[*report["checks"], {"check_id": "i_invented_this", "passed": True, "detail": "x"}],
    )
    assert any("unexpected check id" in p for p in validate_report(packet, invented))

    duplicated = dict(report, checks=[*report["checks"], report["checks"][0]])
    assert any("duplicated check id" in p for p in validate_report(packet, duplicated))


def test_completed_requires_every_check_green() -> None:
    packet = mini_packet()
    report = report_from(packet, "completed")  # red_check honestly failed
    problems = validate_report(packet, report)
    assert any("requires every check passed=true" in p for p in problems)


def test_malformed_check_entries_are_rejected() -> None:
    packet = mini_packet()
    report = report_from(packet, "blocked")
    report["checks"][0]["passed"] = "yes"
    problems = validate_report(packet, report)
    assert any("boolean 'passed'" in p for p in problems)
    report["checks"][0].pop("detail")
    problems = validate_report(packet, report)
    assert any("string 'detail'" in p for p in problems)


# --- mechanical check runners --------------------------------------------------------


def test_python_import_check_accepts_base_connector() -> None:
    observation = run_check(
        {
            "check_id": "import_ok",
            "kind": CHECK_PYTHON_IMPORT,
            "module": "connectors.base",
            "symbol": "BaseConnector",
        }
    )
    assert observation.passed


def test_python_import_check_rejects_missing_symbol() -> None:
    observation = run_check(
        {
            "check_id": "import_bad",
            "kind": CHECK_PYTHON_IMPORT,
            "module": "connectors.base",
            "symbol": "NoSuchConnector",
        }
    )
    assert not observation.passed
    assert "not a BaseConnector subclass" in observation.detail


def test_sources_yml_check_enforces_declared_expectations() -> None:
    ok = run_check(
        {
            "check_id": "c",
            "kind": CHECK_SOURCES_YML,
            "source_id": REAL_SOURCE,
            "expected_enabled": True,
        }
    )
    assert ok.passed
    bad = run_check(
        {
            "check_id": "c",
            "kind": CHECK_SOURCES_YML,
            "source_id": REAL_SOURCE,
            "expected_enabled": False,
        }
    )
    assert not bad.passed
    unknown = run_check({"check_id": "c", "kind": CHECK_SOURCES_YML, "source_id": "ghost"})
    assert not unknown.passed


def test_unknown_check_kind_fails_closed() -> None:
    observation = run_check({"check_id": "c", "kind": "vibes"})
    assert not observation.passed
    assert "unknown check kind" in observation.detail


# --- verification: agreement between report and re-run --------------------------------


def test_honest_blocked_report_verifies() -> None:
    packet = mini_packet()
    outcome = verify_packet(packet, report_from(packet, "blocked"))
    assert outcome.verified, outcome.problems
    observations = {o.check_id: o for o in outcome.observations}
    assert not observations["red_check"].passed  # the verifier saw the same red


def test_lying_report_is_refuted_with_evidence() -> None:
    packet = mini_packet()
    liar = report_from(packet, "completed", overrides={"red_check": True})
    outcome = verify_packet(packet, liar)
    assert not outcome.verified
    disagreement = next(p for p in outcome.problems if "red_check" in p)
    assert "reported passed=True" in disagreement
    assert "verifier observed passed=False" in disagreement


def test_blocked_status_contradicting_all_green_checks_is_refused() -> None:
    packet = mini_packet()
    packet["acceptance_checks"] = packet["acceptance_checks"][:2]  # both pass
    outcome = verify_packet(packet, report_from(packet, "blocked"))
    assert not outcome.verified
    assert any("contradicts all-green" in p for p in outcome.problems)


def test_structurally_invalid_report_is_rejected_without_checks() -> None:
    packet = mini_packet()
    report = report_from(packet, "blocked")
    report["schema"] = "nope/v9"
    outcome = verify_packet(packet, report)
    assert not outcome.verified
    assert outcome.observations == []  # never ran mechanics on a malformed report


# --- the standalone verifier script (the machine-checkable boundary) -------------------


@pytest.fixture()
def packet_dir(tmp_path_factory: pytest.TempPathFactory) -> Any:
    """Packet directories must live under the repo root: the standalone
    verifier locates the repo by walking up from the packet directory."""
    repo_root = Path(__file__).resolve().parents[1]
    scratch = repo_root / ".tmp_packet_tests" / f"packet-{uuid.uuid4().hex[:8]}"
    target = write_packet_directory(build_packet(REAL_SOURCE), scratch)
    yield target
    shutil.rmtree(scratch.parent, ignore_errors=True)


def _honest_report_for_real_packet(packet: dict[str, Any]) -> dict[str, Any]:
    checks = []
    all_passed = True
    for acceptance in packet["acceptance_checks"]:
        observation = run_check(acceptance)
        all_passed = all_passed and observation.passed
        checks.append(
            {
                "check_id": observation.check_id,
                "passed": observation.passed,
                "detail": observation.detail,
            }
        )
    return {
        "schema": REPORT_SCHEMA,
        "packet_id": packet["packet_id"],
        "source_id": packet["source_id"],
        "status": "completed" if all_passed else "blocked",
        "checks": checks,
        "artifacts": [],
        "notes": "generated by the test harness from verifier-observed results",
        "agent": "pytest",
        "completed_at": "2026-09-20T00:00:00Z",
    }


def test_standalone_verifier_accepts_honest_report(packet_dir: Path) -> None:
    packet = json.loads((packet_dir / "packet.json").read_text(encoding="utf-8"))
    report = _honest_report_for_real_packet(packet)
    report_path = packet_dir / "report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    completed = subprocess.run(
        [
            sys.executable,
            str(packet_dir / "verify_report.py"),
            "--packet",
            str(packet_dir / "packet.json"),
            "--report",
            str(report_path),
        ],
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_standalone_verifier_rejects_tampered_report(packet_dir: Path) -> None:
    packet = json.loads((packet_dir / "packet.json").read_text(encoding="utf-8"))
    report = _honest_report_for_real_packet(packet)
    # Tamper: claim one check failed when the verifier re-runs it green.
    report["checks"][0]["passed"] = False
    report["status"] = "blocked"
    report_path = packet_dir / "report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    completed = subprocess.run(
        [
            sys.executable,
            str(packet_dir / "verify_report.py"),
            "--packet",
            str(packet_dir / "packet.json"),
            "--report",
            str(report_path),
        ],
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert completed.returncode == 1
    assert "reported passed=False" in completed.stdout
