"""Agent-packet work partitioning for connector onboarding ([Data] P4;
stackoverflow-community-radar build_kg.py reference).

Connector onboarding is handed to independent agents — one packet per
source, no shared state, no hallway conversations. A packet is therefore
**self-contained**: it carries the task brief, the exact repo inputs, the
constraints that must hold on return, a FIXED report schema, and the
acceptance checks a verifier executes mechanically. The onboarding agent's
output is machine-checkable: either ``verify_packet`` passes on the report,
or it names the failed check — an agent cannot talk its way past the suite.

Two honesty rules mirror the control plane's contract philosophy:

- The report is bound to the packet (``packet_id`` + ``source_id``) and to
  the packet's exact acceptance-check id set — no invented checks, no
  silently skipped ones.
- A reported ``passed`` is only believed when the verifier's own run of that
  check agrees. Reporting green on a red check fails verification.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any

from connectors.base import BaseConnector, ConnectorMaturity
from connectors.registry import build_registry_connectors, load_source_configs
from control_plane.models import SourceConfig

# --- scheme constants --------------------------------------------------------

PACKET_SCHEMA = "cubiczan-connector-onboarding-packet/v1"
REPORT_SCHEMA = "cubiczan-connector-onboarding-report/v1"

REPORT_STATUS_COMPLETED = "completed"
REPORT_STATUS_BLOCKED = "blocked"
REPORT_STATUSES = frozenset({REPORT_STATUS_COMPLETED, REPORT_STATUS_BLOCKED})

# Acceptance-check kinds — each maps to a mechanical runner in run_check().
CHECK_PYTHON_IMPORT = "python_import"
CHECK_SOURCES_YML = "sources_yml_entry"
CHECK_DESCRIBE_EXTRACTION = "describe_extraction"
CHECK_PYTEST_NODE = "pytest_node"
CHECK_KINDS = frozenset(
    {CHECK_PYTHON_IMPORT, CHECK_SOURCES_YML, CHECK_DESCRIBE_EXTRACTION, CHECK_PYTEST_NODE}
)

REPORT_FIELDS = (
    "schema",
    "packet_id",
    "source_id",
    "status",
    "checks",
    "artifacts",
    "notes",
    "agent",
    "completed_at",
)


class PacketError(Exception):
    """Packet construction failed (unknown source, registry error)."""


# --- packet construction ------------------------------------------------------


def build_packet(source_id: str, *, created_by: str = "control-plane") -> dict[str, Any]:
    """Assemble a self-contained onboarding packet for one source.

    Everything an independent agent needs is embedded: the guide to read, the
    entities to stage, the constraints that hold repo-wide, and the exact
    acceptance checks verification will run. Nothing is implied by "context".
    """
    source = _find_source(source_id)
    connector = _connector_for(source_id)
    class_ref = f"{type(connector).__module__}.{type(connector).__name__}"
    module = type(connector).__module__
    entities = connector.entities()
    return {
        "schema": PACKET_SCHEMA,
        "packet_id": f"onboard-{source_id}",
        "source_id": source_id,
        "erp": source.erp,
        "created_by": created_by,
        "created_at": dt.datetime.now(dt.UTC).isoformat(),
        "maturity_now": connector.maturity.value,
        "task": (
            f"Bring the {source_id} connector to {ConnectorMaturity.IMPLEMENTED.value} "
            f"maturity and make the full contract suite pass."
        ),
        "brief": (
            f"{source.description}\n\n"
            "Implement the extraction surface on the connector class "
            f"({class_ref}) per docs/CONNECTOR_GUIDE.md: validate_config(), "
            "_iter_records(), and any per-tenant settings. Staging records are "
            "canonical-shaped dicts; provenance columns are stamped by the base "
            "class and must not be set manually. Keep `enabled: false` in "
            "sources.yml until live per-tenant discovery is done — credential "
            "discovery is a human step, never an agent guess."
        ),
        "inputs": {
            "connector_guide": "docs/CONNECTOR_GUIDE.md",
            "base_class": "connectors/base.py",
            "sources_yml": "connectors/sources.yml",
            "connector_module": module,
            "connector_class": type(connector).__name__,
            "entities": list(entities),
            "default_entities": list(source.default_entities),
            "current_settings_keys": sorted(source.settings.keys()),
        },
        "constraints": [
            "No secrets, tokens, or credentials in code, fixtures, or reports — "
            "settings resolve from the environment (${VAR} interpolation).",
            "No network calls in the test suite; fixture-recorded behavior only.",
            "sources.yml entry keeps enabled: false until live discovery passes.",
            "The full contract suite (tests/test_connector_contract.py) must pass.",
        ],
        "report_schema": REPORT_SCHEMA,
        "acceptance_checks": [
            {
                "check_id": "connector_imports",
                "kind": CHECK_PYTHON_IMPORT,
                "module": module,
                "symbol": type(connector).__name__,
                "erp_id": source.erp,
            },
            {
                "check_id": "registered_in_sources_yml",
                "kind": CHECK_SOURCES_YML,
                "source_id": source_id,
                "expected_maturity": ConnectorMaturity.IMPLEMENTED.value,
                "expected_enabled": False,
            },
            {
                "check_id": "extraction_plan_offline",
                "kind": CHECK_DESCRIBE_EXTRACTION,
                "source_id": source_id,
                "entities": list(entities),
            },
            {
                "check_id": "contract_suite_passes",
                "kind": CHECK_PYTEST_NODE,
                "node": "tests/test_connector_contract.py",
                "keyword": source_id,
            },
        ],
    }


def write_packet_directory(packet: dict[str, Any], out_dir: Path) -> Path:
    """Materialize the packet as a self-contained directory.

    Layout (stackoverflow-community-radar build_kg.py pattern)::

        <out>/<packet_id>/
            packet.json         # the packet itself
            REPORT_SCHEMA.md    # the fixed report schema, in prose
            verify_report.py    # standalone verifier: report.json -> exit 0/1
    """
    packet_id = packet["packet_id"]
    target = Path(out_dir) / packet_id
    target.mkdir(parents=True, exist_ok=True)
    (target / "packet.json").write_text(
        json.dumps(packet, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    (target / "REPORT_SCHEMA.md").write_text(REPORT_SCHEMA_DOC, encoding="utf-8")
    (target / "verify_report.py").write_text(VERIFY_REPORT_SCRIPT, encoding="utf-8")
    return target


# --- report validation ----------------------------------------------------------


def validate_report(packet: dict[str, Any], report: dict[str, Any]) -> list[str]:
    """Structural validation against the fixed report schema.

    Returns a list of problems (empty = structurally valid). The report is
    never "verified" unless it is first structurally valid — a malformed
    report is a rejection, not a failed check.
    """
    problems: list[str] = []
    if report.get("schema") != REPORT_SCHEMA:
        problems.append(f"schema must be exactly {REPORT_SCHEMA!r}")
    for absent in (f for f in REPORT_FIELDS if f not in report):
        problems.append(f"missing required field {absent!r}")
    if problems:
        return problems  # field-set failures make the rest noisy, not clearer
    if report["packet_id"] != packet["packet_id"]:
        problems.append(
            f"packet_id {report['packet_id']!r} does not bind to this packet "
            f"({packet['packet_id']!r})"
        )
    if report["source_id"] != packet["source_id"]:
        problems.append(f"source_id {report['source_id']!r} does not match {packet['source_id']!r}")
    if report["status"] not in REPORT_STATUSES:
        problems.append(f"status must be one of {sorted(REPORT_STATUSES)}")

    checks = report["checks"]
    if not isinstance(checks, list):
        problems.append("checks must be a list")
        return problems
    expected_ids = [c["check_id"] for c in packet["acceptance_checks"]]
    reported_ids: list[str] = []
    for check in checks:
        if not isinstance(check, dict) or not isinstance(check.get("check_id"), str):
            problems.append(f"malformed check entry: {check!r}")
            continue
        reported_ids.append(check["check_id"])
        if not isinstance(check.get("passed"), bool):
            problems.append(f"check {check['check_id']!r} needs a boolean 'passed'")
        if not isinstance(check.get("detail"), str):
            problems.append(f"check {check['check_id']!r} needs a string 'detail'")
    extra = sorted(set(reported_ids) - set(expected_ids))
    missing = sorted(set(expected_ids) - set(reported_ids))
    if extra:
        problems.append(f"unexpected check id(s): {', '.join(extra)}")
    if missing:
        problems.append(f"missing acceptance check(s): {', '.join(missing)}")
    duplicated = sorted({cid for cid in reported_ids if reported_ids.count(cid) > 1})
    if duplicated:
        problems.append(f"duplicated check id(s): {', '.join(duplicated)}")

    all_passed = all(isinstance(c, dict) and c.get("passed") is True for c in checks)
    if report["status"] == REPORT_STATUS_COMPLETED and not all_passed:
        problems.append("status 'completed' requires every check passed=true")
    return problems


# --- mechanical check runners ----------------------------------------------------


@dataclass(frozen=True)
class CheckObservation:
    check_id: str
    passed: bool
    detail: str


def run_check(check: dict[str, Any]) -> CheckObservation:
    """Execute one acceptance check mechanically. No trust, only evidence."""
    kind = check.get("kind")
    check_id = check.get("check_id", "<unnamed>")
    try:
        if kind == CHECK_PYTHON_IMPORT:
            module = import_module(check["module"])
            symbol = getattr(module, check["symbol"], None)
            if symbol is None or not (
                isinstance(symbol, type) and issubclass(symbol, BaseConnector)
            ):
                return CheckObservation(
                    check_id,
                    False,
                    f"{check['module']}.{check['symbol']} is not a BaseConnector subclass",
                )
            erp_id = getattr(symbol, "erp_id", None)
            expected_erp = check.get("erp_id")
            if expected_erp and erp_id != expected_erp:
                return CheckObservation(
                    check_id, False, f"erp_id {erp_id!r} != expected {expected_erp!r}"
                )
            return CheckObservation(
                check_id, True, f"{check['module']}.{check['symbol']} imports as a BaseConnector"
            )
        if kind == CHECK_SOURCES_YML:
            source = _find_source(check["source_id"])
            expected_maturity = check.get("expected_maturity")
            if expected_maturity:
                actual = _declared_maturity(source.source_id).value
                if actual != expected_maturity:
                    return CheckObservation(
                        check_id, False, f"maturity {actual!r} != expected {expected_maturity!r}"
                    )
            if "expected_enabled" in check and source.enabled != check["expected_enabled"]:
                return CheckObservation(
                    check_id,
                    False,
                    f"enabled={source.enabled!r} != expected {check['expected_enabled']!r}",
                )
            return CheckObservation(
                check_id, True, f"sources.yml declares {source.source_id} as expected"
            )
        if kind == CHECK_DESCRIBE_EXTRACTION:
            connector = _connector_for(check["source_id"])
            plans = []
            for entity in check["entities"]:
                plan = connector.describe_extraction(entity)
                plans.append(f"{entity}: {plan.surface}")
            return CheckObservation(
                check_id, True, "; ".join(plans) + " (offline describe_extraction)"
            )
        if kind == CHECK_PYTEST_NODE:
            command = [sys.executable, "-m", "pytest", check["node"], "-q"]
            keyword = check.get("keyword")
            if keyword:
                command += ["-k", keyword]
            completed = subprocess.run(command, check=False, capture_output=True, text=True)
            tail = (completed.stdout or completed.stderr).strip().splitlines()
            detail = tail[-1] if tail else f"exit code {completed.returncode}"
            return CheckObservation(check_id, completed.returncode == 0, detail)
    except PacketError as exc:
        return CheckObservation(check_id, False, str(exc))
    except Exception as exc:
        return CheckObservation(check_id, False, f"check crashed: {exc}")
    return CheckObservation(check_id, False, f"unknown check kind {kind!r}")


@dataclass(frozen=True)
class VerificationOutcome:
    """The verifier's verdict: structural validity + agreement per check."""

    verified: bool
    problems: list[str]
    observations: list[CheckObservation]

    def to_dict(self) -> dict[str, Any]:
        return {
            "verified": self.verified,
            "problems": self.problems,
            "observations": [
                {
                    "check_id": o.check_id,
                    "passed": o.passed,
                    "detail": o.detail,
                }
                for o in self.observations
            ],
        }


def verify_packet(packet: dict[str, Any], report: dict[str, Any]) -> VerificationOutcome:
    """Verify an onboarding report against its packet.

    Verified = structurally valid AND the packet's acceptance checks were
    actually run by this verifier AND every reported pass/fail agrees with
    the observed pass/fail. A 'blocked' report with green checks is a
    contradiction; a 'completed' report over a red check is a lie.
    """
    problems = validate_report(packet, report)
    if problems:
        return VerificationOutcome(verified=False, problems=problems, observations=[])

    observations = [run_check(check) for check in packet["acceptance_checks"]]
    reported = {c["check_id"]: c["passed"] for c in report["checks"]}
    for observation in observations:
        if reported.get(observation.check_id) != observation.passed:
            problems.append(
                f"check {observation.check_id!r}: reported "
                f"passed={reported.get(observation.check_id)} but verifier observed "
                f"passed={observation.passed} ({observation.detail})"
            )
    if report["status"] == REPORT_STATUS_BLOCKED and all(o.passed for o in observations):
        problems.append("status 'blocked' contradicts all-green acceptance checks")
    return VerificationOutcome(verified=not problems, problems=problems, observations=observations)


# --- internals -------------------------------------------------------------------


def _find_source(source_id: str) -> SourceConfig:
    matches = [s for s in load_source_configs() if s.source_id == source_id]
    if len(matches) != 1:
        raise PacketError(f"source {source_id!r} not declared (exactly once) in sources.yml")
    return matches[0]


def _connector_for(source_id: str) -> BaseConnector:
    _find_source(source_id)  # unknown sources fail here with a clear error
    for connector in build_registry_connectors():
        if connector.source.source_id == source_id:
            return connector
    raise PacketError(f"no connector builds for source {source_id!r}")


def _declared_maturity(source_id: str) -> ConnectorMaturity:
    """Maturity is declared on the connector class, not in sources.yml —
    resolve it from the built connector."""
    return _connector_for(source_id).maturity


REPORT_SCHEMA_DOC = """\
# Onboarding report schema (`cubiczan-connector-onboarding-report/v1`)

The packet's fixed report contract. `verify_report.py` checks this schema and
re-runs every acceptance check; a report the verifier disagrees with fails.

| Field | Type | Rules |
|---|---|---|
| `schema` | string | exactly `cubiczan-connector-onboarding-report/v1` |
| `packet_id` | string | must bind to this packet's `packet_id` |
| `source_id` | string | must match this packet's `source_id` |
| `status` | string | `completed` (all checks green) or `blocked` (with notes) |
| `checks` | list | EXACTLY this packet's acceptance-check ids, each `{check_id, passed: bool, detail: string}` |
| `artifacts` | list | repo-relative paths of files the work added/changed |
| `notes` | string | anything the next reviewer needs (blocked: the blocker) |
| `agent` | string | which agent produced the report |
| `completed_at` | string | ISO-8601 timestamp |

Report a `blocked` status honestly — a blocked report with evidence is a
successful packet run; a completed report the verifier refutes is not.
"""

VERIFY_REPORT_SCRIPT = '''"""Standalone packet-report verifier.

Usage: python verify_report.py --packet packet.json --report report.json

Exit 0 = report verified (schema valid, every acceptance check re-run and
agreeing). Exit 1 = rejected, with reasons on stdout. Requires the target
repository importable (the script walks up to the repo root).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _find_repo_root() -> Path:
    """Walk up from the packet directory to the repo root (the directory
    whose child ``connectors/`` is importable) so the standalone verifier
    works wherever the packet was dropped."""
    for candidate in [Path(__file__).resolve().parent, *Path(__file__).resolve().parents]:
        if (candidate / "connectors" / "packets.py").exists():
            return candidate
    raise SystemExit(
        "could not locate the repository root (no connectors/packets.py above the packet); "
        "run the verifier from inside the repo"
    )


sys.path.insert(0, str(_find_repo_root()))

from connectors.packets import verify_packet  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet", required=True, help="path to packet.json")
    parser.add_argument("--report", required=True, help="path to the agent's report.json")
    args = parser.parse_args()
    packet = json.loads(Path(args.packet).read_text(encoding="utf-8"))
    report = json.loads(Path(args.report).read_text(encoding="utf-8"))
    outcome = verify_packet(packet, report)
    print(json.dumps(outcome.to_dict(), indent=2, ensure_ascii=False))
    return 0 if outcome.verified else 1


if __name__ == "__main__":
    raise SystemExit(main())
'''


def main(argv: list[str] | None = None) -> int:
    """CLI: build a packet directory, or verify a report against one."""
    parser = argparse.ArgumentParser(
        prog="python -m connectors.packets", description=__doc__.splitlines()[0]
    )
    sub = parser.add_subparsers(dest="command", required=True)

    build = sub.add_parser("build", help="build a self-contained packet directory")
    build.add_argument("--source", required=True, help="source_id from connectors/sources.yml")
    build.add_argument("--out", required=True, help="output directory for the packet")

    verify = sub.add_parser("verify", help="verify a report against a packet directory")
    verify.add_argument("--packet", required=True, help="packet directory (or packet.json path)")
    verify.add_argument("--report", required=True, help="path to the agent's report.json")

    args = parser.parse_args(argv)
    if args.command == "build":
        packet = build_packet(args.source)
        target = write_packet_directory(packet, Path(args.out))
        print(json.dumps({"packet": packet, "written_to": str(target)}, indent=2, default=str))
        return 0
    packet_path = Path(args.packet)
    if packet_path.is_dir():
        packet_path = packet_path / "packet.json"
    packet = json.loads(packet_path.read_text(encoding="utf-8"))
    report = json.loads(Path(args.report).read_text(encoding="utf-8"))
    outcome = verify_packet(packet, report)
    print(json.dumps(outcome.to_dict(), indent=2, ensure_ascii=False))
    return 0 if outcome.verified else 1


if __name__ == "__main__":
    raise SystemExit(main())
