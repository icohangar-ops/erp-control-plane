"""Guardrail fixtures: every case runs the real policy; the rot check holds.

Three layers (bd-coach config/dlp/run_tests.py pattern):

1. Every YAML fixture case under ``config/guardrails/`` executes the live
   policy function — must_pass must not raise, must_block must raise the
   declared error class (and, for the CHP gate, the declared R0 result must
   be FATAL). No mocks, no reimplementations: a policy change that flips a
   verdict fails here.
2. The loader is fail-closed — malformed fixtures are fixture errors, never
   skipped cases.
3. The rot check (``python -m guardrail_fixtures.check``) fails when a
   declared policy surface changes without a same-PR fixture update.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from guardrail_fixtures.loader import (
    FixtureCase,
    FixtureError,
    FixtureSpec,
    load_fixtures,
    missing_fixture_updates,
    parse_fixture,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURES = load_fixtures(REPO_ROOT / "config" / "guardrails")


# --- executors: the real policy entry points -----------------------------------


@pytest.fixture()
def executors(tmp_path: Path) -> dict[str, object]:
    from test_genbi_chp import base_env

    from api.genbi.chp import ChpPromotionGate
    from api.genbi.config import GenbiSettings
    from api.genbi.guardrails import ensure_read_only_uri, validate_select_only

    gate = ChpPromotionGate(GenbiSettings.from_env(base_env(tmp_path)))
    return {
        "validate_select_only": validate_select_only,
        "ensure_read_only_uri": ensure_read_only_uri,
        "ChpPromotionGate.open_r0": gate.open_r0,
    }


def _call(executor: object, case: FixtureCase) -> object:
    """Dispatch a fixture case to its policy entry point by payload shape."""
    if executor is None:
        raise FixtureError(f"case {case.name!r}: no executor resolved")
    if case.uri is not None:
        return executor(case.uri)  # type: ignore[operator]
    if case.question is not None and case.sql is not None:
        return executor(case.question, case.sql, None)  # type: ignore[operator]
    if case.sql is not None:
        return executor(case.sql)  # type: ignore[operator]
    raise FixtureError(f"case {case.name!r}: no recognizable payload (sql/uri/question)")


def _error_classes() -> dict[str, type[Exception]]:
    from api.genbi.chp import ChpRejection
    from api.genbi.guardrails import MultipleStatements, NotReadOnlyUri, NotSelectOnly

    return {
        cls.__name__: cls
        for cls in (NotSelectOnly, MultipleStatements, NotReadOnlyUri, ChpRejection)
    }


def test_every_fixture_case_against_live_policy(executors: dict[str, object]) -> None:
    error_classes = _error_classes()
    for spec in FIXTURES:
        executor = executors.get(spec.function)
        assert executor is not None, f"{spec.path}: unknown policy function {spec.function}"

        for case in spec.must_pass:
            try:
                _call(executor, case)
            except Exception as exc:
                pytest.fail(f"{spec.path} must_pass case {case.name!r} was refused: {exc}")

        for case in spec.must_block:
            expected = error_classes.get(case.error or "")
            assert expected is not None, f"{spec.path}: unknown error class {case.error!r}"
            with pytest.raises(expected) as excinfo:  # type: ignore[valid-type]
                _call(executor, case)
            if case.result is not None:  # CHP gate: the named R0 result is the fatal one
                evaluation = getattr(excinfo.value, "evaluation", None)
                assert evaluation is not None, (
                    f"{spec.path} case {case.name!r}: no R0 evaluation attached"
                )
                assert evaluation.results.get(case.result) == "FATAL", (
                    f"{spec.path} case {case.name!r}: {case.result} is not the failing R0 result"
                )


def test_every_declared_surface_file_exists() -> None:
    for spec in FIXTURES:
        for surface in spec.surface:
            assert (REPO_ROOT / surface).is_file(), (
                f"{spec.path} pins surface {surface}, which does not exist — "
                "the fixture outlived its policy (update or retire it)"
            )


def test_fixtures_cover_the_prompt_bearing_surfaces() -> None:
    pinned = {surface for spec in FIXTURES for surface in spec.surface}
    for required in ("api/genbi/guardrails.py", "api/genbi/chp.py"):
        assert required in pinned, f"no fixture pins the prompt-bearing surface {required}"


# --- the loader is fail-closed --------------------------------------------------


def _fixture_yaml(**overrides: object) -> str:
    import yaml

    doc: dict[str, object] = {
        "surface": ["api/genbi/guardrails.py"],
        "function": "validate_select_only",
        "must_pass": [{"name": "ok", "sql": "SELECT 1"}],
        "must_block": [{"name": "bad", "sql": "DELETE FROM x", "error": "NotSelectOnly"}],
    }
    doc.update(overrides)
    return yaml.safe_dump(doc)


def test_parse_fixture_accepts_a_valid_spec(tmp_path: Path) -> None:
    spec = parse_fixture(tmp_path / "f.yaml", _fixture_yaml())
    assert spec.function == "validate_select_only"
    assert spec.surface == ("api/genbi/guardrails.py",)
    assert spec.must_pass[0].name == "ok"
    assert spec.must_block[0].error == "NotSelectOnly"


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ({"surface": []}, "non-empty list"),
        ({"surface": "api/genbi/guardrails.py"}, "non-empty list"),
        ({"function": ""}, "must name the policy entry point"),
        ({"must_pass": []}, "at least one must_pass"),
        ({"must_block": []}, "at least one must_block"),
        (
            {"must_block": [{"name": "bad", "sql": "DELETE FROM x"}]},
            "must declare the expected error class",
        ),
        (
            {"must_pass": [{"name": "ok", "sql": "SELECT 1", "error": "NotSelectOnly"}]},
            "must not declare an error",
        ),
        ({"unknown_key": 1}, "unknown top-level field"),
        ({"must_pass": [{"sql": "SELECT 1"}]}, "missing required field 'name'"),
        ({"must_pass": [{"name": "ok", "sql": "SELECT 1", "bogus": 1}]}, "unknown field"),
    ],
)
def test_parse_fixture_rejects_malformed_specs(
    tmp_path: Path, mutation: dict, message: str
) -> None:
    with pytest.raises(FixtureError, match=message):
        parse_fixture(tmp_path / "f.yaml", _fixture_yaml(**mutation))


def test_parse_fixture_rejects_unparseable_yaml(tmp_path: Path) -> None:
    with pytest.raises(FixtureError, match="unparseable YAML"):
        parse_fixture(tmp_path / "f.yaml", "surface: [open (")
    with pytest.raises(FixtureError, match="must be a mapping"):
        parse_fixture(tmp_path / "f.yaml", "- just\n- a\n- list\n")


def test_load_fixtures_fails_closed_on_missing_or_empty_dir(tmp_path: Path) -> None:
    with pytest.raises(FixtureError, match="missing"):
        load_fixtures(tmp_path / "absent")
    (tmp_path / "empty").mkdir()
    with pytest.raises(FixtureError, match="no fixtures"):
        load_fixtures(tmp_path / "empty")


# --- the rot check as a pure function -------------------------------------------


def _spec(tmp_path: Path, fixture_name: str, *surfaces: str) -> FixtureSpec:
    return parse_fixture(tmp_path / fixture_name, _fixture_yaml(surface=list(surfaces)))


def test_rot_lists_fixture_whose_surface_changed_without_it(tmp_path: Path) -> None:
    spec = _spec(tmp_path, "sql.yaml", "api/genbi/guardrails.py")
    rot = missing_fixture_updates({"api/genbi/guardrails.py"}, [spec])
    assert rot == {spec.path: {"api/genbi/guardrails.py"}}


def test_rot_is_clean_when_fixture_changed_with_its_surface(tmp_path: Path) -> None:
    spec = _spec(tmp_path, "sql.yaml", "api/genbi/guardrails.py")
    rot = missing_fixture_updates({"api/genbi/guardrails.py", str(spec.path)}, [spec])
    assert rot == {}


def test_rot_ignores_unrelated_changes(tmp_path: Path) -> None:
    spec = _spec(tmp_path, "sql.yaml", "api/genbi/guardrails.py")
    assert missing_fixture_updates({"README.md", "dbt/models/x.sql"}, [spec]) == {}
    assert missing_fixture_updates(set(), [spec]) == {}


def test_rot_tracks_each_surface_independently(tmp_path: Path) -> None:
    spec = _spec(tmp_path, "uri.yaml", "api/genbi/guardrails.py", "genbi/connection.py")
    rot = missing_fixture_updates({"genbi/connection.py"}, [spec])
    assert rot == {spec.path: {"genbi/connection.py"}}


# --- the CLI end to end ----------------------------------------------------------


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)


@pytest.fixture()
def cli_env() -> dict[str, str]:
    return {**os.environ, "PYTHONPATH": str(REPO_ROOT)}


def test_check_cli_fails_when_surface_changes_without_fixture(
    tmp_path: Path, cli_env: dict[str, str]
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "test")
    (repo / "surface.py").write_text("policy v1\n", encoding="utf-8")
    fixtures = repo / "config" / "guardrails"
    fixtures.mkdir(parents=True)
    (fixtures / "sql.yaml").write_text(_fixture_yaml(surface=["surface.py"]), encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "fixture pins surface")
    base = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True
    ).stdout.strip()

    # Surface changes without a fixture update -> the check fails loudly.
    (repo / "surface.py").write_text("policy v2\n", encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "policy change without fixture")
    stale = subprocess.run(
        [sys.executable, "-m", "guardrail_fixtures.check", f"--base={base}"],
        cwd=repo,
        env=cli_env,
        capture_output=True,
        text=True,
    )
    assert stale.returncode == 1, stale.stdout + stale.stderr
    assert "GUARDRAIL FIXTURES STALE" in stale.stderr
    assert "surface.py" in stale.stderr

    # The same-PR fixture update clears the check.
    sql = fixtures / "sql.yaml"
    sql.write_text(sql.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "fixture updated in same PR")
    coupled = subprocess.run(
        [sys.executable, "-m", "guardrail_fixtures.check", f"--base={base}"],
        cwd=repo,
        env=cli_env,
        capture_output=True,
        text=True,
    )
    assert coupled.returncode == 0, coupled.stdout + coupled.stderr
    assert "coupling holds" in coupled.stdout


def test_check_cli_passes_on_the_real_repo_head(cli_env: dict[str, str]) -> None:
    """Empty diff (HEAD...HEAD) -> coupling trivially holds; the fixtures also parse."""
    result = subprocess.run(
        [sys.executable, "-m", "guardrail_fixtures.check", "--base=HEAD"],
        cwd=REPO_ROOT,
        env=cli_env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
