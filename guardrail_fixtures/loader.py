"""Fail-closed YAML fixture loader and the rot check as a pure function."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

FIXTURES_DIR = Path("config/guardrails")

_ALLOWED_CASE_KEYS = frozenset({"name", "error", "sql", "uri", "question", "result"})
_ALLOWED_TOP_KEYS = frozenset({"surface", "function", "must_pass", "must_block"})


class FixtureError(Exception):
    """A guardrail fixture is malformed — fail closed, never skip."""


@dataclass(frozen=True)
class FixtureCase:
    """One policy verdict: payload plus (for blocks) the expected error class."""

    name: str
    error: str | None = None  # guardrail error class name, must-block only
    sql: str | None = None
    uri: str | None = None
    question: str | None = None
    result: str | None = None  # R0 result key that must be FATAL (chp gate)

    def payload(self, key: str) -> str:
        value = getattr(self, key)
        if value is None:
            raise FixtureError(f"case {self.name!r}: missing required payload field {key!r}")
        return value


@dataclass(frozen=True)
class FixtureSpec:
    """One YAML fixture: the policy it pins and the surface files that pin it."""

    path: Path
    surface: tuple[str, ...]
    function: str
    must_pass: tuple[FixtureCase, ...] = field(default_factory=tuple)
    must_block: tuple[FixtureCase, ...] = field(default_factory=tuple)


def _parse_case(raw: object, fixture: Path, section: str) -> FixtureCase:
    if not isinstance(raw, dict):
        raise FixtureError(f"{fixture}: {section} case must be a mapping, got {type(raw).__name__}")
    unknown = set(raw) - _ALLOWED_CASE_KEYS
    if unknown:
        raise FixtureError(
            f"{fixture}: {section} case {raw.get('name', '?')!r} has unknown field(s): {sorted(unknown)}"
        )
    if "name" not in raw:
        raise FixtureError(f"{fixture}: {section} case is missing required field 'name'")
    if section == "must_block" and not raw.get("error"):
        raise FixtureError(
            f"{fixture}: must_block case {raw['name']!r} must declare the expected error class"
        )
    if section == "must_pass" and raw.get("error"):
        raise FixtureError(
            f"{fixture}: must_pass case {raw['name']!r} must not declare an error class"
        )
    return FixtureCase(
        name=str(raw["name"]),
        error=raw.get("error"),
        sql=raw.get("sql"),
        uri=raw.get("uri"),
        question=raw.get("question"),
        result=raw.get("result"),
    )


def parse_fixture(path: Path, content: str) -> FixtureSpec:
    """Parse one fixture file's YAML, validating the schema fail-closed."""
    try:
        doc = yaml.safe_load(content)
    except yaml.YAMLError as exc:
        raise FixtureError(f"{path}: unparseable YAML — {exc}") from exc
    if not isinstance(doc, dict):
        raise FixtureError(f"{path}: fixture must be a mapping")
    unknown = set(doc) - _ALLOWED_TOP_KEYS
    if unknown:
        raise FixtureError(f"{path}: unknown top-level field(s): {sorted(unknown)}")
    surface = doc.get("surface")
    if (
        not isinstance(surface, list)
        or not surface
        or not all(isinstance(s, str) and s for s in surface)
    ):
        raise FixtureError(
            f"{path}: 'surface' must be a non-empty list of repo-relative file paths"
        )
    function = doc.get("function")
    if not isinstance(function, str) or not function:
        raise FixtureError(f"{path}: 'function' must name the policy entry point")
    must_pass_raw = doc.get("must_pass", [])
    must_block_raw = doc.get("must_block", [])
    if not isinstance(must_pass_raw, list) or not isinstance(must_block_raw, list):
        raise FixtureError(f"{path}: 'must_pass'/'must_block' must be lists")
    if not must_pass_raw:
        raise FixtureError(f"{path}: at least one must_pass case is required")
    if not must_block_raw:
        raise FixtureError(f"{path}: at least one must_block case is required")
    return FixtureSpec(
        path=path,
        surface=tuple(surface),
        function=function,
        must_pass=tuple(_parse_case(c, path, "must_pass") for c in must_pass_raw),
        must_block=tuple(_parse_case(c, path, "must_block") for c in must_block_raw),
    )


def load_fixtures(fixtures_dir: Path = FIXTURES_DIR) -> list[FixtureSpec]:
    """Load every YAML fixture in the directory (sorted, deterministic)."""
    if not fixtures_dir.is_dir():
        raise FixtureError(
            f"fixtures directory {fixtures_dir} is missing — policy surfaces are unpinned"
        )
    specs = [
        parse_fixture(path, path.read_text(encoding="utf-8"))
        for path in sorted(fixtures_dir.glob("*.yaml"))
    ]
    if not specs:
        raise FixtureError(f"no fixtures found in {fixtures_dir} — policy surfaces are unpinned")
    return specs


def missing_fixture_updates(
    changed_files: set[str], specs: list[FixtureSpec]
) -> dict[Path, set[str]]:
    """The rot check as a pure function.

    Returns fixture path -> the changed surface files that pin it, for every
    fixture whose declared surface changed in this diff without a same-diff
    update to the fixture itself. Empty dict = all clean.
    """
    rot: dict[Path, set[str]] = {}
    changed = {Path(p).as_posix() for p in changed_files}
    for spec in specs:
        if spec.path.as_posix() in changed:  # Path never equals str — compare normalized
            continue  # the fixture itself changed — the coupling holds
        touched = sorted(set(spec.surface) & changed)
        if touched:
            rot[spec.path] = set(touched)
    return rot
