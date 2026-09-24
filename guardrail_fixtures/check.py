"""``python -m guardrail_fixtures.check [--base=origin/main]`` — the fixture rot check.

Fails (exit 1) when a guardrail fixture's declared surface file changed in the
diff but the fixture itself did not: a policy surface changed without a
same-PR fixture update. CI runs this on every pull request; the diff base is
the PR's base ref. Locally, pass ``--base`` explicitly.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

from guardrail_fixtures.loader import FixtureError, load_fixtures, missing_fixture_updates


def changed_files(base: str, repo_root: Path) -> set[str]:
    """Files changed between ``base`` and HEAD, as repo-relative POSIX paths."""
    # A source archive has no Git metadata. ``--base=HEAD`` is explicitly the
    # empty-diff local smoke-test mode, so make it deterministic outside a
    # working tree as well as inside one.
    if base == "HEAD":
        probe = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=repo_root,
            capture_output=True,
            text=True,
        )
        if probe.returncode != 0:
            return set()
    result = subprocess.run(
        ["git", "diff", "--name-only", f"{base}...HEAD"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
    )
    return {line.strip() for line in result.stdout.splitlines() if line.strip()}


def discover_repo_root() -> Path:
    """The repository the check runs in: git's top-level, else the package's repo."""
    probe = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True)
    if probe.returncode == 0 and probe.stdout.strip():
        return Path(probe.stdout.strip()).resolve()
    return Path(__file__).resolve().parent.parent


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Guardrail fixture coupling check")
    parser.add_argument(
        "--base", default="origin/main", help="diff base ref (default: origin/main)"
    )
    args = parser.parse_args(argv)

    repo_root = discover_repo_root()
    try:
        loaded = load_fixtures(repo_root / "config" / "guardrails")
    except FixtureError as exc:
        print(f"GUARDRAIL FIXTURES BROKEN — refusing to pass: {exc}", file=sys.stderr)
        return 1
    # git diffs speak repo-relative paths; compare the rot check in that frame.
    specs = [replace(spec, path=spec.path.relative_to(repo_root)) for spec in loaded]

    try:
        rot = missing_fixture_updates(changed_files(args.base, repo_root), specs)
    except subprocess.CalledProcessError as exc:
        print(f"could not diff against {args.base}: {exc.stderr.strip()}", file=sys.stderr)
        return 1

    if not rot:
        print(
            "guardrail fixtures: coupling holds — every changed policy surface shipped with its fixture"
        )
        return 0

    print(
        "GUARDRAIL FIXTURES STALE — policy surface(s) changed without a same-PR fixture update:",
        file=sys.stderr,
    )
    for fixture_path, surfaces in sorted(rot.items()):
        print(
            f"  {fixture_path} must be updated (changed surface: {', '.join(sorted(surfaces))})",
            file=sys.stderr,
        )
    print(
        "Policy without fixtures rots: add or update must-block/must-pass cases in the same PR.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
