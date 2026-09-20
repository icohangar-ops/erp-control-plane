"""CI gate for the semantic metric registry: python -m analytics.metrics.check.

Validates the registry three ways against the dbt-built warehouse: schema and
refusal invariants (load), a cross-check of every binding against the dbt
manifest (dbt stays the single source of truth), and re-measured population
sizes. Exits non-zero on any violation or refusal.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from analytics.metrics.registry import (
    MetricRegistryError,
    check_against_manifest,
    check_population_sizes,
    load_registry,
)

DEFAULT_DBT_DIR = Path("dbt")
DEFAULT_DUCKDB = Path("data/analytics/analytics.duckdb")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--registry", type=Path, default=None, help="Registry YAML (default: in-package)"
    )
    parser.add_argument(
        "--dbt-dir",
        type=Path,
        default=DEFAULT_DBT_DIR,
        help="dbt project dir (reads target/manifest.json)",
    )
    parser.add_argument("--duckdb", type=Path, default=DEFAULT_DUCKDB, help="Built warehouse")
    args = parser.parse_args(argv)

    try:
        registry = load_registry(args.registry)
    except MetricRegistryError as exc:
        print(f"metric registry REFUSED: {exc}", file=sys.stderr)
        return 1

    manifest_path = args.dbt_dir / "target" / "manifest.json"
    if not manifest_path.exists():
        print(
            f"dbt manifest not found at {manifest_path} — run `make dbt-build` first",
            file=sys.stderr,
        )
        return 2
    if not args.duckdb.exists():
        print(f"warehouse not found at {args.duckdb} — run `make dbt-build` first", file=sys.stderr)
        return 2

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    violations = check_against_manifest(registry, manifest) + check_population_sizes(
        registry, str(args.duckdb)
    )
    if violations:
        print(f"METRIC REGISTRY INVALID ({len(violations)} violation(s)):", file=sys.stderr)
        for violation in violations:
            print(f"  - {violation}", file=sys.stderr)
        return 1

    print(
        f"metric registry OK: {len(registry.entries)} metrics · manifest cross-check clean · "
        "populations re-measured"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
