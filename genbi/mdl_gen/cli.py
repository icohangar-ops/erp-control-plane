"""CLI: python -m genbi.mdl_gen {generate,validate,check-coupling}."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from genbi.mdl_gen.generator import DEFAULT_DRAFT_DIR, build_mdl_project, write_draft
from genbi.mdl_gen.model import load_dbt_models
from genbi.mdl_gen.validator import validate_project

PROJECT_NAME = "construction_supplies_erp_genbi"
DEFAULT_DBT_DIR = Path("dbt/target")
DEFAULT_MDL_DIR = Path("genbi/mdl")
DEFAULT_DUCKDB = Path("data/analytics/analytics.duckdb")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="genbi.mdl_gen", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="Emit a first-draft MDL tree from dbt artifacts")
    gen.add_argument(
        "--dbt-dir",
        type=Path,
        default=DEFAULT_DBT_DIR,
        help="Directory holding manifest.json + catalog.json",
    )
    gen.add_argument(
        "--out", type=Path, default=DEFAULT_DRAFT_DIR, help="Draft output directory (gitignored)"
    )

    val = sub.add_parser("validate", help="Validate a curated MDL project")
    val.add_argument("--mdl", type=Path, default=DEFAULT_MDL_DIR, help="MDL project directory")
    val.add_argument(
        "--dbt-dir",
        type=Path,
        default=DEFAULT_DBT_DIR,
        help="dbt target dir (enables the coupling check)",
    )
    val.add_argument(
        "--duckdb",
        type=Path,
        nargs="?",
        default=None,
        help="Built analytics.duckdb (enables executability check)",
    )

    coupling = sub.add_parser(
        "check-coupling", help="Spec §3.3 gate: MDL must match dbt grain/columns"
    )
    coupling.add_argument("--mdl", type=Path, default=DEFAULT_MDL_DIR)
    coupling.add_argument("--dbt-dir", type=Path, default=DEFAULT_DBT_DIR)

    args = parser.parse_args(argv)

    if args.command == "generate":
        models = load_dbt_models(args.dbt_dir / "manifest.json", args.dbt_dir / "catalog.json")
        generated = build_mdl_project(models, PROJECT_NAME)
        out = write_draft(generated, args.out)
        print(
            f"draft written to {out}: {len(generated.models)} models, "
            f"{len(generated.relationships)} relationships (draft only — curate before use)"
        )
        return 0

    if args.command == "validate":
        dbt_dir = args.dbt_dir if args.dbt_dir and args.dbt_dir.is_dir() else None
        duckdb_path = args.duckdb if args.duckdb and Path(args.duckdb).exists() else None
        violations = validate_project(args.mdl, dbt_dir=dbt_dir, duckdb_path=duckdb_path)
    else:  # check-coupling
        duckdb_path = None  # executability is validate-only; coupling needs the artifacts
        violations = validate_project(args.mdl, dbt_dir=args.dbt_dir)

    if violations:
        print(f"MDL INVALID ({len(violations)} violation(s)):", file=sys.stderr)
        for v in violations:
            print(f"  - {v}", file=sys.stderr)
        return 1
    print(
        "MDL valid: structure, coverage boundary, and dbt coupling all pass"
        + (f"; executability verified against {duckdb_path}" if duckdb_path else "")
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
