"""dbt assets: the whole dbt project loaded through dagster-dbt.

Bootstraps ``dbt parse`` on first load so the code location can start from a
clean checkout (manifest.json is a build artifact and not committed).

Dagster invokes dbt with the project dir as cwd, while ``make demo`` runs from
the repo root; both LAKE_ROOT and ANALYTICS_DUCKDB_PATH are pinned to absolute
paths here so the dbt profile and external sources resolve identically.
"""

import os
import subprocess
from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

REPO_ROOT = Path(__file__).resolve().parents[1]

os.environ.setdefault("LAKE_ROOT", str(REPO_ROOT / "data/lake"))
os.environ.setdefault("ANALYTICS_DUCKDB_PATH", str(REPO_ROOT / "data/analytics/analytics.duckdb"))
(REPO_ROOT / "data/analytics").mkdir(parents=True, exist_ok=True)

DBT_PROJECT = DbtProject(
    project_dir=REPO_ROOT / "dbt",
    profiles_dir=REPO_ROOT / "dbt",
    target="demo",
)

if not DBT_PROJECT.manifest_path.exists():
    subprocess.run(
        [
            "dbt",
            "parse",
            "--project-dir",
            str(DBT_PROJECT.project_dir),
            "--profiles-dir",
            str(DBT_PROJECT.profiles_dir),
            "--target",
            "demo",
        ],
        check=True,
        cwd=REPO_ROOT,
        capture_output=True,
    )

dbt_resource = DbtCliResource(project_dir=DBT_PROJECT)


@dbt_assets(manifest=DBT_PROJECT.manifest_path, project=DBT_PROJECT)
def bmd_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """All dbt models + tests in one invocable set (staging -> canonical -> marts)."""
    yield from dbt.cli(["build"], context=context).stream()
