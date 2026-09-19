"""Dagster Definitions: extraction assets, dbt assets, checks, and the demo job."""

from __future__ import annotations

from dagster import AssetSelection, Definitions, define_asset_job

from orchestration.assets import extraction_assets
from orchestration.checks import asset_checks
from orchestration.dbt_assets import bmd_dbt_assets, dbt_resource

demo_job = define_asset_job(name="demo_job", selection=AssetSelection.all())

defs = Definitions(
    assets=[*extraction_assets, bmd_dbt_assets],
    asset_checks=[*asset_checks],
    jobs=[demo_job],
    resources={"dbt": dbt_resource},
)
