"""Dagster code location for the ERP control plane.

Assets:
- ``source_registry``: loads connectors/sources.yml and registers enabled
  sources in the control-plane store.
- one extraction asset per (source, entity), keyed to match the dbt external
  sources so dbt models inherit lineage automatically.
- dbt assets: the whole dbt project (staging -> canonical -> marts) via
  dagster-dbt, with all dbt tests as asset checks.

Jobs:
- ``demo_job``: the seeded CSV dealer end to end.
"""

from orchestration.definitions import defs

__all__ = ["defs"]
