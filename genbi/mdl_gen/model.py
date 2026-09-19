"""dbt artifact model: the typed view of manifest.json + catalog.json the MDL tools consume.

The coverage boundary (spec §3.2) lives here in one place:

- MODELED schemas: the dbt-duckdb physical schemas holding the canonical
  dimensional model (dims/facts) and the KPI marts.
- NEVER modeled: staging, seeds/reference, and the crosswalk_/ref_ integration
  scaffolding — they are plumbing, not business surface, and loading them into
  the NL vocabulary would put raw-system names in front of the LLM.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

# dbt-duckdb composes physical schemas as <target_schema>_<custom_schema>;
# the demo/CI target schema is "main", so canonical → main_canonical etc.
CANONICAL_SCHEMA = "main_canonical"
MARTS_SCHEMA = "main_marts"
MODELED_SCHEMAS = frozenset({CANONICAL_SCHEMA, MARTS_SCHEMA})

# Schemas that must never appear in an MDL table_reference.
FORBIDDEN_SCHEMAS = frozenset({"main_staging", "main_reference", "main_raw"})

# Reference/integration tables inside the canonical schema that are not
# business surface: source-system crosswalks and code mappings.
UNMODELED_MODEL_PREFIXES = ("crosswalk_", "ref_")

CATALOG_STEM = "analytics"  # DuckDB attached-database name = file stem


class ModeledSetError(Exception):
    """The dbt artifacts violate the coverage boundary in a way that cannot be resolved."""


def _parse_ref(ref_expr: str) -> str:
    """Extract a model name from a dbt ref expression like ``ref('dim_item')``."""
    match = re.search(r"ref\(['\"]([^'\"]+)['\"]\)", ref_expr)
    return match.group(1) if match else ""


@dataclass(frozen=True)
class InferredRelationship:
    """A join inferred from a dbt relationship test."""

    from_model: str
    from_column: str
    to_model: str
    to_column: str


@dataclass(frozen=True)
class DbtModel:
    """One dbt model as physically materialized, with its declared tests."""

    name: str
    schema: str
    database: str
    description: str
    columns: tuple[tuple[str, str], ...]  # (name, physical type) in catalog order
    unique_columns: tuple[str, ...]  # grain declared by dbt unique tests
    not_null_columns: tuple[str, ...]
    relationships: tuple[InferredRelationship, ...]

    @property
    def is_canonical(self) -> bool:
        return self.schema == CANONICAL_SCHEMA

    @property
    def is_kpi_mart(self) -> bool:
        return self.schema == MARTS_SCHEMA


def is_modeled_model(schema: str, name: str) -> bool:
    """The coverage boundary: which dbt models belong in the MDL (spec §3.2)."""
    if schema not in MODELED_SCHEMAS:
        return False
    return not name.startswith(UNMODELED_MODEL_PREFIXES)


def load_dbt_models(manifest_path: Path, catalog_path: Path) -> dict[str, DbtModel]:
    """Parse dbt manifest.json + catalog.json into modeled DbtModels.

    catalog.json is the physical source of truth for column names and types;
    manifest.json supplies descriptions and the unique/not_null/relationship
    tests the generator and coupling check consume.
    """
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))

    tests_by_model: dict[str, list[dict]] = {}
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "test":
            continue
        metadata = node.get("test_metadata") or {}
        kind = metadata.get("name")
        attached = node.get("attached_node") or ""
        # attached_node names the tested model; depends_on order is NOT reliable
        # for relationship tests (the target may sort first).
        if not attached or "." not in attached:
            continue
        model = attached.split(".")[-1]
        if kind in ("unique", "not_null") and node.get("column_name"):
            tests_by_model.setdefault(model, []).append(
                {"kind": kind, "column": node["column_name"]}
            )
        elif kind == "relationships":
            kwargs = metadata.get("kwargs") or {}
            target = _parse_ref(str(kwargs.get("to", "")))
            field = kwargs.get("field") or node.get("column_name")
            if target and field:
                tests_by_model.setdefault(model, []).append(
                    {"kind": kind, "column": field, "target": target}
                )

    catalog_nodes = catalog.get("nodes", {})
    modeled_names = {
        node["metadata"]["name"]
        for node in catalog_nodes.values()
        if is_modeled_model(node["metadata"]["schema"], node["metadata"]["name"])
    }

    models: dict[str, DbtModel] = {}
    for node_id, node in catalog_nodes.items():
        meta = node["metadata"]
        schema, name = meta["schema"], meta["name"]
        if not is_modeled_model(schema, name):
            continue
        if node_id.split(".")[-1] != name or name in models:
            raise ModeledSetError(f"duplicate model name in catalog: {name}")

        columns = tuple(
            (col["name"], col["type"])
            for col in sorted(node["columns"].values(), key=lambda c: c.get("index", 0))
        )

        model_tests = tests_by_model.get(name, [])
        unique_columns = tuple(t["column"] for t in model_tests if t["kind"] == "unique")
        not_null_columns = tuple(t["column"] for t in model_tests if t["kind"] == "not_null")
        # Keep only joins whose both sides are inside the modeled set —
        # a relationship referencing ref_price_list or a crosswalk has no
        # MDL model to attach to.
        relationships = tuple(
            InferredRelationship(
                from_model=name,
                from_column=t["column"],
                to_model=t["target"],
                to_column=t["column"],  # refined by resolve_relationship_target_column
            )
            for t in model_tests
            if t["kind"] == "relationships" and t["target"] in modeled_names
        )

        models[name] = DbtModel(
            name=name,
            schema=schema,
            database=meta["database"],
            description=(
                manifest.get("nodes", {}).get(node_id, {}).get("description") or ""
            ).strip(),
            columns=columns,
            unique_columns=unique_columns,
            not_null_columns=not_null_columns,
            relationships=relationships,
        )
    return models


def resolve_relationship_target_column(
    models: dict[str, DbtModel], rel: InferredRelationship
) -> str:
    """Find the referenced column on the target model.

    dbt relationship tests declare (from_column, to_model); the target-side
    field is the target's unique/grain column per the Kimball rule that
    surrogate keys join and natural keys are attributes.
    """
    target = models.get(rel.to_model)
    if target and target.unique_columns:
        return target.unique_columns[0]
    # Fall back to the convention: <model stem>_key on the target.
    fallback = rel.to_model.removeprefix("dim_") + "_key"
    if target and any(col == fallback for col, _ in target.columns):
        return fallback
    raise ModeledSetError(
        f"cannot resolve target column for relationship "
        f"{rel.from_model}.{rel.from_column} -> {rel.to_model}: no unique test on target"
    )
