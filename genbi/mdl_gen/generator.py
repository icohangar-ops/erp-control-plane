"""First-draft MDL generation from dbt artifacts (spec §3.1).

Generation kills transcription work; it never ships. Output is a draft tree
(``genbi/draft/`` by default, gitignored) that a human curates into
``genbi/mdl/``: business terms into knowledge/, relationships validated
against the Kimball grain rules, column descriptions where dbt has none.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from genbi.mdl_gen.model import (
    CATALOG_STEM,
    DbtModel,
    resolve_relationship_target_column,
)

# Draft output location (gitignored): generated trees are review input,
# never the versioned artifact.
DEFAULT_DRAFT_DIR = Path("genbi/draft")


@dataclass(frozen=True)
class GeneratedProject:
    """In-memory first draft: project manifest, models, relationships."""

    project: dict[str, Any]
    models: dict[str, dict[str, Any]]
    relationships: list[dict[str, Any]]


def _column_entry(model: DbtModel, col_name: str, col_type: str) -> dict[str, Any]:
    entry: dict[str, Any] = {"name": col_name, "type": col_type}
    if col_name in model.not_null_columns:
        entry["not_null"] = True
    entry["properties"] = {"description": ""}
    return entry


def build_model_metadata(models: dict[str, DbtModel], model: DbtModel) -> dict[str, Any]:
    """Emit one model's metadata.yml as a plain dict (schema_version 5 shape)."""
    columns = [_column_entry(model, col_name, col_type) for col_name, col_type in model.columns]
    if model.unique_columns:
        for entry in columns:
            entry["is_primary_key"] = entry["name"] == model.unique_columns[0]
    metadata: dict[str, Any] = {
        "name": model.name,
        "table_reference": {
            "catalog": model.database or CATALOG_STEM,
            "schema": model.schema,
            "table": model.name,
        },
        "columns": columns,
        "properties": {"description": model.description},
    }
    if model.unique_columns:
        metadata["primary_key"] = (
            model.unique_columns[0]
            if len(model.unique_columns) == 1
            else list(model.unique_columns)
        )
    return metadata


def build_relationships(models: dict[str, DbtModel]) -> list[dict[str, Any]]:
    """Join declarations inferred from dbt relationship tests (fact → dimension)."""
    relationships: list[dict[str, Any]] = []
    seen: set[str] = set()
    for model in sorted(models.values(), key=lambda m: m.name):
        for rel in model.relationships:
            to_column = resolve_relationship_target_column(models, rel)
            join_type = "MANY_TO_ONE" if model.is_canonical or model.is_kpi_mart else "ONE_TO_MANY"
            name = f"{rel.from_model}_{rel.to_model}"
            if name in seen:
                name = f"{name}_{rel.from_column}"
            seen.add(name)
            relationships.append(
                {
                    "name": name,
                    "models": [rel.from_model, rel.to_model],
                    "join_type": join_type,
                    "condition": f"{rel.from_model}.{rel.from_column} = {rel.to_model}.{to_column}",
                }
            )
    return relationships


def build_mdl_project(models: dict[str, DbtModel], project_name: str) -> GeneratedProject:
    """Assemble the full first-draft project from indexed dbt models."""
    project = {
        "schema_version": 5,
        "name": project_name,
        "version": "0.1.0",
        "catalog": "wren",  # WrenAI namespace — not the database catalog
        "schema": "public",  # WrenAI namespace — not the database schema
        "data_source": "duckdb",
    }
    model_meta = {name: build_model_metadata(models, model) for name, model in models.items()}
    return GeneratedProject(
        project=project,
        models=model_meta,
        relationships=build_relationships(models),
    )


def write_draft(generated: GeneratedProject, out_dir: Path) -> Path:
    """Write the draft tree: wren_project.yml, models/<n>/metadata.yml, relationships.yml."""
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "wren_project.yml").write_text(
        yaml.safe_dump(generated.project, sort_keys=False), encoding="utf-8"
    )
    for name, metadata in generated.models.items():
        model_dir = out_dir / "models" / name
        model_dir.mkdir(parents=True, exist_ok=True)
        (model_dir / "metadata.yml").write_text(
            yaml.safe_dump(metadata, sort_keys=False, width=100), encoding="utf-8"
        )
    (out_dir / "relationships.yml").write_text(
        yaml.safe_dump({"relationships": generated.relationships}, sort_keys=False, width=100),
        encoding="utf-8",
    )
    return out_dir
