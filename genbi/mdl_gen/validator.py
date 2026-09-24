"""MDL validation (spec §3.3): structure, coverage boundary, dbt coupling, executability.

``validate_project`` layers four checks over the curated tree at ``genbi/mdl/``:

1. Structure — schema_version 5 shapes for wren_project.yml, models,
   relationships, views.
2. Boundary — every model points at a modeled schema (canonical dims/facts,
   KPI marts); staging/reference/raw are never modeled.
3. Coupling (the CI gate) — column sets, types, and declared grain must match
   the dbt manifest + catalog exactly. A dbt change that alters mart grain or a
   metric column fails here unless the corresponding MDL model was updated in
   the same PR.
4. Executability (when a built analytics.duckdb is supplied) — every declared
   column must select from the physical table, proving the MDL matches reality.

All functions are pure over their inputs and return violation strings;
an empty list means valid.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import duckdb
import yaml

from genbi.mdl_gen.model import (
    CANONICAL_SCHEMA,
    FORBIDDEN_SCHEMAS,
    MARTS_SCHEMA,
    load_dbt_models,
)

JOIN_TYPES = frozenset({"ONE_TO_ONE", "ONE_TO_MANY", "MANY_TO_ONE", "MANY_TO_MANY"})

# Statements a view may start with — everything else (DML/DDL/ATTACH/PRAGMA)
# violates the read-only discipline.
_VIEW_FORBIDDEN_KEYWORDS = re.compile(
    r"^\s*(INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|ATTACH|DETACH|COPY|EXPORT|IMPORT|INSTALL|LOAD|PRAGMA|SET|CALL|VACUUM|CHECKPOINT)\b",
    re.IGNORECASE,
)


def normalize_type(dtype: str) -> str:
    """Case- and whitespace-insensitive type identity for comparisons."""
    return " ".join(dtype.upper().split())


def _load_yaml(path: Path) -> tuple[str | None, Any]:
    try:
        return None, yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return f"missing file: {path}", None
    except yaml.YAMLError as exc:
        return f"invalid YAML in {path}: {exc}", None


def _validate_project_manifest(mdl_dir: Path) -> list[str]:
    violations: list[str] = []
    data, project = _load_yaml(mdl_dir / "wren_project.yml")
    if data:
        violations.append(data)
        return violations
    if not isinstance(project, dict):
        return [f"wren_project.yml must be a mapping, got {type(project).__name__}"]
    if project.get("schema_version") != 5:
        violations.append(
            f"wren_project.yml: schema_version must be 5, got {project.get('schema_version')!r}"
        )
    if not str(project.get("name", "")).strip():
        violations.append("wren_project.yml: 'name' is required")
    if project.get("data_source") != "duckdb":
        violations.append(
            f"wren_project.yml: data_source must be 'duckdb' (analytics.duckdb is the only "
            f"serving surface this phase), got {project.get('data_source')!r}"
        )
    return violations


def _primary_key_columns(metadata: dict[str, Any]) -> set[str]:
    pk = metadata.get("primary_key")
    if isinstance(pk, str):
        return {pk}
    if isinstance(pk, list):
        return {str(c) for c in pk}
    return set()


def _validate_model_file(model_dir: Path) -> list[str]:
    violations: list[str] = []
    path = model_dir / "metadata.yml"
    data, metadata = _load_yaml(path)
    if data:
        return [data]
    if not isinstance(metadata, dict):
        return [f"{path}: must be a mapping"]

    name = metadata.get("name")
    if name != model_dir.name:
        violations.append(f"{path}: name {name!r} must match directory {model_dir.name!r}")

    ref = metadata.get("table_reference") or {}
    if not isinstance(ref, dict) or not ref.get("table"):
        violations.append(f"{path}: table_reference with 'table' is required")
        ref = {}
    schema = ref.get("schema")
    if schema in FORBIDDEN_SCHEMAS:
        violations.append(
            f"{path}: {schema} is never modeled (staging/reference/raw stay outside the MDL)"
        )
    if schema not in (CANONICAL_SCHEMA, MARTS_SCHEMA):
        violations.append(
            f"{path}: table_reference.schema {schema!r} is outside the modeled set "
            f"({CANONICAL_SCHEMA}, {MARTS_SCHEMA})"
        )
    if schema == CANONICAL_SCHEMA and not str(ref.get("table", "")).startswith(("dim_", "fact_")):
        violations.append(
            f"{path}: canonical models must be dim_*/fact_* — crosswalks and ref tables are "
            f"integration scaffolding, not business surface"
        )
    if schema == MARTS_SCHEMA and not str(ref.get("table", "")).startswith("kpi_"):
        violations.append(f"{path}: marts models must be kpi_*")

    columns = metadata.get("columns")
    if not isinstance(columns, list) or not columns:
        violations.append(f"{path}: 'columns' must be a non-empty list")
        return violations

    seen: set[str] = set()
    for col in columns:
        if not isinstance(col, dict) or not col.get("name"):
            violations.append(f"{path}: every column needs a name")
            continue
        col_name = col["name"]
        if col_name in seen:
            violations.append(f"{path}: duplicate column {col_name!r}")
        seen.add(col_name)
        if not col.get("type"):
            violations.append(f"{path}: column {col_name!r} needs a 'type'")
        if col.get("is_primary_key") and col_name not in _primary_key_columns(metadata):
            violations.append(
                f"{path}: column {col_name!r} sets is_primary_key but the model's primary_key "
                f"does not include it"
            )
    pk_missing = _primary_key_columns(metadata) - seen
    if pk_missing:
        violations.append(f"{path}: primary_key references undeclared columns {sorted(pk_missing)}")
    return violations


def _validate_models(mdl_dir: Path) -> list[str]:
    violations: list[str] = []
    models_root = mdl_dir / "models"
    if not models_root.is_dir() or not any(models_root.iterdir()):
        return [f"no models found under {models_root}"]
    for model_dir in sorted(p for p in models_root.iterdir() if p.is_dir()):
        violations.extend(_validate_model_file(model_dir))
    return violations


def _validate_relationships(mdl_dir: Path) -> list[str]:
    violations: list[str] = []
    data, doc = _load_yaml(mdl_dir / "relationships.yml")
    if data:
        return [data]
    declared_models = (
        {p.name for p in (mdl_dir / "models").glob("*/")}
        if (mdl_dir / "models").is_dir()
        else set()
    )
    entries = (doc or {}).get("relationships") if isinstance(doc, dict) else None
    if entries is None:
        return [f"{mdl_dir / 'relationships.yml'}: must be a mapping with a 'relationships' list"]
    for rel in entries:
        if not isinstance(rel, dict):
            violations.append("relationships: every entry must be a mapping")
            continue
        name = rel.get("name", "<unnamed>")
        models = rel.get("models")
        if not isinstance(models, list) or len(models) != 2:
            violations.append(f"relationship {name}: 'models' must be exactly two model names")
            continue
        for m in models:
            if m not in declared_models:
                violations.append(f"relationship {name}: model {m!r} is not declared under models/")
        join_type = rel.get("join_type")
        if join_type not in JOIN_TYPES:
            violations.append(
                f"relationship {name}: join_type {join_type!r} not in {sorted(JOIN_TYPES)}"
            )
        condition = rel.get("condition", "")
        if not isinstance(condition, str) or "=" not in condition:
            violations.append(f"relationship {name}: condition must be an equality")
        elif models and isinstance(condition, str):
            left = condition.split("=", 1)[0].strip().split(".")[0].strip()
            if left != models[0]:
                violations.append(
                    f"relationship {name}: the first model ({models[0]!r}) must appear on the "
                    f"left side of the condition"
                )
    return violations


def _validate_views(mdl_dir: Path) -> list[str]:
    violations: list[str] = []
    views_root = mdl_dir / "views"
    if not views_root.is_dir():
        return violations
    declared = {p.name for p in (mdl_dir / "models").glob("*/")} | {
        p.name for p in views_root.glob("*/")
    }
    for view_dir in sorted(p for p in views_root.iterdir() if p.is_dir()):
        path = view_dir / "metadata.yml"
        data, metadata = _load_yaml(path)
        if data:
            violations.append(data)
            continue
        if not isinstance(metadata, dict):
            violations.append(f"{path}: must be a mapping")
            continue
        statement = metadata.get("statement", "")
        if not isinstance(statement, str) or not statement.strip():
            violations.append(f"{path}: 'statement' must be a non-empty SQL SELECT")
            continue
        if _VIEW_FORBIDDEN_KEYWORDS.match(statement):
            violations.append(
                f"{path}: view statements must be read-only SELECTs (read-only discipline, §2.3)"
            )
        for model in sorted(declared):
            if re.search(rf"\b{re.escape(model)}\b", statement):
                break
        else:
            violations.append(
                f"{path}: statement references no declared model or view — views must be "
                f"grounded in the modeled surface"
            )
    return violations


def check_coupling(mdl_dir: Path, dbt_dir: Path) -> list[str]:
    """Spec §3.3 gate: dbt grain/column changes require the MDL update in the same PR."""
    violations: list[str] = []
    models = load_dbt_models(dbt_dir / "manifest.json", dbt_dir / "catalog.json")

    mdl_models: dict[str, dict[str, Any]] = {}
    for model_dir in sorted((mdl_dir / "models").glob("*/")):
        _, metadata = _load_yaml(model_dir / "metadata.yml")
        if isinstance(metadata, dict):
            mdl_models[metadata.get("name", model_dir.name)] = metadata

    for name, dbt_model in sorted(models.items()):
        metadata = mdl_models.get(name)
        if metadata is None:
            violations.append(
                f"dbt model {name} ({dbt_model.schema}) has no MDL model under models/{name}/ — "
                f"add it or extend the coverage boundary deliberately"
            )
            continue
        ref = metadata.get("table_reference") or {}
        if ref.get("schema") != dbt_model.schema or ref.get("table") != dbt_model.name:
            violations.append(
                f"{name}: MDL table_reference {ref.get('schema')}.{ref.get('table')} does not "
                f"match dbt {dbt_model.schema}.{dbt_model.name}"
            )
        mdl_cols = {
            c.get("name"): normalize_type(c.get("type", ""))
            for c in metadata.get("columns", [])
            if isinstance(c, dict)
        }
        dbt_cols = {col: normalize_type(dtype) for col, dtype in dbt_model.columns}
        if mdl_cols != dbt_cols:
            missing = sorted(set(dbt_cols) - set(mdl_cols))
            extra = sorted(set(mdl_cols) - set(dbt_cols))
            changed = sorted(
                f"{c}: {dbt_cols[c]} -> {mdl_cols[c]}"
                for c in set(dbt_cols) & set(mdl_cols)
                if dbt_cols[c] != mdl_cols[c]
            )
            detail = [
                p
                for p in (f"missing={missing}", f"extra={extra}", f"changed={changed}")
                if not p.endswith("=[]")
            ]
            violations.append(
                f"{name}: MDL columns drifted from dbt catalog ({'; '.join(detail)}) — update "
                f"models/{name}/metadata.yml in the same PR (spec §3.3)"
            )
        grain = {c for c in dbt_model.unique_columns}
        if grain != _primary_key_columns(metadata):
            violations.append(
                f"{name}: grain changed — dbt declares unique {sorted(grain)}, MDL primary_key "
                f"is {sorted(_primary_key_columns(metadata))} — update the MDL in the same PR"
            )

    modeled_dbt = set(models)
    for name in sorted(set(mdl_models) - modeled_dbt):
        violations.append(
            f"MDL model {name} has no modeled dbt counterpart — MDL maps 1:1 to dbt marts; "
            f"remove it or move it behind the generator"
        )
    return violations


def check_executable(mdl_dir: Path, duckdb_path: Path) -> list[str]:
    """Prove every declared column selects from the built DuckDB file (read-only)."""
    violations: list[str] = []
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        for model_dir in sorted((mdl_dir / "models").glob("*/")):
            _, metadata = _load_yaml(model_dir / "metadata.yml")
            if not isinstance(metadata, dict):
                continue  # structural violations reported elsewhere
            ref = metadata.get("table_reference") or {}
            columns = [
                c["name"]
                for c in metadata.get("columns", [])
                if isinstance(c, dict) and c.get("name")
            ]
            if not ref.get("table") or not columns:
                continue
            qualified = ".".join(f'"{ref[k]}"' for k in ("catalog", "schema") if ref.get(k))
            qualified = f"{qualified}.{ref['table']}" if qualified else str(ref["table"])
            collist = ", ".join(f'"{c}"' for c in columns)
            try:
                con.execute(f"SELECT {collist} FROM {qualified} LIMIT 0")
            except (duckdb.Error, RuntimeError) as exc:
                violations.append(
                    f"{metadata.get('name')}: not executable against {duckdb_path}: {exc}"
                )
    finally:
        con.close()
    return violations


def validate_project(
    mdl_dir: Path, dbt_dir: Path | None = None, duckdb_path: Path | None = None
) -> list[str]:
    """Run every enabled layer; returns the violation list (empty = valid)."""
    violations: list[str] = []
    violations.extend(_validate_project_manifest(mdl_dir))
    violations.extend(_validate_models(mdl_dir))
    violations.extend(_validate_relationships(mdl_dir))
    violations.extend(_validate_views(mdl_dir))
    if dbt_dir is not None:
        violations.extend(check_coupling(mdl_dir, dbt_dir))
    if duckdb_path is not None:
        violations.extend(check_executable(mdl_dir, duckdb_path))
    return violations
