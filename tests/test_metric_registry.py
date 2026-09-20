"""Tests for the semantic metric registry (analytics/metrics)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import pytest
import yaml

from analytics.evals import run_evals
from analytics.metrics.registry import (
    AmbiguousMetricError,
    MetricDefinition,
    MetricRegistry,
    Population,
    RegistrySchemaError,
    UnknownMetricError,
    check_against_manifest,
    check_population_sizes,
    load_registry,
    provenance_sql,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
REGISTRY_PATH = REPO_ROOT / "analytics" / "metrics" / "registry.yaml"
GOLDEN_PATH = REPO_ROOT / "analytics" / "evals" / "golden_qa.yaml"


# ---------------------------------------------------------------------------
# Fixtures and builders


def make_entry(**overrides: Any) -> MetricDefinition:
    fields: dict[str, Any] = {
        "name": "open_po@csv_sftp",
        "description": "Open purchase-order lines for the csv_sftp dealer feed.",
        "dbt_model": "fact_purchase_order_line",
        "expression": "count(*) filter (where not is_received_in_full)",
        "population": Population(
            count_sql=(
                "select count(*) from main_canonical.fact_purchase_order_line "
                "where source_system = 'csv_sftp' and not is_received_in_full"
            ),
            measured=69,
        ),
    }
    fields.update(overrides)
    return MetricDefinition(**fields)


def make_headline_entry(**overrides: Any) -> MetricDefinition:
    fields: dict[str, Any] = {
        "name": "gmroi@csv_sftp",
        "description": "GMROI headline value.",
        "dbt_model": "kpi_headline",
        "expression": "gmroi",
        "population": Population(
            count_sql="select count(*) from main_marts.kpi_headline",
            measured=1,
        ),
    }
    fields.update(overrides)
    return MetricDefinition(**fields)


def registry_entry_dict(**overrides: Any) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "name": "open_po@csv_sftp",
        "description": "Open purchase-order lines for the csv_sftp dealer feed.",
        "dbt_model": "fact_purchase_order_line",
        "expression": "count(*) filter (where not is_received_in_full)",
        "population": {
            "count_sql": (
                "select count(*) from main_canonical.fact_purchase_order_line "
                "where source_system = 'csv_sftp' and not is_received_in_full"
            ),
            "measured": 69,
        },
    }
    entry.update(overrides)
    return entry


def headline_entry_dict(**overrides: Any) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "name": "gmroi@csv_sftp",
        "description": "GMROI headline value.",
        "dbt_model": "kpi_headline",
        "expression": "gmroi",
        "population": {
            "count_sql": "select count(*) from main_marts.kpi_headline",
            "measured": 1,
        },
    }
    entry.update(overrides)
    return entry


def write_registry(directory: Path, metrics: list[dict[str, Any]]) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "registry.yaml"
    path.write_text(
        yaml.safe_dump({"schema_version": 1, "metrics": metrics}, sort_keys=False),
        encoding="utf-8",
    )
    return path


def model_node(name: str, schema: str, columns: list[str]) -> dict[str, Any]:
    return {
        "name": name,
        "resource_type": "model",
        "schema": schema,
        "columns": {column: {"name": column} for column in columns},
    }


def build_manifest(with_plumbing: bool) -> dict[str, Any]:
    nodes: dict[str, Any] = {
        "model.demo.fact_purchase_order_line": model_node(
            "fact_purchase_order_line",
            "main_canonical",
            [
                "po_line_key",
                "source_system",
                "ordered_qty",
                "received_qty",
                "is_received_in_full",
            ],
        ),
        "model.demo.kpi_headline": model_node(
            "kpi_headline", "main_marts", ["gmroi", "avg_ticket"]
        ),
    }
    if with_plumbing:
        nodes["model.demo.stg_csvsftp__purchase_order_lines"] = model_node(
            "stg_csvsftp__purchase_order_lines", "main_staging", ["po_no", "ordered_qty"]
        )
        nodes["model.demo.crosswalk_source_vendor"] = model_node(
            "crosswalk_source_vendor",
            "main_canonical",
            ["source_system", "source_key", "canonical_vendor_key"],
        )
    return {"nodes": nodes}


@pytest.fixture
def demo_manifest() -> dict[str, Any]:
    return build_manifest(with_plumbing=True)


def build_mini_warehouse(path: Path) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute("create schema main_canonical")
        con.execute(
            "create table main_canonical.fact_purchase_order_line ("
            "source_system varchar, is_received_in_full boolean)"
        )
        con.execute(
            "insert into main_canonical.fact_purchase_order_line values "
            "('csv_sftp', false), ('csv_sftp', true)"
        )
        con.execute("create schema main_marts")
        con.execute("create table main_marts.kpi_headline (gmroi double)")
        con.execute("insert into main_marts.kpi_headline values (1.73)")
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Real registry: load + resolution semantics


def test_real_registry_loads() -> None:
    registry = load_registry(REGISTRY_PATH)
    assert len(registry.entries) >= 22
    assert all("@" in entry.name for entry in registry.entries)


def test_real_registry_entries_carry_measured_populations() -> None:
    for entry in load_registry(REGISTRY_PATH).entries:
        assert entry.population.measured >= 1
        assert entry.population.count_sql.strip()


def test_resolve_qualified_name() -> None:
    registry = load_registry(REGISTRY_PATH)
    entry = registry.resolve("open_po@csv_sftp")
    assert entry.dbt_model == "fact_purchase_order_line"
    assert entry.source == "csv_sftp"


def test_resolve_bare_name_while_unambiguous() -> None:
    registry = load_registry(REGISTRY_PATH)
    assert registry.resolve("open_po") == registry.resolve("open_po@csv_sftp")


def test_unknown_metric_refused_with_closed_vocabulary() -> None:
    registry = load_registry(REGISTRY_PATH)
    with pytest.raises(UnknownMetricError) as excinfo:
        registry.resolve("open_po@dynamics")
    message = str(excinfo.value)
    assert "closed vocabulary" in message
    assert "open_po@csv_sftp" in message  # valid names are listed


def test_unknown_qualified_name_refused() -> None:
    registry = load_registry(REGISTRY_PATH)
    with pytest.raises(UnknownMetricError):
        registry.resolve("no_such_metric@csv_sftp")


def test_ambiguous_bare_name_refused_listing_definitions() -> None:
    registry = MetricRegistry(
        [
            make_entry(name="open_po@csv_sftp", description="csv_sftp open PO lines."),
            make_entry(
                name="open_po@dynamics",
                description="Dynamics open PO lines.",
                population=Population(
                    count_sql=(
                        "select count(*) from main_canonical.fact_purchase_order_line "
                        "where source_system = 'dynamics' and not is_received_in_full"
                    ),
                    measured=123,
                ),
            ),
        ]
    )
    with pytest.raises(AmbiguousMetricError) as excinfo:
        registry.resolve("open_po")
    message = str(excinfo.value)
    assert "open_po@csv_sftp" in message
    assert "open_po@dynamics" in message
    assert "69" in message and "123" in message  # measured sizes accompany every definition
    assert "Refusing to guess" in message
    # Qualified names still resolve each definition exactly.
    assert registry.resolve("open_po@csv_sftp").population.measured == 69
    assert registry.resolve("open_po@dynamics").population.measured == 123


# ---------------------------------------------------------------------------
# Registry validation (schema invariants)


def test_duplicate_name_refused() -> None:
    with pytest.raises(RegistrySchemaError) as excinfo:
        MetricRegistry([make_entry(), make_entry()])
    assert "named exactly once" in str(excinfo.value)


def test_duplicate_definition_refused() -> None:
    with pytest.raises(RegistrySchemaError) as excinfo:
        MetricRegistry(
            [
                make_entry(name="open_po@csv_sftp"),
                make_entry(name="open_pos@csv_sftp"),
            ]
        )
    assert "redefines" in str(excinfo.value)


def test_same_predicate_for_two_sources_is_not_a_redefinition() -> None:
    registry = MetricRegistry(
        [
            make_entry(name="open_po@csv_sftp"),
            make_entry(
                name="open_po@dynamics",
                population=Population(
                    count_sql=(
                        "select count(*) from main_canonical.fact_purchase_order_line "
                        "where source_system = 'dynamics' and not is_received_in_full"
                    ),
                    measured=123,
                ),
            ),
        ]
    )
    assert len(registry.entries) == 2


def test_registry_yaml_rejects_unqualified_name(tmp_path: Path) -> None:
    path = write_registry(tmp_path, [registry_entry_dict(name="open_po")])
    with pytest.raises(RegistrySchemaError) as excinfo:
        load_registry(path)
    assert "metric@source_system" in str(excinfo.value)


@pytest.mark.parametrize("bad_measured", [0, -1, "many", True])
def test_registry_yaml_rejects_non_positive_measured(tmp_path: Path, bad_measured: Any) -> None:
    entry = registry_entry_dict()
    entry["population"]["measured"] = bad_measured
    path = write_registry(tmp_path, [entry])
    with pytest.raises(RegistrySchemaError):
        load_registry(path)


def test_registry_yaml_rejects_missing_field(tmp_path: Path) -> None:
    entry = registry_entry_dict()
    del entry["expression"]
    path = write_registry(tmp_path, [entry])
    with pytest.raises(RegistrySchemaError) as excinfo:
        load_registry(path)
    assert "expression" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Manifest cross-check: dbt is the single source of truth


def test_cross_check_clean_registry(demo_manifest: dict[str, Any]) -> None:
    registry = MetricRegistry([make_entry(), make_headline_entry()])
    assert check_against_manifest(registry, demo_manifest) == []


def test_cross_check_unknown_model(demo_manifest: dict[str, Any]) -> None:
    registry = MetricRegistry([make_entry(dbt_model="fact_nonexistent")])
    violations = check_against_manifest(registry, demo_manifest)
    assert len(violations) == 1
    assert "manifest does not define" in violations[0]


def test_cross_check_undeclared_column(demo_manifest: dict[str, Any]) -> None:
    registry = MetricRegistry(
        [make_entry(expression="count(*) filter (where not is_fully_billed)")]
    )
    violations = check_against_manifest(registry, demo_manifest)
    assert len(violations) == 1
    assert "is_fully_billed" in violations[0]


def test_cross_check_mart_expression_must_be_bare_column(demo_manifest: dict[str, Any]) -> None:
    registry = MetricRegistry([make_headline_entry(expression="sum(gmroi)")])
    violations = check_against_manifest(registry, demo_manifest)
    assert len(violations) == 1
    assert "never restates mart math" in violations[0]


def test_cross_check_count_sql_must_query_bound_relation(demo_manifest: dict[str, Any]) -> None:
    entry = make_headline_entry(
        population=Population(count_sql="select count(*) from main_marts.kpi_inventory", measured=1)
    )
    violations = check_against_manifest(MetricRegistry([entry]), demo_manifest)
    assert len(violations) == 1
    assert "bound relation" in violations[0]


def test_cross_check_fact_population_must_scope_source(demo_manifest: dict[str, Any]) -> None:
    entry = make_entry(
        population=Population(
            count_sql=(
                "select count(*) from main_canonical.fact_purchase_order_line "
                "where not is_received_in_full"
            ),
            measured=69,
        )
    )
    violations = check_against_manifest(MetricRegistry([entry]), demo_manifest)
    assert len(violations) == 1
    assert "source_system = 'csv_sftp'" in violations[0]


def test_cross_check_refuses_staging_model(demo_manifest: dict[str, Any]) -> None:
    entry = make_entry(
        dbt_model="stg_csvsftp__purchase_order_lines",
        population=Population(
            count_sql=(
                "select count(*) from main_staging.stg_csvsftp__purchase_order_lines "
                "where source_system = 'csv_sftp'"
            ),
            measured=69,
        ),
    )
    violations = check_against_manifest(MetricRegistry([entry]), demo_manifest)
    assert len(violations) == 1
    assert "outside the modeled business surface" in violations[0]


def test_cross_check_refuses_crosswalk_model(demo_manifest: dict[str, Any]) -> None:
    entry = make_entry(
        dbt_model="crosswalk_source_vendor",
        expression="count(*)",
        population=Population(
            count_sql=(
                "select count(*) from main_canonical.crosswalk_source_vendor "
                "where source_system = 'csv_sftp'"
            ),
            measured=8,
        ),
    )
    violations = check_against_manifest(MetricRegistry([entry]), demo_manifest)
    assert len(violations) == 1
    assert "outside the modeled business surface" in violations[0]


# ---------------------------------------------------------------------------
# Population size check (CI size-check)


def test_size_check_drift_detected() -> None:
    registry = MetricRegistry([make_entry()])
    violations = check_population_sizes(registry, "unused.duckdb", executor=lambda path, sql: 68)
    assert len(violations) == 1
    assert "re-measure and update the registry" in violations[0]


def test_size_check_match_passes() -> None:
    registry = MetricRegistry([make_entry(), make_headline_entry()])
    measured_by_name = {"open_po@csv_sftp": 69, "gmroi@csv_sftp": 1}

    def executor(duckdb_path: str, sql: str) -> int:
        for name, size in measured_by_name.items():
            if f"-- metric: {name}" in sql:
                return size
        raise AssertionError(f"no measured size found for sql: {sql}")

    assert check_population_sizes(registry, "unused.duckdb", executor=executor) == []


def test_provenance_sql_carries_definition() -> None:
    entry = make_entry()
    sql = provenance_sql(entry)
    assert f"-- metric: {entry.name}" in sql
    assert entry.description in sql
    assert f"measured: {entry.population.measured} rows" in sql
    assert sql.endswith(entry.population.count_sql)


# ---------------------------------------------------------------------------
# Integration: real artifacts when built (CI dbt job runs these for real)


def test_real_registry_against_built_manifest() -> None:
    manifest_path = REPO_ROOT / "dbt" / "target" / "manifest.json"
    if not manifest_path.exists():
        pytest.skip("dbt artifacts not built locally; CI dbt job runs the cross-check")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert check_against_manifest(load_registry(REGISTRY_PATH), manifest) == []


def test_size_check_real_warehouse() -> None:
    mart = REPO_ROOT / "data" / "analytics" / "analytics.duckdb"
    if not mart.exists():
        pytest.skip("dbt mart not built locally; CI dbt job runs the size check")
    assert check_population_sizes(load_registry(REGISTRY_PATH), str(mart)) == []


def test_every_golden_metric_resolves_through_registry() -> None:
    registry = load_registry(REGISTRY_PATH)
    golden = run_evals.load_golden(GOLDEN_PATH)
    for case in golden["cases"]:
        entry = registry.resolve(case["metric"])
        assert entry.dbt_model == "kpi_headline"
        assert entry.name == f"{case['metric']}@csv_sftp"
        assert entry.expression == case["metric"]


# ---------------------------------------------------------------------------
# CLI gate (python -m analytics.metrics.check)


def test_cli_gate_passes_and_detects_drift(tmp_path: Path) -> None:
    from analytics.metrics import check

    warehouse = tmp_path / "analytics.duckdb"
    build_mini_warehouse(warehouse)
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(json.dumps(build_manifest(False)), encoding="utf-8")

    # Mini warehouse ground truth: exactly one open PO row and one headline row.
    entry = registry_entry_dict()
    entry["population"]["measured"] = 1
    passing = write_registry(tmp_path / "pass", [entry, headline_entry_dict()])
    assert (
        check.main(
            ["--registry", str(passing), "--dbt-dir", str(tmp_path), "--duckdb", str(warehouse)]
        )
        == 0
    )

    # Same registry with a stale measured size: the mini warehouse holds exactly
    # one open PO row, so recording 2 must fail the gate loudly.
    document = {"schema_version": 1, "metrics": [registry_entry_dict()]}
    document["metrics"][0]["population"]["measured"] = 2
    drifted = tmp_path / "drift" / "registry.yaml"
    drifted.parent.mkdir()
    drifted.write_text(yaml.safe_dump(document), encoding="utf-8")
    assert (
        check.main(
            ["--registry", str(drifted), "--dbt-dir", str(tmp_path), "--duckdb", str(warehouse)]
        )
        == 1
    )


def test_cli_gate_missing_manifest(tmp_path: Path) -> None:
    from analytics.metrics import check

    registry = write_registry(tmp_path, [registry_entry_dict()])
    assert (
        check.main(
            [
                "--registry",
                str(registry),
                "--dbt-dir",
                str(tmp_path),
                "--duckdb",
                "x.duckdb",
            ]
        )
        == 2
    )
