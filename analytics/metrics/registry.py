"""Semantic metric registry over the canonical dbt warehouse.

Port of the cohortc pattern (architecture review [Data] P1): every metric
population is named exactly once in a version-controlled registry, qualified
by source system (``metric@source_system``), and carries its MEASURED size.
Resolution refuses anything it cannot resolve unambiguously — an unknown name
or a bare name with several registered definitions is a loud error, never a
guess.

dbt stays the single source of truth. Each entry binds to a dbt model and may
only reference columns that model declares in the dbt manifest; entries bound
to a marts model must reference the dbt column bare — the registry never
restates mart math. :func:`check_against_manifest` enforces both, and
:func:`check_population_sizes` re-measures every recorded population against
the dbt-built warehouse.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import yaml

from genbi.mdl_gen.model import MARTS_SCHEMA, is_modeled_model

DEFAULT_REGISTRY_PATH = Path(__file__).with_name("registry.yaml")
SCHEMA_VERSION = 1

# A qualified name is `metric@source_system` — the source_system side must
# match the value stamped by the connector (connectors/base.py erp_id stamp).
_QUALIFIED_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*@[a-z][a-z0-9_]*$")
_BARE_COLUMN_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_IDENTIFIER_RE = re.compile(r"\b[a-z_][a-z0-9_]*\b")

# SQL vocabulary a registry expression may use without it counting as a column
# reference. Anything outside this set must be a column the dbt manifest
# declares on the bound model.
_SQL_KEYWORDS = frozenset(
    {
        "abs",
        "and",
        "as",
        "avg",
        "case",
        "cast",
        "coalesce",
        "count",
        "date_diff",
        "distinct",
        "else",
        "end",
        "false",
        "filter",
        "greatest",
        "in",
        "interval",
        "is",
        "least",
        "max",
        "min",
        "not",
        "null",
        "nullif",
        "or",
        "round",
        "sum",
        "then",
        "true",
        "when",
        "where",
    }
)


class MetricRegistryError(Exception):
    """Base refusal: the registry refuses rather than guesses."""


class RegistrySchemaError(MetricRegistryError):
    """The registry YAML violates its own invariants and cannot be trusted."""


class UnknownMetricError(MetricRegistryError):
    """The name is not in the registry — the registry is a closed vocabulary."""


class AmbiguousMetricError(MetricRegistryError):
    """A bare name matches several registered definitions."""


@dataclass(frozen=True)
class Population:
    """The countable rows behind a metric, with its measured size."""

    count_sql: str
    measured: int


@dataclass(frozen=True)
class MetricDefinition:
    """One registry entry: a qualified name bound to a dbt model + expression."""

    name: str
    description: str
    dbt_model: str
    expression: str
    population: Population

    @property
    def metric(self) -> str:
        return self.name.split("@", 1)[0]

    @property
    def source(self) -> str:
        return self.name.split("@", 1)[1]


class MetricRegistry:
    """Immutable set of definitions with cohortc-style resolution semantics."""

    def __init__(self, entries: Sequence[MetricDefinition]) -> None:
        self._entries = tuple(entries)
        self._by_name: dict[str, MetricDefinition] = {}
        self._by_metric: dict[str, list[MetricDefinition]] = {}
        seen_definitions: set[tuple[str, str, str]] = set()
        for entry in self._entries:
            if entry.name in self._by_name:
                raise RegistrySchemaError(
                    f"metric {entry.name!r} is registered more than once — every metric "
                    "is named exactly once"
                )
            # The definition identity includes the source qualifier: two ERPs
            # legitimately share the same predicate over one fact model while
            # remaining different populations.
            definition_key = (entry.dbt_model, entry.expression, entry.source)
            if definition_key in seen_definitions:
                raise RegistrySchemaError(
                    f"metric {entry.name!r} redefines an existing definition "
                    f"({entry.dbt_model}, {entry.expression!r}, {entry.source!r}) — a "
                    "definition carries exactly one name"
                )
            seen_definitions.add(definition_key)
            self._by_name[entry.name] = entry
            self._by_metric.setdefault(entry.metric, []).append(entry)

    @property
    def entries(self) -> tuple[MetricDefinition, ...]:
        return self._entries

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._by_name))

    def resolve(self, name: str) -> MetricDefinition:
        """Resolve a metric name, refusing unknown or ambiguous references.

        A qualified name (``open_po@csv_sftp``) must exist exactly. A bare name
        (``open_po``) resolves only while exactly one definition is registered
        under it; with several, raise :class:`AmbiguousMetricError` listing
        every definition with its measured size — name one explicitly.
        """
        if "@" in name:
            entry = self._by_name.get(name)
            if entry is None:
                raise UnknownMetricError(self._unknown_message(name))
            return entry
        candidates = self._by_metric.get(name, [])
        if not candidates:
            raise UnknownMetricError(self._unknown_message(name))
        if len(candidates) > 1:
            raise AmbiguousMetricError(self._ambiguous_message(name, candidates))
        return candidates[0]

    def _unknown_message(self, name: str) -> str:
        valid = ", ".join(self.names()) or "<registry is empty>"
        return (
            f"unknown metric {name!r} — the metric registry is a closed vocabulary, "
            f"refusing to guess. Registered metric names: {valid}"
        )

    def _ambiguous_message(self, name: str, candidates: Sequence[MetricDefinition]) -> str:
        lines = [
            f"ambiguous metric {name!r} — {len(candidates)} registered definitions; "
            "name one explicitly:"
        ]
        for entry in candidates:
            lines.append(f"  - {entry.name}: {entry.population.measured} rows ({entry.dbt_model})")
        lines.append("Refusing to guess: the definitions may differ.")
        return "\n".join(lines)


def load_registry(path: Path | None = None) -> MetricRegistry:
    """Load and validate the registry YAML. Invalid structure fails loudly."""
    registry_path = path if path is not None else DEFAULT_REGISTRY_PATH
    document = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    if not isinstance(document, dict) or document.get("schema_version") != SCHEMA_VERSION:
        raise RegistrySchemaError(f"{registry_path}: schema_version must be {SCHEMA_VERSION}")
    raw_entries = document.get("metrics")
    if not isinstance(raw_entries, list) or not raw_entries:
        raise RegistrySchemaError(f"{registry_path}: must declare a non-empty 'metrics' list")
    return MetricRegistry(tuple(_parse_entry(raw, registry_path) for raw in raw_entries))


def _parse_entry(raw: Any, path: Path) -> MetricDefinition:
    if not isinstance(raw, dict):
        raise RegistrySchemaError(f"{path}: each metric entry must be a mapping")
    missing = [
        field
        for field in ("name", "description", "dbt_model", "expression", "population")
        if raw.get(field) in (None, "")
    ]
    if missing:
        raise RegistrySchemaError(
            f"{path}: metric {raw.get('name')!r} is missing {', '.join(missing)}"
        )
    name = raw["name"]
    if not _QUALIFIED_NAME_RE.match(name):
        raise RegistrySchemaError(
            f"{path}: metric name {name!r} must be qualified as "
            "'metric@source_system' (lowercase snake_case on both sides)"
        )
    population_raw = raw["population"]
    if not isinstance(population_raw, dict):
        raise RegistrySchemaError(f"{path}: metric {name!r} population must be a mapping")
    count_sql = population_raw.get("count_sql")
    measured = population_raw.get("measured")
    if not isinstance(count_sql, str) or not count_sql.strip():
        raise RegistrySchemaError(
            f"{path}: metric {name!r} population.count_sql must be a non-empty SQL string"
        )
    if isinstance(measured, bool) or not isinstance(measured, int) or measured < 1:
        raise RegistrySchemaError(
            f"{path}: metric {name!r} population.measured must be a positive integer — "
            "the registry records measured sizes, never zero or a guess"
        )
    return MetricDefinition(
        name=name,
        description=str(raw["description"]).strip(),
        dbt_model=raw["dbt_model"],
        expression=str(raw["expression"]).strip(),
        population=Population(count_sql=count_sql.strip(), measured=measured),
    )


def _expression_identifiers(expression: str) -> set[str]:
    """Column identifiers referenced by a registry expression."""
    return {
        token for token in _IDENTIFIER_RE.findall(expression.lower()) if token not in _SQL_KEYWORDS
    }


def _is_bare_column(expression: str) -> bool:
    return _BARE_COLUMN_RE.match(expression.strip()) is not None


def _normalized_sql(sql: str) -> str:
    return " ".join(sql.split())


def check_against_manifest(registry: MetricRegistry, manifest: dict[str, Any]) -> list[str]:
    """Cross-check every binding against the dbt manifest.

    dbt metadata stays the single source of truth: a metric may only bind to a
    modeled dbt model (canonical facts / marts — not staging, crosswalks, or
    reference plumbing) and reference columns that model declares. Returns
    violation strings; an empty list means the registry is consistent with dbt.
    """
    models = {
        node["name"]: node
        for node in manifest.get("nodes", {}).values()
        if node.get("resource_type") == "model"
    }
    violations: list[str] = []
    for entry in registry.entries:
        node = models.get(entry.dbt_model)
        if node is None:
            violations.append(
                f"{entry.name}: binds to dbt model {entry.dbt_model!r}, which the "
                "manifest does not define"
            )
            continue
        schema = node.get("schema", "")
        if not is_modeled_model(schema, entry.dbt_model):
            violations.append(
                f"{entry.name}: binds to {schema}.{entry.dbt_model}, which is outside "
                "the modeled business surface (staging/crosswalk/reference plumbing)"
            )
            continue
        relation = f"{schema}.{entry.dbt_model}"
        declared = set(node.get("columns", {}))
        undeclared = _expression_identifiers(entry.expression) - declared
        if undeclared:
            violations.append(
                f"{entry.name}: expression references columns dbt does not declare on "
                f"{relation}: {sorted(undeclared)}"
            )
        if schema == MARTS_SCHEMA and not _is_bare_column(entry.expression):
            violations.append(
                f"{entry.name}: mart-bound expression must be a bare dbt column "
                "reference — the registry never restates mart math"
            )
        count_sql = _normalized_sql(entry.population.count_sql)
        if relation not in count_sql:
            violations.append(
                f"{entry.name}: population.count_sql must query its bound relation {relation}"
            )
        if "source_system" in declared and f"source_system = '{entry.source}'" not in count_sql:
            violations.append(
                f"{entry.name}: population.count_sql must scope the population to "
                f"source_system = '{entry.source}'"
            )
    return violations


def provenance_sql(entry: MetricDefinition) -> str:
    """The count query with its definition attached, cohortc-style.

    A measured population that travels (CI logs, decks, tickets) arrives with
    its definition and size, not as a bare number.
    """
    return (
        f"-- metric: {entry.name}\n"
        f"-- definition: {entry.description}\n"
        f"-- dbt model: {entry.dbt_model} · measured: {entry.population.measured} rows\n"
        f"{entry.population.count_sql}"
    )


CountExecutor = Callable[[str, str], int]


def execute_population_count(duckdb_path: str, sql: str) -> int:
    """Effectful edge: run a registry count query read-only against the warehouse."""
    con = duckdb.connect(duckdb_path, read_only=True)
    try:
        row = con.execute(sql).fetchone()
        if row is None or row[0] is None:
            raise ValueError(f"count query returned no rows: {sql[:120]}")
        return int(row[0])
    finally:
        con.close()


def check_population_sizes(
    registry: MetricRegistry,
    duckdb_path: str,
    executor: CountExecutor = execute_population_count,
) -> list[str]:
    """Re-measure every registered population against the built warehouse.

    The registry records measured sizes; any drift between the recorded size
    and the warehouse is a violation — the registry must be re-measured, not
    assumed.
    """
    violations: list[str] = []
    for entry in registry.entries:
        actual = executor(duckdb_path, provenance_sql(entry))
        if actual != entry.population.measured:
            violations.append(
                f"{entry.name}: registry records {entry.population.measured} rows but the "
                f"warehouse measures {actual} — re-measure and update the registry "
                "(data changed)"
            )
    return violations
