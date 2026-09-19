"""DB-API batch extraction shared by the legacy SQL connector pack (spec §5).

Every legacy database row in the coverage matrix lands in the warehouse the
same way: a watermark-driven ``SELECT`` over a DB-API connection, rows mapped
positionally onto the canonical entity columns, streamed through the
:class:`BaseConnector` contract (provenance stamping, Parquet promotion,
watermark persistence) and reconciled for hard deletes by the §6 anti-join.

What varies per ERP is declared, not coded:

* :class:`EntitySource` — where each canonical entity lives (table, canonical
  column ← source column map, watermark column).
* ``param_placeholder`` — DB-API paramstyle of the driver (``?`` for pyodbc /
  ODBC and JDBC bridges, ``%s`` for psycopg2 / PyMySQL).
* ``_build_default_connection_factory`` — the driver plumbing; every connector
  also accepts an injected factory, which is how the fixture-based tests (and
  any site that needs a custom transport, e.g. JayDeBeApi over JT400) reach
  the extraction logic without a live ERP.

Watermark semantics: the incremental predicate compares the source watermark
column against the stored checkpoint with ``>`` — ISO dates/timestamps compare
correctly as strings. The first incremental run (no stored checkpoint) scans
the full history, identical to backfill, and then advances the checkpoint.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, ClassVar, Protocol

from connectors.base import (
    BaseConnector,
    ConnectorError,
    ExtractionMode,
    ExtractionPlan,
    natural_id_for,
)
from connectors.legacy.schemas import (
    CANONICAL_ENTITY_COLUMNS,
    NATURAL_KEY_FIELDS,
    canonical_arrow_schema,
)

#: Settings-declared identifiers (table, column, soft-delete column) are
#: interpolated into SQL — restricted to conservative identifier syntax and
#: validated before use. Values are always bound parameters, never interpolated.
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]{0,127}$")


def validate_identifier(value: str, role: str) -> str:
    """Reject settings-declared SQL identifiers outside conservative syntax."""
    if not _IDENTIFIER.match(value):
        raise ConnectorError(
            f"invalid {role} identifier {value!r}: must match {_IDENTIFIER.pattern}"
        )
    return value


class DbApiCursor(Protocol):
    """The DB-API 2.0 surface the extractor uses."""

    description: Any

    def execute(self, operation: str, parameters: Any = None) -> None: ...

    def fetchall(self) -> list[tuple]: ...


class DbApiConnection(Protocol):
    def cursor(self) -> DbApiCursor: ...

    def close(self) -> None: ...


#: Builds the driver connection from the source's resolved settings. Injected
#: by tests (fake connections over fixture rows); production connectors build
#: one lazily around their driver import (never at import time — the driver
#: may legitimately be absent until a site provisions it).
ConnectionFactory = Any  # Callable[[Mapping[str, str]], DbApiConnection]


@dataclass(frozen=True)
class EntitySource:
    """Where one canonical entity lives in a legacy SQL source.

    ``columns`` maps canonical staging columns to source columns; ``None``
    means identity (the source table already uses canonical names — typical
    when extraction reads a staging view agreed at onboarding).
    """

    table: str
    columns: tuple[tuple[str, str], ...] | None = None  # (canonical, source)
    incremental_column: str | None = None  # source-side watermark column


class DbApiBatchConnector(BaseConnector):
    """Batch extraction over a DB-API connection for canonical entities.

    Subclasses declare ``entity_sources``, ``transport_label``, and the driver
    factory; everything else (watermark SQL, positional row mapping, key
    inventory for the anti-join, Arrow schemas) is shared here and tested once.
    """

    #: Driver paramstyle: ``?`` (ODBC/JDBC bridges) or ``%s`` (psycopg2, PyMySQL).
    param_placeholder: ClassVar[str] = "?"
    #: Human-readable transport used in extraction plans, e.g. "ODBC (DataDirect)".
    transport_label: ClassVar[str] = "DB-API connection"
    #: Settings that must resolve before a live connection is attempted.
    required_settings: ClassVar[tuple[str, ...]] = ()

    #: entity -> location of the entity in the source database.
    entity_sources: ClassVar[dict[str, EntitySource]] = {}

    #: Every legacy connector stages the canonical entities, so the shared
    #: natural-key registry is the default; a connector may still override.
    natural_key_fields: ClassVar[dict[str, tuple[str, ...]]] = NATURAL_KEY_FIELDS

    def __init__(self, source, store, config, connection_factory: ConnectionFactory | None = None):
        super().__init__(source, store, config)
        self._connection_factory = connection_factory or self._build_default_connection_factory()
        self._max_incremental_seen: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Contract surface
    # ------------------------------------------------------------------

    def entities(self) -> list[str]:
        return list(self.entity_sources)

    def arrow_schema(self, entity: str):
        return canonical_arrow_schema(entity)

    def validate_config(self) -> list[str]:
        missing = [name for name in self.required_settings if not self.source.settings.get(name)]
        if missing:
            return [
                f"missing settings required for live extraction: {', '.join(missing)} "
                "(the source stays disabled until site discovery completes)"
            ]
        return []

    def describe_extraction(self, entity: str) -> ExtractionPlan:
        src = self._entity_source(entity)
        predicate = ""
        if src.incremental_column:
            predicate = f" WHERE {src.incremental_column} > {self.param_placeholder} (watermark)"
        return ExtractionPlan(
            entity=entity,
            surface=f"{self.transport_label}: SELECT canonical columns FROM {src.table}{predicate}",
            incremental_key=src.incremental_column,
            notes=self.extraction_notes,
        )

    # ------------------------------------------------------------------
    # Extraction
    # ------------------------------------------------------------------

    def _iter_records(
        self, entity: str, mode: ExtractionMode, watermark: str | None
    ) -> Iterator[dict[str, object]]:
        src = self._entity_source(entity)
        canonical_names, sql, params = self._select_sql(entity, src, mode, watermark)
        # The watermark column rides at the tail of every SELECT so incremental
        # runs can observe it; it is observed, never staged (it is not a
        # canonical column and the Arrow schema would drop it anyway).
        # The driver returns it in both modes — _select_sql appends it
        # unconditionally — so the width check must count it in BACKFILL too.
        observe = src.incremental_column is not None and mode is ExtractionMode.INCREMENTAL
        expected_width = len(canonical_names) + (1 if src.incremental_column is not None else 0)
        cursor = self._connect().cursor()
        try:
            cursor.execute(sql, params or None)
            rows = cursor.fetchall()
        finally:
            cursor.close()
        for row in rows:
            if len(row) != expected_width:
                raise ConnectorError(
                    f"source returned {len(row)} columns for {self.erp_id}/{entity}; "
                    f"expected {expected_width} — column map is stale"
                )
            record: dict[str, object] = dict(
                zip(canonical_names, row[: len(canonical_names)], strict=True)
            )
            if observe:
                assert src.incremental_column is not None
                self._observe_incremental(
                    entity, {src.incremental_column: row[len(canonical_names)]}
                )
            yield record

    def source_key_inventory(self, entity: str) -> set[str]:
        """Full key scan of the source table — the anti-join's source side."""
        src = self._entity_source(entity)
        key_fields = self.natural_key_fields[entity]
        source_cols = self._source_columns(entity, tuple(key_fields))
        select_list = ", ".join(
            f"{col} AS {validate_identifier(canon, 'alias')}" for canon, col in source_cols
        )
        sql = f"SELECT DISTINCT {select_list} FROM {src.table}"
        params: tuple[object, ...] = ()
        soft_delete = self._soft_delete_predicate()
        if soft_delete is not None:
            column, value = soft_delete
            sql += f" WHERE {column} <> {self.param_placeholder}"
            params = (value,)
        cursor = self._connect().cursor()
        try:
            cursor.execute(sql, params or None)
            rows = cursor.fetchall()
        finally:
            cursor.close()
        canonical_key_names = [canon for canon, _ in source_cols]
        return {
            natural_id_for(
                tuple(canonical_key_names),
                dict(
                    zip(
                        canonical_key_names,
                        row[: len(canonical_key_names)],
                        strict=True,
                    )
                ),
            )
            for row in rows
        }

    def current_watermark(
        self, entity: str, mode: ExtractionMode, watermark_before: str | None
    ) -> str | None:
        """Advance to the newest watermark value this extraction observed."""
        if mode is ExtractionMode.INCREMENTAL:
            return self._max_incremental_seen.get(entity, watermark_before)
        return watermark_before

    # ------------------------------------------------------------------
    # SQL construction (shared, tested once)
    # ------------------------------------------------------------------

    def _select_sql(
        self,
        entity: str,
        src: EntitySource,
        mode: ExtractionMode,
        watermark: str | None,
    ) -> tuple[list[str], str, tuple[object, ...]]:
        """Build (canonical column names, SELECT SQL, bound parameters).

        When the entity declares an incremental column it is appended to the
        SELECT tail (after the mapped canonical columns) so incremental runs
        can observe the newest value — it is read positionally by
        ``_iter_records`` and never staged.
        """
        mapped = self._source_columns(entity, None)  # full column list
        select_list = ", ".join(
            f"{source} AS {validate_identifier(canon, 'alias')}" for canon, source in mapped
        )
        params: tuple[object, ...] = ()
        if src.incremental_column is not None:
            validate_identifier(src.incremental_column, "watermark column")
            select_list += f", {src.incremental_column}"
            if mode is ExtractionMode.INCREMENTAL and watermark:
                params = (watermark,)
        sql = f"SELECT {select_list} FROM {src.table}"
        if params:
            sql += f" WHERE {src.incremental_column} > {self.param_placeholder}"
        return [canon for canon, _ in mapped], sql, params

    def _source_columns(self, entity: str, only: tuple[str, ...] | None):
        """(canonical, source) pairs — filtered to ``only`` canonical names."""
        src = self._entity_source(entity)
        spec = src.columns or self._identity_columns(entity)
        declared = {canon: source for canon, source in spec}
        wanted = only if only is not None else [canon for canon, _ in spec]
        missing = [name for name in wanted if name not in declared]
        if missing:
            raise ConnectorError(
                f"entity '{entity}' column map for {self.erp_id} does not declare "
                f"canonical columns {missing}"
            )
        return [(canon, validate_identifier(declared[canon], "source column")) for canon in wanted]

    def _identity_columns(self, entity: str):
        """Identity map: source columns already carry canonical names."""
        return tuple((name, name) for name, _ in _canonical_spec(entity))

    def _soft_delete_predicate(self) -> tuple[str, object] | None:
        """Optional soft-delete filter for the key inventory (spec: Sybase ASE).

        ``soft_delete_column`` names the flag; ``soft_delete_value`` is the
        value that MARKS a row deleted (e.g. ``1`` for a 0=active/1=deleted
        flag) — rows equal to it are excluded from the key inventory. Both
        must be set together.
        """
        column = self.source.settings.get("soft_delete_column")
        if not column:
            if self.source.settings.get("soft_delete_value"):
                raise ConnectorError(
                    f"connector '{self.erp_id}': soft_delete_value is set without "
                    "soft_delete_column — set both or neither"
                )
            return None
        value = self.source.settings.get("soft_delete_value")
        if not value:
            raise ConnectorError(
                f"connector '{self.erp_id}': soft_delete_column requires "
                "soft_delete_value (the value that marks a row deleted)"
            )
        return validate_identifier(column, "soft-delete column"), value

    def _entity_source(self, entity: str) -> EntitySource:
        src = self.entity_sources.get(entity)
        if src is None:
            raise ConnectorError(
                f"connector '{self.erp_id}' does not declare an entity source for "
                f"'{entity}'; declared: {', '.join(sorted(self.entity_sources))}"
            )
        validate_identifier(src.table, "table")
        return src

    def _observe_incremental(self, entity: str, record: dict[str, object]) -> None:
        src = self.entity_sources[entity]
        assert src.incremental_column is not None
        observed = record.get(src.incremental_column)
        if observed is None:
            return
        text = str(observed)
        current = self._max_incremental_seen.get(entity)
        if current is None or text > current:
            self._max_incremental_seen[entity] = text

    def _connect(self):
        return self._connection_factory(self.source.settings)

    # ------------------------------------------------------------------
    # Driver plumbing (subclasses override; tests inject a factory instead)
    # ------------------------------------------------------------------

    def _build_default_connection_factory(self) -> ConnectionFactory:
        raise ConnectorError(
            f"connector '{self.erp_id}' does not build a default connection; configure "
            "its driver transport or inject a connection factory (tests/fixtures)"
        )


def _canonical_spec(entity: str):
    spec = CANONICAL_ENTITY_COLUMNS.get(entity)
    if spec is None:
        raise ConnectorError(f"entity '{entity}' is not a canonical entity")
    return spec
