# Connector guide — write one in an afternoon

The `BaseConnector` contract (see `connectors/base.py`) is small on purpose.
A connector declares config, yields canonical staging records, and lets the
SDK handle provenance, watermarks, Parquet writing, and idempotency.

## The contract

```python
class BaseConnector(ABC):
    def register(self) -> SourceRegistrationRecord   # upsert source in the control plane
    def entities(self) -> list[str]                 # extractable entity names
    def arrow_schema(self, entity) -> pa.Schema     # explicit Arrow schema (stability!)
    def extract(self, entity, mode) -> ExtractedEntityResult
    def plan(self, entity) -> list[ExtractionStep]  # dry-run description
```

Rules the contract enforces:
1. **`extract` is a generator** of dict rows; the SDK batches to Parquet.
2. **Every row carries provenance** — the SDK stamps `source_system`,
   `source_id`, `batch_id`, `loaded_at`; you supply `source_file` and any
   source-native keys.
3. **Incremental mode** reads the watermark from the control-plane store and
   only yields newer rows; **backfill** re-reads everything. Idempotency is by
   content hash (files) or high-water column (APIs).
4. **Never fabricate behavior** for systems you cannot reach: raise
   `ConnectorNotImplemented` (see `connectors/stubs/skeleton.py`).

## Worked example: extending the CSV/SFTP connector

Suppose an acquired dealer exports a new `credit_memos.csv` with a manifest.

1. **Declare the schema** — append to `connectors/csv_sftp/schemas.yml`:
   ```yaml
   credit_memos:
     required: [memo_id, customer_id, memo_date, amount]
     columns:
       memo_id: string
       customer_id: string
       memo_date: date
       amount: decimal(18,2)
   ```
2. **Register the entity** — add `credit_memos` to the connector's
   `entities()` and to the manifest contract (file name, checksum, row count
   are validated automatically by `manifest.py`).
3. **Map to canonical** (if new entity type) — add a staging model
   `dbt/models/staging/csvsftp_ridgeline/stg_csvsftp__credit_memos.sql`,
   then the canonical fact or bridge. Existing entity? Only step 1–2.
4. **Test** — the contract suite (`tests/test_connector_contract.py`) runs
   against every registered source automatically; add a fixture CSV under
   `seed/` and one seed-integrity assertion in `tests/test_seed_integrity.py`.

That's the whole loop — schema, entity registration, (optional) canonical
model, fixture. The manifest gate, checksums, quarantine, watermark, and
provenance come free.

## Checklist for API/SQL connectors

- Credential names go in `connectors/sources.yml` with `${VAR}` references;
  values live only in the environment (`.env`), never in the repo.
- Validate required credentials in `register()` and fail loudly.
- Declare the incremental column (e.g. `last_modified_date`) in the plan.
- Write an explicit `arrow_schema()` — never let Arrow infer from batch 1.
- Mark the connector's maturity in `connectors/sources.yml`
  (`implemented` / `credential_gated` / `skeleton`) and say so in the README.

## Legacy SQL connectors (spec §5 rows 1–9)

Informix, Db2 LUW, Db2 for i, Oracle, SQL Server, PostgreSQL, MySQL/MariaDB,
Sybase ASE, and Progress OpenEdge share one engine:
`connectors/legacy/sql_source.py` (`DbApiBatchConnector`). A connector
subclass declares three things and inherits the rest:

1. `entity_sources` — per canonical entity: source table, an optional
   canonical-to-source column map (`None` = identity map, i.e. the site agreed
   to expose canonical column names via staging views), and the watermark
   column. The watermark column rides the SELECT tail so incremental runs can
   observe it; it is never staged.
2. `param_placeholder` — `?` for ODBC/JDBC bridges (pyodbc, JayDeBeApi),
   `%s` for psycopg2/PyMySQL.
3. `_build_default_connection_factory()` — the driver plumbing, imported
   lazily (drivers are optional installs). Tests and special sites inject
   `connection_factory=` instead; see `tests/fixtures/fake_dbapi.py`.

What the shared engine gives every legacy connector: identifier validation
(settings-declared tables/columns are restricted to conservative identifier
syntax and values are always bound parameters — never interpolated),
positional row mapping onto the canonical schemas in
`connectors/legacy/schemas.py`, watermark persistence, provenance stamping,
typed Parquet output, and a `dlt` resource wrapper
(`connectors/legacy/dlt_source.py`) for dlt-based loads.

CDC posture is documented per connector in `connectors/legacy/<erp>/cdc.py`
as a `CdcModule` (engine, Debezium artifact + verified maturity, license
gates, recommended mode). Batch via watermark is the CI-tested default for
every class; the modules carry the binding corrections from the verified
inventory — the Db2 LUW IIDR license gate, the Db2 for i Final-artifact pin,
the Informix driver-v15 posture, the Oracle LogMiner/XStream license split,
the MySQL Connector/J license note, and OpenEdge's no-Debezium-connector
reality (native OpenEdge CDC/Pro2 is the site-licensed alternative). Sybase
ASE has no CDC module at all — batch only.

Sybase-style soft deletes: set `soft_delete_column` and `soft_delete_value`
(the value that marks a row deleted) together; the key inventory excludes
those rows so the anti-join never tombstones live keys. Hard deletes are
reconciled by the anti-join below in every case.

## Delete reconciliation is part of the contract (spec §6)

Every batch connector implements `source_key_inventory(entity)` — a cheap
key-only scan — because the scheduled `delete_reconciliation` Dagster asset
runs the anti-join for every enabled source: warehouse keys the source no
longer returns become tombstones (persisted in the control-plane store for
audit). CDC-native connectors carry deletes in the change stream and declare
`delete_handling = DeleteSemantics.CDC_NATIVE` instead; the reconciliation
asset skips them by design. Fixtures prove the delete scenario per transport
in `tests/test_legacy_sql_connectors.py` and `tests/test_api_connectors.py`.

## API connectors (Business Central, Prophet 21)

Both are implemented against their documented surfaces and UNEXERCISED
against live tenants — validate field maps against `$metadata` at onboarding.
Business Central (`connectors/d365_bc/`) pages API v2.0 with `$top` +
`@odata.nextLink` continuation, sends `Data-Access-Intent: ReadOnly`, honors
`Retry-After` on 429/503 with bounded exponential backoff, and flattens
expanded header/line payloads. Prophet 21 (`connectors/epicor_p21/`) pages
OData v4 with explicit `$top`/`$skip` (P21 emits no continuation links, so
`$top` is always set), reads header-driven lines with an explicit FK
`$filter`, and watermarks on `date_last_modified`. Both feed the same
anti-join reconciliation as the SQL pack.

## Warehouse direct-connect configs (spec §5 last row)

Snowflake, BigQuery, ClickHouse, Trino, and Databricks are connection configs
only — no extraction. `connectors/warehouses/warehouses.yml` declares each
endpoint's driver, required settings, and `${VAR}` interpolation;
`load_warehouse_configs()` parses them into typed configs with a `validate()`
that lists unresolvable settings. The FastAPI connectivity layer attaches to
these; the extraction pipeline never does.
