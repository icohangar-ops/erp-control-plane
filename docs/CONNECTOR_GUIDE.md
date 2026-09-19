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
