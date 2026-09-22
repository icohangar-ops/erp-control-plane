# Connector guide — write one in an afternoon

The `BaseConnector` contract (see `connectors/base.py`) is small on purpose.
A connector declares config, yields canonical staging records, and lets the
SDK handle provenance, watermarks, Parquet writing, and idempotency.

## The contract

```python
class BaseConnector(ABC):
    # The four methods a connector implements:
    def entities(self) -> list[str]                                     # extractable entity names
    def validate_config(self) -> list[str]                              # config problems (empty = deployable)
    def describe_extraction(self, entity) -> ExtractionPlan             # offline dry-run plan
    def _iter_records(self, entity, mode, watermark) -> Iterator[dict]  # yields canonical staging rows

    # Inherited from the base — never re-implemented per connector:
    def register(self) -> SourceRegistration             # runs validate_config, upserts the source
    def extract(self, entity, mode) -> ExtractedEntity   # watermark → _iter_records → Parquet → checkpoint
    def reconcile_deletes(self, entity) -> ReconciliationResult  # anti-join tombstoning
```

Rules the contract enforces:
1. **`_iter_records` is a generator** of dict rows; the base's `extract()`
   batches them to Parquet (25k rows per write).
2. **Every row carries provenance** — the base stamps `source_system`,
   `source_id`, `loaded_at`; you supply `source_file`, `source_row_no`, and
   `batch_id` when the source provides them (API sources fold document
   identity into `source_id`), and declare each entity's `natural_key_fields`.
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
- Declare required credentials in `validate_config()` — the base's
  `register()` runs it and fails loudly (`ConnectorNotConfigured`).
- Declare the incremental column (e.g. `last_modified_date`) in
  `describe_extraction()`.
- Write an explicit `arrow_schema()` — never let Arrow infer from batch 1.
- Set the connector's `maturity` ClassVar (`IMPLEMENTED` / `SKELETON`; the
  contract suite enforces honest labeling) and say so in the README.

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

## API connectors (Business Central, Prophet 21, NetSuite)

All three are implemented against their documented surfaces and UNEXERCISED
against live tenants — validate field maps against `$metadata` at onboarding.
Business Central (`connectors/d365_bc/`) loops the configured companies list
(the `companies` setting — comma-separated ids; the only non-company-scoped
call is `GET /companies`), pages API v2.0 with `$top` + `@odata.nextLink`
continuation, sends `Data-Access-Intent: ReadOnly`, honors `Retry-After` on
429/503 with bounded exponential backoff, halves its page on 504, and flattens
expanded header/line payloads. The checkpoint is the max `lastModifiedDateTime`
observed across ALL configured companies (never a per-company max); natural
ids are company-prefixed so two companies' shared document numbers never
collide. `salesInvoices` is the invoice document aggregate, not a
posted-invoice archive (caveat on the `invoice_lines` plan); price lists,
item attributes, and ship-to/order-address masters have no v2.0 entity
(custom AL API pages per tenant); historical backfill is a restore-side
BACPAC export, never an in-connector path; and malformed API pages quarantine
with a machine-readable reason and fail the run. Prophet 21
(`connectors/epicor_p21/`) authenticates by minting a middleware token
(`POST /api/security/token/v2`, credentials in the JSON body; some middleware
answers XML even when JSON is requested, so the response is parsed
defensively — tokens live ~24 h, are cached until near expiry, and one 401
triggers a single re-mint-and-retry), pages
OData v4 with explicit `$top`/`$skip` (P21 emits no continuation links, so
`$top` is always set) ORDERed by each entity's stable key so OFFSET windows
are deterministic, filters soft-delete flags server-side
(`delete_flag eq 'N'` on the tables the spec documents it for) and
client-side in both extraction and the key-inventory scan so a
soft-delete-heavy site cannot mass-tombstone through the anti-join (the
spec's §3.1 "Y = active" note contradicts its §6.4/§7 "Y = deleted"
semantics — the adapter implements §6.4/§7; verify polarity per site at
onboarding), reads header-driven lines with an explicit FK
`$filter`, and watermarks on `date_last_modified`. NetSuite
(`connectors/netsuite/`) posts SuiteQL to the SuiteTalk REST endpoint with
OAuth1 token auth, pages LIMIT/OFFSET at 200 rows (each query ORDERed by the
entity's unique key so windows are deterministic), and honors `Retry-After`
on 429/503/504. Watermarks ride `lastmodifieddate` (UTC audit stamp, never the
`trandate` business date); the `subsidiaries` setting scopes OneWorld queries
server-side and natural ids carry the row's subsidiary id (non-OneWorld
accounts leave the setting empty), while `accounting_book_id` scopes the GL
entries query on multi-book tenants. SuiteQL via REST returns a maximum of
100,000 results per query, and the account concurrency limit (5/15/20 base
concurrent requests by service tier, shared across SOAP/REST/RESTlet calls)
governs the tenant — the adapter runs strictly sequentially. Malformed pages
quarantine with a machine-readable reason and fail the run. All three feed the
same anti-join reconciliation as the SQL pack.

## SAP HANA (SAP Business One on HANA)

`connectors/legacy/sap_hana/` joins the shared `DbApiBatchConnector` batch
path (`connectors/legacy/sql_source.py`) as the 15-class matrix's HANA row.
The connection is a raw SQLAlchemy `hana://` engine connection — the
`sqlalchemy-hana` dialect with the `hdbcli` client — built lazily so the
dependency is a site-provided optional extra, exactly like pyodbc for
OpenEdge; tests inject `connection_factory=` fixtures instead. All nine
canonical entities read DBA-maintained `b1_*` staging views exposed to a
dedicated READ-ONLY extraction user (never the B1ADMIN/owner account). The
optional `db_schema` setting is identifier-validated and qualifies every SQL
path — SELECT, key scan, dry-run plan (`B1SCHEMA.b1_items`). Instance ports
follow the SAP convention 3NN15: instance 90 listens on 39015 (the template
default). Batch-only per spec §6: no CDC path is verified, so deletes
reconcile via the scheduled full-key anti-join. UNEXERCISED against live
HANA — validate staging-view columns and watermarks at onboarding.

## Cloud ERP REST (Plex, Dynamics 365)

`connectors/cloud_erp_rest/` is one connector class behind per-tenant
provider profiles (`provider: plex|d365`), following the dlt rest_api
verified-source approach. Auth resolves per tenant: an API-key header (Plex
default `X-API-Key`) or OAuth2 client credentials (Dynamics 365 via Microsoft
Entra ID — the token URL derives from the AAD `tenant_id`). Pagination is
explicit per profile: `page`/`pageSize` page-number paging for Plex-style
surfaces (a short page ends the scan) and `@odata.nextLink` continuation for
OData. Incremental reads filter server-side on a modified-timestamp field
(settings-overridable) and 429/503 responses back off honoring `Retry-After`
with bounded retries. These APIs expose no delete feeds: the scheduled
full-key anti-join reconciles deletes, using `$select`-style key-only scans
where the profile declares them. Resource paths and field maps are
canonical-shaped defaults — validate them against the tenant's API metadata
at onboarding. UNEXERCISED against live tenants; fixture tests
(`tests/test_cloud_erp_rest.py`) prove paging, auth, watermarks, and
reconciliation against MockTransport recordings.

## Warehouse direct-connect configs (spec §5 last row)

Snowflake, BigQuery, ClickHouse, Trino, and Databricks are connection configs
only — no extraction. `connectors/warehouses/warehouses.yml` declares each
endpoint's driver, required settings, and `${VAR}` interpolation;
`load_warehouse_configs()` parses them into typed configs with a `validate()`
that lists unresolvable settings. The FastAPI connectivity layer attaches to
these; the extraction pipeline never does.
