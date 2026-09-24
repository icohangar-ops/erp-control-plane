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

## IBM Informix (standalone ODBC implemented; per-flavor coverage documented)

`connectors/legacy/informix/` is the primary legacy-database connector: one
class (`InformixConnector`), one coded extraction path — ODBC batch via
watermark on the shared `DbApiBatchConnector` engine (Informix Client SDK
driver through pyodbc), fixture-tested offline via injected DB-API factories
(`demo/run_informix_demo.py` stands in the transport end to end). **Never
exercised against a live tenant.** Informix ships in three deployment
flavors; all-flavors coverage is a documented matrix over that one class —
a flavor is served by settings + onboarding discovery, never by a new
connector class:

| Extraction surface | F1 standalone 14.10/15.0.x | F2 CP4D/Software Hub cartridge | F3 standalone container (K8s/OCP) |
|---|---|---|---|
| ODBC (Client SDK via pyodbc) — **the coded path** | Documented — Client SDK in every purchasable edition | Not established — the cartridge documents MQTT/REST/MongoDB APIs only → [D-1] | Image-specific → [D-7] |
| JDBC (Informix driver) | Documented | Expected (platform connections + credential vaults exist) but unconfirmed → [D-1] | Plausible, unconfirmed per image → [D-7] |
| JDBC via IBM Data Server Driver (DRDA) | Documented | Not documented → [D-1] | Not established → [D-7] |
| DRDA protocol (server listener) | Documented — needs a `sqlhosts` alias (`drsoctcp`/`drsslctcp`) + IBM Data Server Client | Not documented → [D-1] | Not established → [D-7] |
| JSON wire listener (MongoDB API) | Documented — `jsonListener.jar` ships with the server | Documented on the service page | Not established per image → [D-7] |
| REST API (wire listener, driverless) | Documented — SQL passthrough needs `security.sql.passthrough=true` | Documented on the service page | Not established per image → [D-7] |
| Admin/monitoring REST (InformixHQ) | Product surface documented; extraction use unassessed → [D-9] | Not assessed → [D-2] | Bundling unconfirmed → [D-7] |
| MQTT / OData | Documented | MQTT documented on the service page | Not established → [D-7] |

Reading the matrix: the only served cell is **ODBC on F1**. The single
consequential gap is F2 relational access [D-1] — never promise an ODBC DSN
to a CP4D/Software Hub tenant. If [D-1] confirms JDBC-only connectivity, the
planned variant mirrors `db2_iseries_template`'s `jdbc_url` + JayDeBeApi
bridge (a settings/registry extension behind the same batch machinery, not a
new class); building it speculatively is out of scope. F3 is expected to
behave like F1 against the container's exposed listener, but every surface
claim is image-specific until checked against the pinned image's
containerized-deployments docs — fail closed until then [D-7].

Per-flavor CDC posture (the engine substrate is the server-side Change Data
Capture API over the logical log; Debezium consumes it through the
**client-side** Change Streams API for Java, which ships with the Informix
JDBC installation — Maven Central `com.ibm.informix:ifx-changestream-client`
— so it is a Kafka Connect deployment concern, never an engine feature):

- **F1 standalone** — every server-side prerequisite is administrable by the
  site DBA (full-row logging via `cdc_set_fullrowlogging`, `syscdcv1.sql` run
  as user `informix` from `$INFORMIXDIR/etc`, capture mode): CDC attach is a
  configuration exercise, not a feasibility question. Legacy 12.10 estates:
  pilot before promising [D-8].
- **F2 cartridge** — no public documentation for tenant-runnable
  `syscdcv1.sql`, full-row logging, capture mode, or logical-log retention:
  assume no CDC until proven on-site [D-2]; batch-only default.
- **F3 container** — operator-side CDC steps and Kafka Connect placement are
  image- and site-specific [D-7].

CDC stays documented-not-required (Debezium Informix is **"incubating"** —
Debezium's own label, externally corroborated; `debezium_maturity` keeps it).

### Onboarding discovery checklist (fail closed — confirm before design freeze)

- **[D-1] CP4D/Software Hub connectivity matrix** (blocks F2 design): external
  ODBC vs JDBC/REST/Mongo/MQTT only; cartridge engine version on the tenant's
  Software Hub release; is the MQTT/REST/Mongo wording exhaustive. Fail
  closed: plan JDBC-only for F2 until ODBC is proven.
- **[D-2] CDC feasibility on managed CP4D**: tenant-runnable `syscdcv1.sql`,
  full-row logging, capture mode, logical-log retention; any Debezium/IDR
  attach precedent. Fail closed: assume no CDC on F2.
- **[D-3] 14.10 standard-EOS date**: pull the lifecycle dates table /
  announcement letter before any contractual claim.
- **[D-4] 11.50/11.70 exact EOS dates**: verify on the IBM lifecycle UI.
- **[D-5] Reconcile "CP4D Informix 14.10 EOL 2023"** against the specific
  CP4D release lifecycle (cartridge EOM/EOS fields are blank); cite the
  exact CP4D release EOS if used in a proposal.
- **[D-6] 15.0.x Workgroup standalone lifecycle entry** (PID/date): confirm
  the purchasable edition set at proposal time.
- **[D-7] Standalone-container surface specifics** for the pinned image
  version: exposed ports/listeners (ODBC/JDBC/DRDA/wire listener/REST), wire
  listener and InformixHQ bundling, offline image delivery, operator CDC
  steps. Source of truth: the pinned image's containerized-deployments docs
  plus a hands-on pod test.
- **[D-8] Debezium on a legacy 12.10 estate**: the support matrix includes
  DB 12 with driver 15.0.1.1 ("in practice") — confirm the site's exact
  12.10.xC level and Linux platform, and pilot before promising CDC.
- **[D-9] InformixHQ / admin-REST scope**: enumerate what the Swagger/REST
  surface exposes before assuming any extraction use beyond monitoring.

The registry descriptions (`informix_template`, `informix_demo` in
`connectors/sources.yml`) carry the same coverage one-liner. The settings
contract is unchanged — `required_settings` stays the standalone ODBC trio
(`odbc_dsn`, `db_user`, `db_password`); F2 settings would be added only when
the [D-1] trigger fires, and the settings-parity fixture test pins that.

## BisTrack (ODBC implemented, Smart View documented plan)

`connectors/bistrack/connector.py` rides the same `DbApiBatchConnector` engine
as the legacy pack, with a mode switch (spec §3.1's read surfaces):

- `mode: odbc` (default) — read-only pyodbc against the on-prem SQL Server.
  Two entities are mapped: `sales_order_lines` (`OrderLine` JOIN `OrderHeader`)
  and `invoice_lines` (`InvoiceHeader` JOIN the expected `InvoiceLine` table —
  confirm the name at onboarding). These are the only public table names, so
  every other entity stays a documented plan whose extraction raises
  `ConnectorNotImplemented` until the §3.3 discovery pack pins the physical
  tables — the column maps in the spec's `DocumentEntitySpec` are the artifact
  to edit when it does. The connection factory is injectable; fixture tests
  drive the real machinery (`tests/test_bistrack.py`), and the lazy pyodbc
  default factory keeps the driver an optional install.
- `mode: smartview` — the BisTrack Web Smart View API. DOCUMENTED PLAN ONLY:
  the BisTrack API is a separately licensed Epicor product with no public token
  scheme or endpoint catalog (spec §2.1), so the adapter reports planned
  surfaces and raises `ConnectorNotImplemented`.

BisTrack-specific machinery the shared engine does not model:

- **Per-document-type watermarks.** `OrderHeader` numbers orders, quotes,
  call-off orders, reservations, and template orders in separate sequences
  (spec §7.1) — a quote is a sibling transaction, not a flagged order. Extraction
  scans one configured document type at a time (`order_document_types` is
  REQUIRED for orders; invoices may scan unscoped) and the checkpoint is a JSON
  map `{doc_type: max_doc_no}`, compared number-aware. The document type also
  scopes the natural key (`ORDER:1234:5`), and the key-inventory scan carries
  the same type filter so the anti-join diff is scope-faithful.
- **Keyset paging.** Pages advance with `(doc > ? OR (doc = ? AND line > ?))`
  plus `ORDER BY doc, line OFFSET 0 ROWS FETCH NEXT n ROWS ONLY` — the
  predicate carries the cursor, never a sliding OFFSET window (spec §6.4).
- **UOM and branch carriage.** The source UOM rides every quantity/price raw
  (spec §7.3 — never normalize at extraction); `branch_code` rides every row
  and inventory keys stay branch-scoped (spec §7.4).

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

## DMSi Agility (AgilityPublic REST)

`connectors/dmsi_agility/` implements the documented AgilityPublic surface:
per-dealer HTTPS API URL tied to that dealer's database, a dealer-created
integration user (no API keys or OAuth exist), and `Session/Login` issuing the
`SessionContextId` whose `ContextId` + `Branch` headers ride every data call.
Contexts expire unused (4 h default, 24 h max, dealer-configurable) — the
adapter assumes no TTL: one context rejection re-logins and restarts that
entity's chunk walk from pointer 0 (chunk state is server-side per method run,
spec §4), a second rejection fails the run, and the walked entity dedupes by
natural id so the restart never double-stages rows. Paging walks
`ChunkStartPointer` ← `NextChunkStartPointer` until `MoreResultsAvailable` is
false with `RecordFetchLimit` starting at the Appian-tuned 500 (per-dealer
chunk ceilings live in the dealer's System Config and are invisible until
discovery). Watermarks ride `LastChanged` via `FetchOnlyChangedSince` on
exactly the eight list methods that document it; backfill sends the
epoch-equivalent stamp. Orders and invoices are customer-scoped (`CustomerID`
required; `<all>` is valid only with `SearchBy` and is never a bulk path) over
the required `customers` setting; invoices slide `InvoiceDateRangeStart/End`
windows (no changed-since filter exists) and inventory is a quantity-inclusive
item walk stamped as a dated snapshot per logged-in branch. Headers and detail
rowsets join in-payload (dtOrder/dtOrderDetail on OrderID,
dtInvoiceDetailResponse on InvoiceNumber) — a detail row whose header is
absent from the same payload quarantines fail-closed rather than dropping the
document context. GL transactions and PO lists have **no** AgilityPublic
service (spec §3 rows 9/11, absences verified against the full method
inventory): both stay documented plans — GL rides the hybrid vendor-mediated
channel (Agility's embedded Data Warehouse, report exports, or BInformed FTP)
and POs extract one `PurchaseOrderGet` per ID from a source outside the API.
Item pricing never uses `IncludePriceData` (computed against the API user's
default customer — meaningless for an integration user); the API is called
sequentially against the dealer's production OpenEdge database. Malformed
pages quarantine with a machine-readable reason and fail the run. UNEXERCISED
against a live dealer — validate rowset keys, field names, chunk ceilings, and
allocation scoping against the tenant's `AgilityVersion` at onboarding;
fixture tests (`tests/test_dmsi_agility.py`) prove session handling, paging,
watermarks, joins, quarantine, and reconciliation against MockTransport.

## Epicor Eclipse (REST session-token API over Caché)

`connectors/epicor_eclipse/` implements the documented Eclipse REST surface
(spec art_Hp74a48b): the API engine is the only extraction channel — Eclipse
runs on InterSystems Caché and no SQL/ODBC path is offered to external
services on-prem or in Eclipse Cloud. Auth is the proprietary expiring-session
model, not OAuth2 and not basic auth: `POST /Sessions` mints a
`sessionToken` (+ `refreshToken`) and every data call carries the token in the
tenant-pinned token header; `POST /SessionRefresh` recovers an
expired-but-not-deleted session (the refresh request identifies itself with
the same token header) and a full `POST /Sessions` re-login is the fallback
when the session is already deleted. No token TTL is public, so none is
assumed: one session rejection refreshes (or re-logins) and retries that page
exactly once, a second rejection fails the run, and the walked entity dedupes
by natural id so the retry never double-stages rows. The session request-body
field names, token header, watermark stamp field, and paging parameter
names/first-index/values are **per-tenant contract ([D])** — they exist only
in each tenant's deployed API docs (`http://EclipseServer:Port`, default port
5000) and are REQUIRED settings that fail closed on empty; a wrong
page-number first index silently skips a page, so it is validated
(0-based or 1-based) rather than defaulted. List reads walk query-param pages
until a short page with a runaway-page guard; masters ride `updatedAfter`
watermarks (the epoch-equivalent on backfill; rows staged with zero stamps
fail closed — no checkpoint is persisted for a payload that proves the pin
wrong); inventory is a full `/ProductInventoryList` sweep stamped as a dated
snapshot (no watermark is possible for quantities); sales-order and
purchase-order lines flatten the detail object's `LineItems` collection (a
detail without lines quarantines fail-closed rather than dropping document
context); customers drop only rows whose `deleted` flag is explicitly set
(P21 polarity discipline — a missing flag is not a dead account); GL entries
extract from `GLInquiryDetail` by posting-period window; and vendors extract
from `/Vendors` with the same paging contract. License-gated families
(`/SalesOrders` Sales Order API, `/PurchaseOrders` Purchase Order API,
`/GL*`/`/Journals` Accounting API, `ARInquiry` Accounting/AR API — check
Premium bundle entitlements in diligence) and the **Search Index Builder**
dependency (search-based GETs fail with "first index the records" until the
tenant indexes each entity — a deployment prerequisite the connector surfaces
as its own failure, not an empty result) are documented in the extraction
plans. No `/Invoices` endpoint exists (spec §3, verified absence): invoice
lines stay a documented plan — `ARInquiry` is inquiry-shaped and
license-gated, so history rides the hybrid report/file channel negotiated
with the dealer or Epicor CAM. Malformed pages and keyless rows quarantine
with a machine-readable reason and fail the run. UNEXERCISED against a live
tenant — capture the tenant's deployed API docs, pin every [D] setting,
verify Search Index state, and run the stepped-load test at onboarding;
fixture tests (`tests/test_epicor_eclipse.py`) prove session-token auth and
refresh recovery, paging, watermarks, line flattening, quarantine, backoff,
and anti-join reconciliation against MockTransport.

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

## Connector-wave flags

Decisions collected during the first connector wave and recorded here so the
matrix (`evidence/matrix.yaml`) can bind them to a checkable location. Each
flag states its decision and the condition that reverses it.

### Conversion factors carried at the connector layer

Canonical conversion-factor column: deferred. Unit-of-measure conversions are
carried at the connector layer only (flagged in the BisTrack, DMSi Agility, and
ECI Spruce adapters); the canonical fact models keep each source's native UOM
columns and apply no canonical conversion factor. Reversal condition: a
cross-site analytics requirement that compares quantities across ERPs in one
canonical unit — at that point a canonical conversion-factor column lands in
the canonical model with per-source factors validated at onboarding.

### BisTrack invoice scans assume site-global numbering

Unscoped invoice scans in the BisTrack adapter assume site-global invoice numbering
(one sequence per site). The safe path is per-type invoice_document_types
extraction, which scopes scans per document type. Reversal
condition: a tenant whose site reuses invoice numbers across document types —
onboard those per-type only.

### DMSi Agility header/detail nesting confirmed at onboarding

The DMSi Agility adapter treats header and detail results as parallel rowsets
(keyed by the header key, not nested objects). This shape is confirmed at onboarding:
validate the parallel-rowset assumption against a live dealer's AgilityPublic
metadata before enabling extraction. Reversal condition: a tenant whose
AgilityPublic tenant returns nested detail payloads — the rowset flattening
moves into that tenant's profile.

### Spruce SOAP surface is NDA-gated

The Spruce/RockSolid MAX SOAP Ecommerce API surface is NDA-gated and therefore
is not implemented from documentation; the shipped path is the manifest-gated
CSV/pipe file drop. Reversal condition: NDA access granted — the SOAP adapter
is then specified from the gated documentation and exercised against a sandbox
tenant before any dealer cutover.
