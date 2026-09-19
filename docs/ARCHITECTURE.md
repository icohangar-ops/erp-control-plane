# Architecture

The ERP Control Plane standardizes data from acquired construction-supplies
dealers — each running a different ERP — into one canonical model with one
KPI catalog, while keeping per-ERP quirks isolated behind connectors.

## The locked decisions and why

| Decision | Choice | Why | What would flip it |
|---|---|---|---|
| Ingestion engine | **dlt + custom Python connectors** (Apache-2.0) | Connectors own per-ERP extraction logic (SuiteQL, ODBC, SOAP, files); dlt handles normalized loading, schema evolution, and incremental state. Airbyte was rejected: connector SDK not designed for bespoke ERP surfaces and license/ops weight; Meltano: thinner ERP ecosystem. | A future where most sources are well-covered by mature Airbyte Protocol implementations AND we want to shed connector maintenance. |
| Orchestration | **Dagster OSS**, software-defined assets | Asset-level lineage (source → Parquet → dbt → marts) is exactly the mental model for onboarding ERPs one entity at a time; asset checks give data-quality gates natively. Airflow: task graph, no asset model; Prefect: weaker dbt integration. | Team standardizes on a different orchestrator already deployed at the client. |
| Transformation | **dbt OSS engine only** | Warehouse-agnostic SQL, tests, docs, and metric definitions; huge hiring pool. dbt Fusion is licensed under ELv2-style terms — excluded. | dbt Core retirement or a client already licensed for a commercial transform tool. |
| One dbt project, multi-tenant by target schema | Staging models are localized **per source** (`stg_<erp>__*`); canonical + marts never see source logic. New tenant = new staging folder + target schema, zero canonical changes. | Keeps the canonical layer identical across tenants — the whole point of a control plane. | A tenant needing a genuinely different business model (e.g. non-distribution vertical). |
| Storage | **Postgres** for control-plane metadata (registry, sync state, crosswalks, COA mappings, DQ results); **DuckDB** as per-customer analytics engine over Parquet | Postgres: transactional metadata with real SQL types; DuckDB: zero-ops single-file analytics engine reading the Parquet lake directly, no warehouse to provision per customer. | Warehouse-grade scale (multi-tenant consolidated analytics) → move marts to Snowflake/BigQuery via dbt target swap; the model is portable. |
| BI | **Apache Superset** behind an optional `bi` Compose profile | Apache-2.0, RLS + embedding support, keeps the core demo light. | Client already runs Looker/Power BI — ship the SQL assets, skip Superset. |
| Provenance | Every fact/dim row carries `source_system`, `source_id` (and `source_doc_no`/`source_line_no` on facts), `loaded_at` | Auditability and reconciliation back to the ERP are non-negotiable for financial data during diligence and post-close. | None foreseeable; this is a floor, not a choice. |

## Data flow

```
dealer ERPs ──connector──▶ Parquet lake (staging, provenance-stamped)
                                │
        control-plane Postgres ◀┤  (registry, watermarks, file hashes,
                                │   quarantine, crosswalks, DQ results)
                                ▼
             dbt (DuckDB target): staging → canonical → marts
                                ▼
                    Superset / notebooks / reverse ETL
```

## Maturity tiers

- **Working end to end:** `csv_sftp` (manifest-gated, checksum, quarantine,
  idempotent) — exercised by tests and the demo.
- **Coded, credential-gated:** `netsuite` (SuiteQL/TBA) — `--dry-run` only;
  never exercised against a live tenant.
- **Documented skeletons:** BisTrack (ODBC + Smart View API), DMSi Agility,
  Epicor P21, Epicor Eclipse, ECI Spruce/RockSolid MAX, D365 BC.
  No invented API behavior — each states its extraction surface and TODOs.
