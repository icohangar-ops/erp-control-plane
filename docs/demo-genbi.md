# End-to-End GenBI Demo — Governed Natural-Language Loop and Legacy Coverage Matrix

**What this demo validates:** the load-bearing premise of the architecture — that natural-language questions over *curated* marts (WrenAI MDL) can be answered with governed SQL, promoted into Superset as reusable artifacts, and re-run idempotently, with no ungoverned SQL escaping. What would break it: MDL coverage gaps (unsupported questions must return "not modeled yet" and enter the coverage queue) — the golden-set gate in the spec guards exactly this, and it is the main thing production curation must protect.

The dataset behind this run is a **replaceable sample seed** (Informix-shaped, `data/demo_informix/ifx_demo.duckdb`). "GenBI" is an internal code-path label; the product is open-source and industry-generic.

**Source of record for the coverage matrix:** *Legacy Database Coverage — Verified Inventory* (workspace artifact `art_7DIRx9Nu`, published September 19, 2026). The matrix below is reproduced from that artifact, not re-derived. All source citations for every row live there (§6 register).

---

## 1. The pipeline that ran

```
Informix-shaped seed (DuckDB)
  └─ dlt extraction (read-only, watermark)          connectors/
      └─ Dagster assets (registry-driven, 52 keys)  orchestration/
          └─ dbt build → DuckDB                     dbt/
              ├─ staging  (per-source, quality-tested)
              ├─ canonical (Kimball dims/facts, crosswalks)
              └─ marts    (8 KPI headline marts)
                  ├─ Apache Superset (dashboards, READ_ONLY DuckDB)
                  └─ WrenAI (21 MDL models over curated schemas only)
                        └─ governed NL question → SQL → guarded execute
                              → promote → Superset dataset/chart/dashboard
                                  → append-only audit trail
```

## 2. Verified evidence

### 2.1 Cross-source parity (the core claim: any source, same model)

The Informix-shaped path and the v0.1 CSV/SFTP path were both run through the same canonical model. Staging row counts match exactly:

| Entity | stg_informix__* | stg_csvsftp__* |
|---|---|---|
| customers | 13 | 13 |
| items | 32 | 32 |
| gl_entries | 531 | 531 |
| inventory_snapshots | 480 | 480 |
| invoice_lines | 1,808 | 1,808 |
| sales_order_lines | 1,930 | 1,930 |
| purchase_order_lines | 196 | 196 |
| salespeople | 6 | 6 |
| vendors | 8 | 8 |

### 2.2 Canonical and marts populated

`data/analytics/analytics.duckdb` (verified read-only): `dim_customer` 13, `dim_item` 32, `dim_date` 365, `fact_invoice_line` 1,808, `fact_sales_order_line` 1,930, `fact_gl_transaction` 531, `fact_purchase_order_line` 196, `fact_inventory_snapshot` 480, plus crosswalks (`source_*` per entity) and 8 KPI marts (`kpi_headline`, `kpi_finance`, `kpi_growth`, `kpi_inventory`, `kpi_productivity`, `kpi_purchasing`, `kpi_service`, `kpi_window`).

### 2.3 The governed loop (append-only audit, `data/genbi/audit.jsonl`)

The audit trail captures the full contract live, including the guardrail doing its job:

1. **00:02:36 — guardrail rejection.** "What is our GMROI?" produced SQL referencing an unqualified `kpi_headline`; execution failed schema/allowlist validation → `guardrail_rejected`, nothing persisted. Ungoverned SQL does not ship.
2. **00:03:19 — governed execution.** Schema-corrected SQL (`main_marts.kpi_headline`) executed read-only in 19 ms, 1 row.
3. **00:08:38 — promotion.** GMROI answer promoted to Superset: slug `genbi-8801f25497f7-20260920`, dataset 1, **chart 12, dashboard 1**, grid row `ROW-GENBI-8801f25497f7`.
4. **Second question promoted.** "Show gross margin amount by item category" executed (8 rows, canonical join `fact_invoice_line ⋈ dim_item`) and promoted: chart 13, dataset 9, dashboard 1.
5. **00:14:53 and 00:16:53 — idempotent re-runs.** Re-asking both questions returned outcome `updated` — same slugs, no duplicates. The create-or-update contract (spec §4.2) holds under repetition.

Representative answer values: GMROI **1.73**; gross margin by category (8 rows) — Lumber & Panels 509,475.71, Roofing 393,817.39, Framing & Fasteners 256,787.58, … Tools & Accessories 72,132.09.

### 2.4 Visual evidence (committed under `docs/assets/genbi/`)

- `genbi-dashboard-top.png` — dashboard 1 top: KPI headline tiles over the shared READ_ONLY DuckDB connection.
- `genbi-dashboard-asksave.png` — the "Ask → Save" tabpanel containing the two promoted charts, with their governed provenance note ("promoted natural-language answers, saved from GenBI with their governed SQL").
- `promoted-chart-explore.png` — chart 12 in Superset's explore view, showing the governed SQL it backs.

**Known capture limitation:** a screenshot of the WrenAI question-and-answer thread itself was not captured — the wren-ui landing view renders the modeling screen in headless capture and the thread view could not be reached deterministically. The NL→SQL→execution→promotion chain is nonetheless fully evidenced by the append-only audit trail (§2.3), the live UI verification of the same questions earlier in the build, and the promoted artifacts visible in the screenshots.

## 3. Legacy + modern source coverage matrix

Reproduced from the verified inventory (art_7DIRx9Nu, September 19, 2026): **7 CONFIRMED · 5 CORRECTED · 1 embedded GAP** across the 12 researched source classes; no verdict row was wrong at the "connector exists/doesn't exist" level.

| # | Source class | Verdict | Headline |
|---|---|---|---|
| 1 | Informix | **CORRECTED** | Debezium connector (incubating, Change Streams API) confirmed; current Debezium ships JDBC driver v15 only — de-facto supports Informix 12 as well. |
| 2 | Db2 LUW | **CORRECTED** | Connector confirmed (stable); CDC via SQL Replication **requires a separate IBM IIDR license** — a licensing gate the connector pack carries. |
| 3 | Db2 for i (AS/400) | **CORRECTED** | Incubating journal-based connector confirmed; Final releases 3.0.0–3.1.1 exist (newest 3.2.0.Alpha1) but **no reference-docs page**. Batch-default posture correct. |
| 4 | Oracle | **CONFIRMED** | LogMiner default; OpenLogReplicator and XStream (GoldenGate licensing) adapters confirmed. |
| 5 | SQL Server | **CONFIRMED** | Native CDC change tables, SQL Server 2016 SP1+ Standard/Enterprise. |
| 6 | PostgreSQL | **CONFIRMED** | Logical decoding, `pgoutput` default plug-in (UTF-8-only, no DDL events). |
| 7 | MySQL / MariaDB | **CONFIRMED** | Separate non-incubating binlog connectors. |
| 8 | Sybase ASE | **CORRECTED** | No Debezium connector. dlt path corrected: ODBC + external `sqlalchemy-sybase` dialect (SQLAlchemy 2.0 removed its internal one); plan the custom pyodbc resource pattern. |
| 9 | Progress OpenEdge | **CORRECTED — material, includes a GAP** | No Debezium connector; **native vendor CDC + Pro2 verified** (spec's UNVERIFIED flag retired); **no SQLAlchemy dialect exists** → `dlt sql_database` cannot connect; custom Python (pyodbc + DataDirect ODBC) dlt resource required. |
| 10 | Business Central (API) | **CONFIRMED** | API v2.0/OData, 20,000 page size, watermarks, anti-join deletes; limits cited to Microsoft Learn — max connections 100 / queue 95, 6,000 req per 5 min per user. |
| 11 | Epicor Prophet 21 (API) | **CONFIRMED** | Read-only OData `/odataservice/odata/table|view`, Bearer auth, explicit `$top` paging; probe `$metadata` at integration (v4 per vendor-adjacent sources). |
| 12 | Modern warehouses | **CONFIRMED** | No extraction — direct connect from Superset/WrenAI. |

Shipped alongside these 12 classes (spec §5, PR #4; fixture-tested, live-tenant-unverified by design):

| Source class | Path | Posture |
|---|---|---|
| SAP HANA | dlt `sql_database` via `sqlalchemy-hana`/`hdbcli` (named in dlt's supported list) | Batch, credential-gated, disabled until configured |
| Cloud ERP REST (Plex, Dynamics 365) | dlt `rest_api` pattern: API-key/OAuth2, explicit pagination, modified-timestamp watermarks, full-key anti-join delete scans | Batch, per-tenant config |

**Cross-cutting:** Debezium pinned at 3.6.x (3.6.2.Final); vendor client libraries are never bundled (the Informix Change Streams client ships with the Informix JDBC installation). dlt library is Apache-2.0; the control plane uses the library only.

**Corrections the connector pack carries** (condensed from inventory §2, C1–C7): the Db2 IIDR license gate (C1); OpenEdge CDC/Pro2 verified and the custom-resource path mandatory (C2, C3); Informix driver-version drift — 12.x is de-facto supported on driver v15, validate at site (C4); Db2 for i flip condition = documented stable release in Debezium's reference docs (C5); Business Central limits cited to Microsoft with the 100/95 refinement (C6); P21 `$metadata` probe step (C7).

## 4. How to run

```bash
# 1. Stack (base BI + GenBI profiles)
docker compose --profile bi --profile genbi up -d
# Superset :8088 · WrenAI UI :3001 (wren-engine, ibis, ai-service, qdrant, postgres)

# 2. Seed + extract + build (Informix-shaped path)
python demo/run_informix_demo.py      # seed → dlt → Dagster → dbt → marts

# 3. Ask + promote (the §4.2 loop; same PromotionService as the FastAPI route)
python demo/promote_answers.py        # → audit records + Superset create-or-update

# 4. Inspect
open data/genbi/audit.jsonl           # append-only governed-loop trail
open http://localhost:8088            # dashboard 1 → "Ask → Save" section
open http://localhost:3001            # WrenAI, 21 curated MDL models
```

Environment: `OPENAI_API_KEY` must be present for any compose invocation that re-evaluates GenBI services; the offline Ollama profile is the alternative. All DuckDB readers use the READ_ONLY URI — only the Dagster/dbt load process opens the file read-write.

## 5. Honest scope notes

- SAP HANA and Cloud ERP REST connectors are fixture-tested only — no live tenant was exercised (credential-gated by design; see the connector-pack verification record).
- The wren-ui Q&A-thread screenshot is the one missing visual (see §2.4 limitation).
- MDL coverage is curated to the canonical + marts schemas (21 models); raw and staging schemas are deliberately excluded from the semantic layer.
- 136 tests pass on this branch; ruff clean.
