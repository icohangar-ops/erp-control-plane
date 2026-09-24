# Data model

Three layers: **staging** (one folder per source, source-localized), **canonical**
(Kimball-style, source-agnostic), **marts** (KPI-ready). dbt target schema
selects the tenant: `main_staging`, `main_canonical`, `main_marts` for the demo.

## Grains and keys

| Model | Grain | Key |
|---|---|---|
| `dim_item` / `dim_item_category` | one sellable item / category | surrogate; natural key `source_system` + `item_id` via crosswalk |
| `dim_customer` | billing customer | surrogate |
| `dim_ship_to` | ship-to address (derived from customer address in v0.1 — the CSV source has no separate ship-to feed) | surrogate |
| `dim_vendor` | vendor | surrogate |
| `dim_location` | branch | surrogate |
| `dim_salesperson` | salesperson | surrogate |
| `dim_date` | calendar day | `date_key` (YYYYMMDD) |
| `fact_sales_order_line` | order line | `order_id` + `line_no` (+ provenance) |
| `fact_purchase_order_line` | PO line | `po_id` + `line_no` (+ provenance) |
| `fact_invoice_line` | invoice line | `invoice_id` + `line_no` (+ provenance) |
| `fact_inventory_snapshot` | item-location-day snapshot | item + location + date |
| `fact_gl_transaction` | GL entry | `journal_id` + `line_no` |
| `ref_price_list` | item + customer + effective window | effective-dated |
| `ref_coa_mapping` | source account + effective window | effective-dated COA mapping |

## Provenance contract

Every staging row and every canonical row carries:
- `source_system` — e.g. `csvsftp`, `netsuite`
- `source_id` — the dealer/source instance
- facts additionally: `source_doc_no`, `source_line_no`
- `loaded_at` — UTC ingestion timestamp
- staging only: `source_file`, `batch_id`

## Crosswalks (golden master)

`canonical/crosswalk_source_*` maps each source's natural key to a canonical
surrogate key (`source_system`, `source_id`, `source_natural_key`) →
`<entity>_key`. Surviving merges keep the first-seen canonical key; later
sources link in. This is the join point that lets two dealers on different
ERPs roll up into one item/customer without renormalizing either.

## Effective-dated reference data

`ref_coa_mapping` maps a dealer's source GL account to the group chart of
accounts **per effective window** (`effective_start`, `effective_end`,
`is_current`). Re-pointing an account after an ERP migration opens a new
window instead of editing history — GL re-analysis over any past period
resolves the mapping that was in force then. `ref_price_list` follows the
same pattern for pricing.

## KPI catalog (20 metrics)

Defined in `dbt/models/marts/` (YAML + backing views; see
`dbt/models/marts/marts.yml`): GMROI, inventory turns, DIO, weeks of supply,
gross margin, line fill rate, order fill rate, OTD, OTIF, same-branch revenue,
organic vs acquired growth, backorder rate, vendor fill rate, PPV, DSO, DPO,
cash conversion cycle, sales per FTE, average ticket, close cycle time.

Formula notes for the non-obvious ones:
- **GMROI** = annualized gross-margin dollars ÷ average inventory value.
- **Organic vs acquired** = revenue split by `acquired_at <= window_start - 365d`
  per customer's owning-dealer acquisition date.
- **Close cycle time** = days from period end to books-closed, from the seeded
  close calendar.
- All service-level metrics are line-weighted from facts (ordered vs filled
  quantities), never order-count approximations.
