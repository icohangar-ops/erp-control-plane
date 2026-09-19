# Analytics / Superset assets

Apache Superset runs behind the optional Compose profile (`docker compose --profile bi up -d`)
and is intentionally out of the light 10-minute demo path.

## Contents
- `headline_kpis.sql` — single-row executive tile dataset (all 20 KPIs).
- `service_levels_by_branch.sql` — fill-rate chart by branch.
- `inventory_efficiency_by_category.sql` — GMROI / turns by category.

## Connecting
1. Create a DuckDB database connection in Superset
   (`duckdb:////work/data/analytics/analytics.duckdb`; mount the repo at `/work`
   as the Compose file does for the `analytics` volume).
2. Create datasets from the three SQL files above (SQL Lab → Save as dataset).
3. Build the two dashboards (Executive Overview, Operations) from tiles on
   those datasets.

## Row-level security and embedding (customer deployments)
- Superset RLS filters are defined per tenant/customer on the canonical fact
  tables — at minimum filter `fact_invoice_line` / `fact_sales_order_line` /
  `fact_gl_transaction` on `source_system` (per-acquired-dealer isolation) and
  `dim_location` (per-branch visibility).
- Service accounts per viewer group; never share one admin token.
- For embedded dashboards use Superset's embedded feature with a short-TTL
  guest token minted by the control plane; the guest token's RLS clause must
  include the same `source_system` filter so an embedded dealer dashboard can
  only ever see that dealer's rows.
- DuckDB is single-writer: for a Superset deployment prefer the Postgres
  mirror of the canonical layer, or schedule refreshes outside BI hours.
