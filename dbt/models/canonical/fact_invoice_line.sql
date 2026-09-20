{{ config(materialized='table') }}
-- Billed revenue and COGS at line grain. Revenue recognized here (not on the
-- order) so KPIs tie to invoiced amounts; order linkage is retained for
-- service-level reconciliation.
select
    {{ sk("s.source_system", "s.invoice_no", "s.line_no") }} as invoice_line_key,
    {{ sk("s.source_system", "s.invoice_no") }} as invoice_key,
    {{ sk("s.source_system", "s.order_no") }} as order_key,
    s.source_system,
    s.invoice_no as source_doc_no,
    s.line_no as source_line_no,
    s.order_no as source_order_no,
    cc.canonical_customer_key as customer_key,
    ci.canonical_item_key as item_key,
    {{ sk("s.branch_code") }} as location_key,
    s.invoice_date,
    s.uom,
    s.invoiced_qty,
    s.unit_price,
    s.unit_cost,
    s.invoiced_qty * s.unit_price as revenue_amount,
    s.invoiced_qty * s.unit_cost as cogs_amount,
    (s.invoiced_qty * s.unit_price) - (s.invoiced_qty * s.unit_cost) as gross_margin_amount,
    s.freight_amt,
    s.tax_amt,
    s.source_file,
    s.source_row_no,
    s.batch_id,
    s.loaded_at
from {{ demo_staging('invoice_lines') }} s
join {{ ref('crosswalk_source_customer') }} cc
  on cc.source_system = s.source_system and cc.source_key = s.customer_no
join {{ ref('crosswalk_source_item') }} ci
  on ci.source_system = s.source_system and ci.source_key = s.item_no
