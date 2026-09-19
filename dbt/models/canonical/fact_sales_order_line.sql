{{ config(materialized='table') }}
-- One row per source order line. Facts carry document identity
-- (source_doc_no/source_line_no), canonical dim keys through the crosswalks,
-- measures, and derived service-level flags used by the KPI marts.
select
    {{ sk("s.source_system", "s.order_no", "s.line_no") }} as order_line_key,
    {{ sk("s.source_system", "s.order_no") }} as order_key,
    s.source_system,
    s.order_no as source_doc_no,
    s.line_no as source_line_no,
    cc.canonical_customer_key as customer_key,
    ci.canonical_item_key as item_key,
    {{ sk("s.branch_code") }} as location_key,
    {{ sk("s.source_system", "s.salesperson_code") }} as salesperson_key,
    s.order_date,
    s.promised_date,
    s.shipped_date,
    s.order_status,
    s.uom,
    s.ordered_qty,
    s.filled_qty,
    s.cancelled_qty,
    s.unit_price,
    s.unit_cost,
    s.ordered_qty * s.unit_price as ordered_amount,
    least(s.filled_qty, s.ordered_qty) * s.unit_price as filled_amount,
    s.shipped_date is not null and s.shipped_date <= s.promised_date as is_on_time,
    s.filled_qty >= s.ordered_qty as is_line_complete,
    s.ordered_qty > 0
        and s.cancelled_qty < s.ordered_qty
        and s.filled_qty < s.ordered_qty as is_backordered,
    s.source_file,
    s.source_row_no,
    s.batch_id,
    s.loaded_at
from {{ ref('stg_csvsftp__sales_order_lines') }} s
join {{ ref('crosswalk_source_customer') }} cc
  on cc.source_system = s.source_system and cc.source_key = s.customer_no
join {{ ref('crosswalk_source_item') }} ci
  on ci.source_system = s.source_system and ci.source_key = s.item_no
