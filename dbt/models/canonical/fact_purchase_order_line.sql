{{ config(materialized='table') }}
select
    {{ sk("s.source_system", "s.po_no", "s.line_no") }} as po_line_key,
    {{ sk("s.source_system", "s.po_no") }} as po_key,
    s.source_system,
    s.po_no as source_doc_no,
    s.line_no as source_line_no,
    cv.canonical_vendor_key as vendor_key,
    ci.canonical_item_key as item_key,
    {{ sk("s.branch_code") }} as location_key,
    s.po_date,
    s.received_date,
    s.uom,
    s.ordered_qty,
    s.received_qty,
    s.unit_cost_actual,
    s.unit_cost_standard,
    (s.unit_cost_actual - s.unit_cost_standard) * s.received_qty as purchase_price_variance,
    s.received_qty >= s.ordered_qty as is_received_in_full,
    s.received_qty > 0 as is_received,
    s.source_file,
    s.source_row_no,
    s.batch_id,
    s.loaded_at
from {{ ref('stg_csvsftp__purchase_order_lines') }} s
join {{ ref('crosswalk_source_vendor') }} cv
  on cv.source_system = s.source_system and cv.source_key = s.vendor_no
join {{ ref('crosswalk_source_item') }} ci
  on ci.source_system = s.source_system and ci.source_key = s.item_no
