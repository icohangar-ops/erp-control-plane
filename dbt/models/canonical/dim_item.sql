{{ config(materialized='table') }}
-- Golden item master: one row per canonical item, source-attributed.
-- Additional sources append through their own crosswalk + union upstream.
select
    ci.canonical_item_key as item_key,
    i.item_no as source_item_no,
    i.source_system,
    i.description,
    i.category,
    i.subcategory,
    ic.category_key,
    i.uom,
    i.unit_cost as current_unit_cost,
    i.list_price as current_list_price,
    i.item_status,
    i.batch_id as last_batch_id,
    i.loaded_at
from {{ demo_staging('items') }} i
join {{ ref('crosswalk_source_item') }} ci
  on ci.source_system = i.source_system and ci.source_key = i.item_no
left join {{ ref('dim_item_category') }} ic
  on ic.category = i.category and ic.subcategory = i.subcategory
