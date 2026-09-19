{{ config(materialized='table') }}
-- Period-end stock position by branch/item. Value is stated by the source;
-- the mart layer recomputes value at canonical cost for reconciliation.
select
    {{ sk("s.snapshot_date", "s.branch_code", "s.item_no") }} as snapshot_key,
    s.source_system,
    cast(s.snapshot_date as date) as snapshot_date,
    {{ sk("s.branch_code") }} as location_key,
    ci.canonical_item_key as item_key,
    s.on_hand_qty,
    s.allocated_qty,
    s.on_order_qty,
    s.backorder_qty,
    s.unit_cost,
    s.inventory_value,
    s.source_file,
    s.source_row_no,
    s.batch_id,
    s.loaded_at
from {{ ref('stg_csvsftp__inventory_snapshots') }} s
join {{ ref('crosswalk_source_item') }} ci
  on ci.source_system = s.source_system and ci.source_key = s.item_no
