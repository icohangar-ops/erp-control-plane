{{ config(materialized='table') }}
-- Maps (source_system, source_key) -> canonical item key. One row per source
-- item version; the canonical dim joins through this table so acquired ERPs
-- remap without touching canonical logic.
select
    source_system,
    item_no as source_key,
    {{ sk("source_system", "item_no") }} as canonical_item_key,
    item_status,
    loaded_at
from {{ demo_staging('items') }}
