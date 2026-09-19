{{ config(materialized='table') }}
-- Effective-dated reference price list (per source item). New batches open a
-- new effective interval rather than overwriting history, so margin
-- comparisons across acquisitions reproduce.
select
    {{ sk("i.source_system", "i.item_no") }} as item_source_key,
    ci.canonical_item_key as item_key,
    i.source_system,
    i.list_price as unit_list_price,
    cast(i.loaded_at as date) as effective_from,
    cast('9999-12-31' as date) as effective_to,
    i.batch_id
from {{ ref('stg_csvsftp__items') }} i
join {{ ref('crosswalk_source_item') }} ci
  on ci.source_system = i.source_system and ci.source_key = i.item_no
where i.list_price is not null
