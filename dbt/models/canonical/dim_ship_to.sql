{{ config(materialized='table') }}
-- v0.1: the Ridgeline dealer export has no separate ship-to master, so each
-- customer contributes one ship-to derived from its address. When a source
-- with a real ship-to file onboards (most SQL ERPs have one), its staging
-- model unions in here keyed by its own surrogate; customer_key stays the
-- billing parent.
select
    {{ sk("'ship_to'", "stg.customer_no") }} as ship_to_key,
    cc.canonical_customer_key as customer_key,
    stg.customer_no as source_customer_no,
    stg.source_system,
    stg.customer_name as ship_to_name,
    stg.address1 as ship_to_address1,
    stg.city as ship_to_city,
    stg.state as ship_to_state,
    stg.postal_code as ship_to_postal_code,
    stg.loaded_at
from {{ demo_staging('customers') }} stg
join {{ ref('crosswalk_source_customer') }} cc
  on cc.source_system = stg.source_system and cc.source_key = stg.customer_no
