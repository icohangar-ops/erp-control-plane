{{ config(materialized='table') }}
select
    source_system,
    salesperson_code as source_key,
    {{ sk("source_system", "salesperson_code") }} as canonical_salesperson_key,
    loaded_at
from {{ demo_staging('salespeople') }}
