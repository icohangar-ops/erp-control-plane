{{ config(materialized='table') }}
select
    source_system,
    vendor_no as source_key,
    {{ sk("source_system", "vendor_no") }} as canonical_vendor_key,
    loaded_at
from {{ demo_staging('vendors') }}
