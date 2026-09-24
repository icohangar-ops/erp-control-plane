{{ config(materialized='table') }}
select
    source_system,
    customer_no as source_key,
    {{ sk("source_system", "customer_no") }} as canonical_customer_key,
    loaded_at
from {{ demo_staging('customers') }}
