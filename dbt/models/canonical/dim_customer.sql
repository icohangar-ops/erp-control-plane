{{ config(materialized='table') }}
select
    cc.canonical_customer_key as customer_key,
    c.customer_no as source_customer_no,
    c.source_system,
    c.customer_name,
    c.customer_class,
    c.terms,
    c.credit_limit,
    c.city,
    c.state,
    c.postal_code,
    c.batch_id as last_batch_id,
    c.loaded_at
from {{ demo_staging('customers') }} c
join {{ ref('crosswalk_source_customer') }} cc
  on cc.source_system = c.source_system and cc.source_key = c.customer_no
