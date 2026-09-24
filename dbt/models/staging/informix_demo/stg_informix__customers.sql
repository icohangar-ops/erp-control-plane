select
    customer_no,
    customer_name,
    customer_class,
    terms,
    credit_limit,
    address1,
    city,
    state,
    postal_code,
    source_system,
    source_id,
    batch_id,
    loaded_at
from {{ source('informix_demo', 'customers') }}
