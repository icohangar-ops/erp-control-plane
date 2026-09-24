select
    salesperson_code,
    salesperson_name,
    home_branch,
    source_system,
    source_id,
    batch_id,
    loaded_at
from {{ source('informix_demo', 'salespeople') }}
