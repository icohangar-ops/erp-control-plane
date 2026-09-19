select
    item_no,
    description,
    category,
    subcategory,
    uom,
    unit_cost,
    list_price,
    item_status,
    source_system,
    source_id,
    batch_id,
    loaded_at
from {{ source('informix_demo', 'items') }}
