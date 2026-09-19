select
    snapshot_date,
    branch_code,
    item_no,
    on_hand_qty,
    allocated_qty,
    on_order_qty,
    backorder_qty,
    unit_cost,
    inventory_value,
    source_system,
    source_id,
    source_file,
    source_row_no,
    batch_id,
    loaded_at
from {{ source('informix_demo', 'inventory_snapshots') }}
