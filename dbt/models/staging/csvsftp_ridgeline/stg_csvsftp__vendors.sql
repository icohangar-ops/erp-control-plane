select
    vendor_no,
    vendor_name,
    terms,
    lead_time_days,
    source_system,
    source_id,
    batch_id,
    loaded_at
from {{ source('csvsftp_ridgeline', 'vendors') }}
