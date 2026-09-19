{{ config(materialized='table') }}
select
    cv.canonical_vendor_key as vendor_key,
    v.vendor_no as source_vendor_no,
    v.source_system,
    v.vendor_name,
    v.terms,
    v.lead_time_days,
    v.loaded_at
from {{ ref('stg_csvsftp__vendors') }} v
join {{ ref('crosswalk_source_vendor') }} cv
  on cv.source_system = v.source_system and cv.source_key = v.vendor_no
