{{ config(materialized='table') }}
-- Maps (source_system, source_key) -> canonical location key. v0.1 sources
-- share the group branch code list (seed branch master); when an acquired
-- ERP uses its own branch ids, its staging models append their rows here and
-- facts resolve canonical keys through this table instead of raw codes.
select
    'seed_reference' as source_system,
    branch_code as source_key,
    {{ sk("branch_code") }} as canonical_location_key,
    current_timestamp::timestamp as loaded_at
from {{ ref('seed_branches') }}
