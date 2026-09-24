{{ config(materialized='table') }}
-- Branch/location master. owned_since drives the organic-vs-acquired split;
-- fte_count denominates sales-per-FTE.
select
    {{ sk("branch_code") }} as location_key,
    b.branch_code,
    b.branch_name,
    b.region,
    cast(b.owned_since as date) as owned_since,
    cast(b.fte_count as integer) as fte_count,
    {{ sk("branch_code") }} as canonical_location_key
from {{ ref('seed_branches') }} b
