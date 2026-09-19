{{ config(materialized='table') }}
select
    cs.canonical_salesperson_key as salesperson_key,
    s.salesperson_code as source_salesperson_code,
    s.source_system,
    s.salesperson_name,
    coalesce(loc.canonical_location_key, {{ sk("home_branch") }}) as home_location_key,
    s.home_branch,
    s.loaded_at
from {{ ref('stg_csvsftp__salespeople') }} s
join {{ ref('crosswalk_source_salesperson') }} cs
  on cs.source_system = s.source_system and cs.source_key = s.salesperson_code
left join {{ ref('dim_location') }} loc on loc.branch_code = s.home_branch
