{{ config(materialized='table') }}
-- Effective-dated chart-of-accounts mapping: (source_company, source_account,
-- date) -> consolidated group account. Loaded from the reference seed; per
-- acquisition, new source GL accounts append rows here before their GL data
-- is promoted to canonical.
select
    m.source_company_id,
    m.source_gl_account,
    m.source_account_name,
    m.target_consolidated_account,
    cast(m.effective_start_date as date) as effective_start_date,
    cast(m.effective_end_date as date) as effective_end_date,
    m.mapping_rule_type,
    cast(m.active_flag as boolean) as active_flag
from {{ ref('seed_coa_mapping') }} m
