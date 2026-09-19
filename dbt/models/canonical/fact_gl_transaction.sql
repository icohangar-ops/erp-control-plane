{{ config(materialized='table') }}
-- GL lines mapped to the consolidated account through the effective-dated
-- COA mapping (account + entry_date inside the effective window, active rule).
select
    {{ sk("s.source_system", "s.journal_no", "s.line_no") }} as gl_line_key,
    s.source_system,
    s.journal_no as source_doc_no,
    s.line_no as source_line_no,
    {{ sk("s.branch_code") }} as location_key,
    s.entry_date,
    s.account as source_gl_account,
    coa.target_consolidated_account as consolidated_account,
    coa.mapping_rule_type,
    s.description,
    s.debit_amt,
    s.credit_amt,
    s.debit_amt - s.credit_amt as net_amount,
    s.source_file,
    s.source_row_no,
    s.batch_id,
    s.loaded_at
from {{ ref('stg_csvsftp__gl_entries') }} s
left join {{ ref('ref_coa_mapping') }} coa
  on coa.source_company_id = 'ridgeline_lumber'
 and coa.source_gl_account = s.account
 and coa.active_flag
 and s.entry_date >= coa.effective_start_date
 and s.entry_date <= coa.effective_end_date
