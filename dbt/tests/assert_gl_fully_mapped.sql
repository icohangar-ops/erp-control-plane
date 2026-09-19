-- Every GL line must resolve to a consolidated account; an unmapped row means
-- a new source account was created without adding a COA mapping rule.
select source_gl_account, count(*) as unmapped_lines
from {{ ref('fact_gl_transaction') }}
where consolidated_account is null
group by 1
