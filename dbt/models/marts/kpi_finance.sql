{{ config(materialized='table') }}
-- Finance KPIs. Balances come from the mapped GL (consolidated accounts
-- GRP-1100 = accounts receivable, GRP-2000 = accounts payable, resolved via
-- the effective-dated COA mapping in fact_gl_transaction).
--   gross_margin_pct = (revenue - COGS) / revenue
--   dso_days         = AR balance / revenue x 365
--   dpo_days         = AP balance / COGS x 365
--   ccc_days         = DIO + DSO - DPO   (DIO from kpi_inventory)
--   close_cycle_days = average days from period end to close completion
with invoice_totals as (
    select
        sum(revenue_amount) as revenue_window,
        sum(cogs_amount) as cogs_window,
        sum(gross_margin_amount) as gm_window
    from {{ ref('fact_invoice_line') }}
),
balances as (
    select
        sum(case when consolidated_account = 'GRP-1100' then net_amount end) as ar_balance,
        sum(case when consolidated_account = 'GRP-2000' then net_amount end) as ap_balance
    from {{ ref('fact_gl_transaction') }}
),
close_cycle as (
    select avg(date_diff('day', period_end_date, close_completed_on)) as close_cycle_days
    from {{ ref('seed_close_calendar') }}
)
select
    round(i.gm_window / nullif(i.revenue_window, 0), 4) as gross_margin_pct,
    round(i.revenue_window, 2) as revenue_window,
    round(i.cogs_window, 2) as cogs_window,
    round(b.ar_balance / nullif(i.revenue_window, 0) * 365, 1) as dso_days,
    round(abs(b.ap_balance) / nullif(i.cogs_window, 0) * 365, 1) as dpo_days,
    round(
        inv.dio_days
        + b.ar_balance / nullif(i.revenue_window, 0) * 365
        - abs(b.ap_balance) / nullif(i.cogs_window, 0) * 365,
        1
    ) as ccc_days,
    round(c.close_cycle_days, 1) as close_cycle_days
from invoice_totals i
cross join balances b
cross join close_cycle c
cross join {{ ref('kpi_inventory') }} inv
