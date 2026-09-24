{{ config(materialized='table') }}
-- Inventory efficiency KPIs (annualized to the derived window):
--   turns            = window COGS x annualization / avg inventory value
--   dio_days         = 365 / turns
--   weeks_of_supply  = avg inventory value / (window COGS / window_days x 7)
--   gmroi            = annualized gross margin dollars / avg inventory value
-- Avg inventory value is the mean of month-end snapshot totals.
with analysis_window as (select * from {{ ref('kpi_window') }}),
cogs as (
    select sum(cogs_amount) as cogs_window,
           sum(gross_margin_amount) as gm_window
    from {{ ref('fact_invoice_line') }}
),
avg_inv as (
    select avg(month_value) as avg_inventory_value
    from (
        select snapshot_date, sum(inventory_value) as month_value
        from {{ ref('fact_inventory_snapshot') }}
        group by 1
    )
)
select
    w.window_start,
    w.window_end,
    w.window_days,
    round(c.cogs_window * w.annualization_factor / a.avg_inventory_value, 2) as inventory_turns,
    round(365.0 / (c.cogs_window * w.annualization_factor / a.avg_inventory_value), 1) as dio_days,
    round(a.avg_inventory_value / (c.cogs_window / w.window_days * 7), 1) as weeks_of_supply,
    round(c.gm_window * w.annualization_factor / a.avg_inventory_value, 2) as gmroi
from analysis_window w, cogs c, avg_inv a
