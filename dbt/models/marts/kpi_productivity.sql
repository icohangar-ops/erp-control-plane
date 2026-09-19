{{ config(materialized='table') }}
-- Productivity KPIs:
--   sales_per_fte_annualized = annualized window revenue / total group FTE
--     (FTE counts from the branch master; headcount is a group-wide monthly
--     figure in production — v0.1 uses branch FTE)
--   avg_ticket = revenue / distinct invoices
--   orders_per_active_day = orders / distinct order days (operational tempo)
with analysis_window as (select * from {{ ref('kpi_window') }}),
invoice_totals as (
    select
        sum(revenue_amount) as revenue_window,
        count(distinct invoice_key) as invoice_count,
        count(distinct invoice_date) as active_days
    from {{ ref('fact_invoice_line') }}
),
headcount as (
    select sum(fte_count) as total_fte from {{ ref('dim_location') }}
)
select
    round(
        i.revenue_window * (select annualization_factor from analysis_window)
        / nullif(h.total_fte, 0),
        0
    ) as sales_per_fte_annualized,
    round(i.revenue_window / nullif(i.invoice_count, 0), 2) as avg_ticket,
    round(i.invoice_count / nullif(i.active_days, 0), 2) as orders_per_active_day,
    i.revenue_window,
    i.invoice_count,
    h.total_fte
from invoice_totals i
cross join headcount h
