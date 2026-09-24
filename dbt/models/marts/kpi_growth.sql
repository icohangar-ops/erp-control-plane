{{ config(materialized='table') }}
-- Growth-mix KPIs:
--   same_branch_revenue_pct: revenue sold by the customer's modal branch
--     (home branch inferred from the customer's revenue mix — v0.1 has no
--     explicit home-branch field in the dealer export)
--   organic_revenue_pct: revenue from branches owned before the window start
--   acquired_revenue_pct: revenue from branches acquired during the window
--     (the seeded RL-JCB acquisition closes 2026-03-15)
with analysis_window as (select * from {{ ref('kpi_window') }}),
customer_branch_revenue as (
    select customer_key, location_key, sum(revenue_amount) as revenue_amount
    from {{ ref('fact_invoice_line') }}
    group by 1, 2
),
customer_home_branch as (
    select customer_key, location_key as home_location_key
    from (
        select
            customer_key,
            location_key,
            row_number() over (partition by customer_key order by revenue_amount desc) as rn
        from customer_branch_revenue
    )
    where rn = 1
),
invoice_joined as (
    select
        f.revenue_amount,
        f.location_key = h.home_location_key as is_same_branch,
        coalesce(loc.owned_since < (select window_start from analysis_window), false) as is_organic
    from {{ ref('fact_invoice_line') }} f
    join customer_home_branch h on h.customer_key = f.customer_key
    left join {{ ref('dim_location') }} loc on loc.location_key = f.location_key
)
select
    round(
        sum(case when is_same_branch then revenue_amount else 0 end)
        / nullif(sum(revenue_amount), 0),
        4
    ) as same_branch_revenue_pct,
    round(
        sum(case when is_organic then revenue_amount else 0 end)
        / nullif(sum(revenue_amount), 0),
        4
    ) as organic_revenue_pct,
    round(
        sum(case when not is_organic then revenue_amount else 0 end)
        / nullif(sum(revenue_amount), 0),
        4
    ) as acquired_revenue_pct
from invoice_joined
