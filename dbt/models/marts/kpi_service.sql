{{ config(materialized='table') }}
-- Order-service KPIs. Line-grain metrics (fill rate, backorder rate) come
-- from active lines; order-grain metrics (fill, OTD, OTIF) from the worst
-- line on each order.
--   line_fill_rate  = SUM(least(filled, ordered)) / SUM(ordered), active lines
--   order_fill_rate = orders whose every line is complete / all orders
--   otd_pct         = fully shipped orders shipped on time / fully shipped
--   otif_pct        = orders complete AND on time / all orders
--   backorder_rate  = short-shipped active lines / active lines
with lines as (
    select *
    from {{ ref('fact_sales_order_line') }}
    where ordered_qty > 0
      and cancelled_qty < ordered_qty
),
line_metrics as (
    select
        round(
            sum(least(filled_qty, ordered_qty)) / nullif(sum(ordered_qty), 0),
            4
        ) as line_fill_rate,
        round(sum(case when is_backordered then 1 else 0 end) / count(*), 4) as backorder_rate,
        count(*) as active_line_count
    from lines
),
orders as (
    select
        order_key,
        min(cast(is_line_complete as integer)) as all_lines_complete,
        min(cast(is_on_time as integer)) as on_time_all_lines,
        bool_and(shipped_date is not null) as fully_shipped
    from lines
    group by order_key
),
order_metrics as (
    select
        count(*) as order_count,
        round(sum(all_lines_complete) / count(*), 4) as order_fill_rate,
        round(
            sum(case when fully_shipped and on_time_all_lines = 1 then 1 else 0 end)
            / nullif(sum(case when fully_shipped then 1 else 0 end), 0),
            4
        ) as otd_pct,
        round(
            sum(case when on_time_all_lines = 1 and all_lines_complete = 1 then 1 else 0 end)
            / count(*),
            4
        ) as otif_pct
    from orders
)
select
    l.line_fill_rate,
    l.backorder_rate,
    l.active_line_count,
    o.order_count,
    o.order_fill_rate,
    o.otd_pct,
    o.otif_pct
from line_metrics l
cross join order_metrics o
