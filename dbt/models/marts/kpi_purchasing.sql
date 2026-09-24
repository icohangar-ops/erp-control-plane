{{ config(materialized='table') }}
-- Purchasing KPIs over the PO book in the analysis window:
--   vendor_fill_rate = received qty / ordered qty
--   ppv_total        = SUM((actual unit cost - standard unit cost) x received qty)
--   ppv_pct          = PPV as a share of standard-cost receipts
select
    round(sum(received_qty) / nullif(sum(ordered_qty), 0), 4) as vendor_fill_rate,
    round(sum(purchase_price_variance), 2) as ppv_total,
    round(
        sum(purchase_price_variance)
        / nullif(sum(unit_cost_standard * received_qty), 0),
        4
    ) as ppv_pct,
    count(*) as po_line_count
from {{ ref('fact_purchase_order_line') }}
