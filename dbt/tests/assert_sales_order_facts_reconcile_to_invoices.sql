-- Filled order quantities must reconcile to invoiced quantities at the
-- document-pair level; divergence beyond tolerance signals lost linkage
-- between the order and invoice feeds.
with per_order as (
    select order_key, sum(least(filled_qty, ordered_qty)) as filled_qty
    from {{ ref('fact_sales_order_line') }}
    group by 1
)
select f.order_key
from (
    select order_key, sum(invoiced_qty) as invoiced_qty
    from {{ ref('fact_invoice_line') }}
    where source_order_no is not null
    group by 1
) f
left join per_order po on po.order_key = f.order_key
where po.order_key is null or abs(po.filled_qty - f.invoiced_qty) > 0.001
