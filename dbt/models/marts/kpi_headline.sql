{{ config(materialized='table') }}
-- One row of headline KPIs for the demo/BI surface. Backed by the domain
-- views (kpi_inventory, kpi_service, kpi_finance, kpi_growth,
-- kpi_purchasing, kpi_productivity) so each keeps its own grain and docs.
select
    inv.gmroi,
    inv.inventory_turns,
    inv.dio_days,
    inv.weeks_of_supply,
    fin.gross_margin_pct,
    svc.line_fill_rate,
    svc.order_fill_rate,
    svc.otd_pct,
    svc.otif_pct,
    svc.backorder_rate,
    pur.vendor_fill_rate,
    pur.ppv_pct,
    fin.dso_days,
    fin.dpo_days,
    fin.ccc_days,
    fin.close_cycle_days,
    gro.same_branch_revenue_pct,
    gro.organic_revenue_pct,
    gro.acquired_revenue_pct,
    pro.sales_per_fte_annualized,
    pro.avg_ticket,
    w.window_start,
    w.window_end,
    w.window_days
from {{ ref('kpi_window') }} w
cross join {{ ref('kpi_inventory') }} inv
cross join {{ ref('kpi_service') }} svc
cross join {{ ref('kpi_finance') }} fin
cross join {{ ref('kpi_growth') }} gro
cross join {{ ref('kpi_purchasing') }} pur
cross join {{ ref('kpi_productivity') }} pro
