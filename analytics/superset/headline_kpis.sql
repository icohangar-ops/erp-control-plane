-- Headline KPI tile source for the "Executive Overview" dashboard.
-- Dataset: connect Superset to the customer's analytics DuckDB (or Postgres
-- mirror) and create a physical dataset from this query. Single row; one
-- column per KPI; values are the trailing window shown by kpi_window.
SELECT
    window_start,
    window_end,
    window_days,
    gmroi,
    inventory_turns,
    dio_days,
    weeks_of_supply,
    gross_margin_pct,
    line_fill_rate,
    order_fill_rate,
    otd_pct,
    otif_pct,
    backorder_rate,
    vendor_fill_rate,
    ppv_pct,
    dso_days,
    dpo_days,
    ccc_days,
    close_cycle_days,
    same_branch_revenue_pct,
    organic_revenue_pct,
    acquired_revenue_pct,
    sales_per_fte_annualized,
    avg_ticket
FROM main_marts.kpi_headline
