-- Inventory efficiency by item category (GMROI / turns / DIO per category).
SELECT
    c.category_name,
    SUM(f.avg_inventory_value) AS avg_inventory_value,
    SUM(f.annualized_cogs)     AS annualized_cogs,
    SUM(f.annualized_gm_dollars) / NULLIF(SUM(f.avg_inventory_value), 0) AS gmroi,
    SUM(f.annualized_cogs) / NULLIF(SUM(f.avg_inventory_value), 0)       AS inventory_turns
FROM main_marts.kpi_inventory f
JOIN main_canonical.dim_item_category c USING (category_key)
GROUP BY c.category_name
ORDER BY gmroi DESC
