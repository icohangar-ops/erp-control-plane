-- Fill-rate service levels by branch (bar/pivot chart on the ops dashboard).
SELECT
    l.location_name AS branch,
    COUNT(DISTINCT i.invoice_key)  AS invoices,
    SUM(f.filled_qty)              AS filled_lines_qty,
    SUM(f.ordered_qty)             AS ordered_lines_qty,
    ROUND(
        SUM(f.filled_qty) / NULLIF(SUM(f.ordered_qty), 0), 4
    )                              AS line_fill_rate
FROM main_canonical.fact_invoice_line f
JOIN main_canonical.dim_item     i USING (item_key)
JOIN main_canonical.dim_location l USING (location_key)
GROUP BY l.location_name
ORDER BY line_fill_rate DESC
