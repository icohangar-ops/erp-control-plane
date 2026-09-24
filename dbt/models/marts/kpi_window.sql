{{ config(materialized='table') }}
-- The analysis window is derived from the invoiced data itself, so KPI
-- annualization (turns, GMROI, DSO/DPO) reproduces from any window of data.
select
    min(invoice_date) as window_start,
    max(invoice_date) as window_end,
    date_diff('day', min(invoice_date), max(invoice_date)) + 1 as window_days,
    365.0 / (date_diff('day', min(invoice_date), max(invoice_date)) + 1) as annualization_factor
from {{ ref('fact_invoice_line') }}
