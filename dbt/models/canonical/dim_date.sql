{{ config(materialized='table') }}
-- Calendar spine for the analysis window plus lookahead.
with dates as (
    select cast(gs.generate_series as date) as date_day
    from generate_series(
        cast('2026-01-01' as timestamp),
        cast('2026-12-31' as timestamp),
        interval 1 day
    ) as gs
)
select
    date_day as date_key,
    extract(year from date_day) as year_number,
    extract(quarter from date_day) as quarter_number,
    extract(month from date_day) as month_number,
    strftime(date_day, '%Y-%m') as year_month,
    extract(day from date_day) as day_of_month,
    extract(dow from date_day) as day_of_week,
    date_trunc('week', date_day) as week_start_date,
    date_trunc('month', date_day) as month_start_date
from dates
