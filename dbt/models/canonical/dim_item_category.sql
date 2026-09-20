{{ config(materialized='table') }}
select distinct
    {{ sk("category", "coalesce(subcategory, '-')") }} as category_key,
    category,
    subcategory
from {{ demo_staging('items') }}
where category is not null
