{{ config(materialized='table') }}
select distinct
    {{ sk("category", "coalesce(subcategory, '-')") }} as category_key,
    category,
    subcategory
from {{ ref('stg_csvsftp__items') }}
where category is not null
