{{ config(tags=['snapshot_dimension']) }}

select
    category_id,
    category_name,
    {{ ampere_silver_lineage_columns() }}
from {{ ref('stg_product_categories') }}
