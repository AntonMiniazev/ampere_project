{{ config(alias='dim_products', tags=['dimension', 'publish']) }}

select
    product_id,
    product_name,
    category_name,
    {{ ampere_gold_lineage_columns() }}
from {{ ampere_gold_silver_relation('dim_products') }}
