with source_data as (
    select * from {{ source('bronze', 'assortment') }}
)
select
    cast(product_id as integer) as product_id,
    cast(store_id as smallint) as store_id,
    {{ ampere_bronze_lineage_columns() }},
    {{ ampere_silver_lineage_columns() }}
from source_data
