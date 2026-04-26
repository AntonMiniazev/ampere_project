{{ config(materialized='table') }}

select
    cast(order_id as integer) as order_id,
    cast(order_date as date) as order_date,
    cast(product_id as integer) as product_id,
    cast(quantity as decimal(10, 2)) as quantity,
    cast(sales_price as decimal(10, 2)) as sales_price,
    {{ ampere_bronze_lineage_columns() }},
    {{ ampere_silver_lineage_columns() }}
from {{ source('bronze', 'order_product') }}
