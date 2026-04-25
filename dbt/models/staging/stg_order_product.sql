with source_data as (
    select * from {{ source('bronze', 'order_product') }}
),
deduped as (
    select
        cast(order_id as integer) as order_id,
        cast(order_date as date) as order_date,
        cast(product_id as integer) as product_id,
        cast(quantity as decimal(10, 2)) as quantity,
        cast(sales_price as decimal(10, 2)) as sales_price,
        {{ ampere_bronze_lineage_columns() }}
    from source_data
    qualify row_number() over (
        partition by order_id, product_id, order_date
        order by _bronze_last_apply_ts desc
    ) = 1
)
select
    *,
    {{ ampere_silver_lineage_columns() }}
from deduped
