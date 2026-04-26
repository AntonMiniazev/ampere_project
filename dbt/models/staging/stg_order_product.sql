{{ config(materialized='table') }}

with typed_source as (
    select
        cast(order_id as integer) as order_id,
        cast(order_date as date) as order_date,
        cast(product_id as integer) as product_id,
        cast(quantity as decimal(10, 2)) as quantity,
        cast(sales_price as decimal(10, 2)) as sales_price,
        {{ ampere_bronze_lineage_columns() }}
    from {{ source('bronze', 'order_product') }}
),
deduped as (
    select
        order_id,
        order_date,
        product_id,
        arg_max(quantity, _bronze_last_apply_ts) as quantity,
        arg_max(sales_price, _bronze_last_apply_ts) as sales_price,
        arg_max(_bronze_last_run_id, _bronze_last_apply_ts) as _bronze_last_run_id,
        max(_bronze_last_apply_ts) as _bronze_last_apply_ts,
        arg_max(_bronze_last_manifest_path, _bronze_last_apply_ts) as _bronze_last_manifest_path
    from typed_source
    group by order_id, order_date, product_id
)
select
    *,
    {{ ampere_silver_lineage_columns() }}
from deduped
