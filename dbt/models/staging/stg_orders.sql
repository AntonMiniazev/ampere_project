{{ config(materialized='table') }}

with source_data as (
    select *
    from {{ source('bronze', 'orders') }}
    where {{ ampere_silver_date_window_predicate('order_date') }}
),
deduped as (
    select
        cast(id as integer) as order_id,
        cast(client_id as integer) as client_id,
        cast(order_date as date) as order_date,
        cast(order_source_id as smallint) as order_source_id,
        cast(store_id as smallint) as store_id,
        cast(total_amount as decimal(12, 4)) as total_amount,
        {{ ampere_bronze_lineage_columns() }}
    from source_data
    qualify row_number() over (
        partition by id
        order by order_date desc nulls last, _bronze_last_apply_ts desc
    ) = 1
)
select
    *,
    {{ ampere_silver_lineage_columns() }}
from deduped
