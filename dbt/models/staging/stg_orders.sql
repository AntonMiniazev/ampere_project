{{ config(materialized='table') }}

with source_data as (
    select *
    from {{ source('bronze', 'orders') }}
    where {{ ampere_silver_date_window_predicate('order_date') }}
        or id in (
            select distinct order_id
            from {{ source('bronze', 'order_product') }}
            where {{ ampere_silver_date_window_predicate('order_date') }}
        )
        or id in (
            select distinct order_id
            from {{ source('bronze', 'payments') }}
            where {{ ampere_silver_date_window_predicate('payment_date') }}
        )
        or id in (
            select distinct order_id
            from {{ source('bronze', 'order_status_history') }}
            where {{ ampere_silver_date_window_predicate('status_datetime') }}
        )
        or id in (
            select distinct order_id
            from {{ source('bronze', 'delivery_tracking') }}
            where {{ ampere_silver_date_window_predicate('status_datetime') }}
        )
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
