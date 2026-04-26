{{ config(materialized='table') }}

with source_data as (
    select * from {{ source('bronze', 'payments') }}
),
deduped as (
    select
        cast(order_id as integer) as order_id,
        cast(amount as decimal(12, 4)) as payment_amount,
        cast(method as varchar) as payment_method,
        cast(payment_status as varchar) as payment_status,
        cast(payment_date as date) as payment_date,
        {{ ampere_bronze_lineage_columns() }}
    from source_data
    qualify row_number() over (
        partition by order_id, payment_date, payment_status
        order by _bronze_last_apply_ts desc
    ) = 1
)
select
    *,
    {{ ampere_silver_lineage_columns() }}
from deduped
