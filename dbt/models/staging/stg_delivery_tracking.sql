with source_data as (
    select * from {{ source('bronze', 'delivery_tracking') }}
),
deduped as (
    select
        cast(order_id as integer) as order_id,
        cast(courier_id as smallint) as courier_id,
        cast(delivery_status_id as smallint) as delivery_status_id,
        cast(status as varchar) as delivery_status,
        cast(status_datetime as timestamp) as status_datetime,
        {{ ampere_bronze_lineage_columns() }}
    from source_data
    qualify row_number() over (
        partition by order_id, status_datetime
        order by _bronze_last_apply_ts desc
    ) = 1
)
select
    *,
    {{ ampere_silver_lineage_columns() }}
from deduped
