with source_data as (
    select * from {{ source('bronze', 'order_status_history') }}
),
deduped as (
    select
        cast(order_id as integer) as order_id,
        cast(order_status_id as smallint) as order_status_id,
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
