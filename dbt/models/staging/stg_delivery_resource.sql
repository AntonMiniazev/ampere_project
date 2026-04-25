with source_data as (
    select * from {{ source('bronze', 'delivery_resource') }}
),
deduped as (
    select
        cast(id as smallint) as delivery_resource_id,
        cast(fullname as varchar) as delivery_resource_name,
        cast(delivery_type_id as smallint) as delivery_type_id,
        cast(store_id as smallint) as store_id,
        cast(created_at as date) as created_at,
        cast(updated_at as date) as updated_at,
        cast(active_flag as boolean) as is_active,
        {{ ampere_bronze_lineage_columns() }}
    from source_data
    qualify row_number() over (
        partition by id
        order by updated_at desc nulls last, created_at desc nulls last, _bronze_last_apply_ts desc
    ) = 1
)
select
    *,
    {{ ampere_silver_lineage_columns() }}
from deduped
