with source_data as (
    select * from {{ source('bronze', 'clients') }}
),
deduped as (
    select
        cast(id as integer) as client_id,
        cast(fullname as varchar) as client_full_name,
        cast(preferred_store_id as smallint) as preferred_store_id,
        cast(registration_date as date) as registration_date,
        cast(updated_at as date) as updated_at,
        cast(churned as boolean) as is_churned,
        {{ ampere_bronze_lineage_columns() }}
    from source_data
    qualify row_number() over (
        partition by id
        order by updated_at desc nulls last, registration_date desc nulls last, _bronze_last_apply_ts desc
    ) = 1
)
select
    *,
    {{ ampere_silver_lineage_columns() }}
from deduped
