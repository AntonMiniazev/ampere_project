with source_data as (
    select * from {{ source('bronze', 'costing') }}
),
deduped as (
    select
        cast(product_id as integer) as product_id,
        cast(store_id as smallint) as store_id,
        cast(avg_cost as decimal(10, 2)) as avg_cost,
        cast(valid_from as date) as valid_from,
        cast(valid_to as date) as valid_to,
        {{ ampere_bronze_lineage_columns() }}
    from source_data
    qualify row_number() over (
        partition by product_id, store_id, valid_from
        order by valid_to desc nulls last, _bronze_last_apply_ts desc
    ) = 1
)
select
    *,
    {{ ampere_silver_lineage_columns() }}
from deduped
