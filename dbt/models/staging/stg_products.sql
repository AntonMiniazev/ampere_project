with source_data as (
    select * from {{ source('bronze', 'products') }}
),
deduped as (
    select
        cast(id as integer) as product_id,
        cast(product_name as varchar) as product_name,
        cast(price as decimal(10, 2)) as unit_price,
        cast(unit_type as varchar) as unit_type,
        cast(category_id as smallint) as category_id,
        cast(chance as decimal(8, 2)) as selection_weight,
        cast(valid_from as date) as valid_from,
        cast(valid_to as date) as valid_to,
        {{ ampere_bronze_lineage_columns() }}
    from source_data
    qualify row_number() over (
        partition by id, valid_from
        order by valid_to desc nulls last, _bronze_last_apply_ts desc
    ) = 1
)
select
    *,
    {{ ampere_silver_lineage_columns() }}
from deduped
