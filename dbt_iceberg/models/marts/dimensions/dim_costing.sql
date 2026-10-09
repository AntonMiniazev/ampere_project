{{ config(tags=['mutable_dimension']) }}

select
    concat_ws(
        '|',
        cast(c.product_id as varchar),
        cast(c.store_id as varchar),
        cast(c.valid_from as varchar)
    ) as costing_key,
    c.product_id,
    p.product_name,
    c.store_id,
    s.store_name,
    c.avg_cost,
    c.valid_from,
    c.valid_to,
    {{ ampere_silver_lineage_columns() }}
from {{ ref('stg_costing') }} as c
left join {{ ref('dim_products') }} as p
    on c.product_id = p.product_id
left join {{ ref('dim_stores') }} as s
    on c.store_id = s.store_id
