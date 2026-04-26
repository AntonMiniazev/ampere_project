{{ config(tags=['mutable_dimension']) }}

with current_products as (
    select
        *,
        row_number() over (
            partition by product_id
            order by valid_from desc, valid_to desc nulls last, _bronze_last_apply_ts desc
        ) as row_num
    from {{ ref('stg_products') }}
)
select
    p.product_id,
    p.product_name,
    p.unit_price,
    p.unit_type,
    p.category_id,
    c.category_name,
    p.selection_weight,
    p.valid_from,
    p.valid_to,
    p._bronze_last_run_id,
    p._bronze_last_apply_ts,
    p._bronze_last_manifest_path,
    {{ ampere_silver_lineage_columns() }}
from current_products as p
left join {{ ref('dim_product_categories') }} as c
    on p.category_id = c.category_id
where p.row_num = 1
