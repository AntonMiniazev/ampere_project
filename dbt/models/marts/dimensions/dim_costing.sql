with current_costing as (
    select
        *,
        row_number() over (
            partition by product_id, store_id
            order by valid_from desc, valid_to desc nulls last, _bronze_last_apply_ts desc
        ) as row_num
    from {{ ref('stg_costing') }}
)
select
    concat_ws('|', cast(c.product_id as varchar), cast(c.store_id as varchar)) as costing_key,
    c.product_id,
    p.product_name,
    c.store_id,
    s.store_name,
    c.avg_cost,
    c.valid_from,
    c.valid_to,
    c._bronze_last_run_id,
    c._bronze_last_apply_ts,
    c._bronze_last_manifest_path,
    {{ ampere_silver_lineage_columns() }}
from current_costing as c
left join {{ ref('dim_products') }} as p
    on c.product_id = p.product_id
left join {{ ref('dim_stores') }} as s
    on c.store_id = s.store_id
where c.row_num = 1
