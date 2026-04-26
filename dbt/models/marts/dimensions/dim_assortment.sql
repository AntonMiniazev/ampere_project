{{ config(tags=['snapshot_dimension']) }}

with latest_assortment as (
    select *
    from {{ ref('stg_assortment') }}
    qualify row_number() over (
        partition by store_id, product_id
        order by
            try_cast(_bronze_last_apply_ts as timestamp) desc nulls last,
            _bronze_last_run_id desc nulls last
    ) = 1
)
select
    concat_ws('|', cast(a.store_id as varchar), cast(a.product_id as varchar)) as assortment_key,
    a.store_id,
    s.store_name,
    a.product_id,
    p.product_name,
    p.category_id,
    p.category_name,
    a._bronze_last_run_id,
    a._bronze_last_apply_ts,
    a._bronze_last_manifest_path,
    {{ ampere_silver_lineage_columns() }}
from latest_assortment as a
left join {{ ref('dim_stores') }} as s
    on a.store_id = s.store_id
left join {{ ref('dim_products') }} as p
    on a.product_id = p.product_id
