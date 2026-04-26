{{ config(tags=['mutable_dimension']) }}

select
    dr.delivery_resource_id,
    dr.delivery_resource_name,
    dr.delivery_type_id,
    dt.courier_type,
    dr.store_id,
    s.store_name,
    dr.created_at,
    dr.updated_at,
    dr.is_active,
    dr._bronze_last_run_id,
    dr._bronze_last_apply_ts,
    dr._bronze_last_manifest_path,
    {{ ampere_silver_lineage_columns() }}
from {{ ref('stg_delivery_resource') }} as dr
left join {{ ref('dim_delivery_type') }} as dt
    on dr.delivery_type_id = dt.delivery_type_id
left join {{ ref('dim_stores') }} as s
    on dr.store_id = s.store_id
