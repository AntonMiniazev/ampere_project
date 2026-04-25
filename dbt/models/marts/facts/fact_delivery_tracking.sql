select
    concat_ws('|', cast(dt.order_id as varchar), cast(dt.status_datetime as varchar)) as fact_delivery_tracking_key,
    dt.order_id,
    fo.order_date,
    fo.client_id,
    fo.store_id,
    dt.courier_id,
    dr.delivery_resource_name,
    dr.courier_type,
    dt.delivery_status_id,
    dt.delivery_status,
    dt.status_datetime,
    dt._bronze_last_run_id,
    dt._bronze_last_apply_ts,
    dt._bronze_last_manifest_path,
    {{ ampere_silver_lineage_columns() }}
from {{ ref('stg_delivery_tracking') }} as dt
left join {{ ref('fact_orders') }} as fo
    on dt.order_id = fo.order_id
left join {{ ref('dim_delivery_resource') }} as dr
    on dt.courier_id = dr.delivery_resource_id
