{{ config(tags=['event']) }}

select
    concat_ws('|', cast(osh.order_id as varchar), cast(osh.status_datetime as varchar)) as fact_order_status_history_key,
    osh.order_id,
    fo.order_date,
    fo.client_id,
    fo.store_id,
    osh.order_status_id,
    os.order_status,
    osh.status_datetime,
    osh._bronze_last_run_id,
    osh._bronze_last_apply_ts,
    osh._bronze_last_manifest_path,
    {{ ampere_silver_lineage_columns() }}
from {{ ref('stg_order_status_history') }} as osh
left join {{ ref('fact_orders') }} as fo
    on osh.order_id = fo.order_id
left join {{ ref('dim_order_statuses') }} as os
    on osh.order_status_id = os.order_status_id
