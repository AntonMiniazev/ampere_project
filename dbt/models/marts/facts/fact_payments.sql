select
    concat_ws('|', cast(p.order_id as varchar), cast(p.payment_date as varchar), coalesce(p.payment_status, 'unknown')) as fact_payments_key,
    p.order_id,
    fo.order_date,
    fo.client_id,
    fo.store_id,
    p.payment_date,
    p.payment_amount,
    p.payment_method,
    p.payment_status,
    fo.latest_order_status,
    p._bronze_last_run_id,
    p._bronze_last_apply_ts,
    p._bronze_last_manifest_path,
    {{ ampere_silver_lineage_columns() }}
from {{ ref('stg_payments') }} as p
left join {{ ref('fact_orders') }} as fo
    on p.order_id = fo.order_id
