select
    o.order_id,
    o.order_date,
    o.client_id,
    c.client_full_name,
    c.is_churned as client_is_churned,
    o.store_id,
    s.store_name,
    o.order_source_id,
    o.total_amount,
    coalesce(v.ordered_quantity, 0) as ordered_quantity,
    coalesce(v.line_sales_amount, 0) as line_sales_amount,
    coalesce(v.line_count, 0) as line_count,
    ls.latest_order_status_id,
    ls.latest_order_status,
    ls.latest_status_datetime,
    cast(abs(o.total_amount - coalesce(v.line_sales_amount, 0)) < 0.01 as boolean) as amount_matches_lines,
    o._bronze_last_run_id,
    o._bronze_last_apply_ts,
    o._bronze_last_manifest_path,
    {{ ampere_silver_lineage_columns() }}
from {{ ref('stg_orders') }} as o
left join {{ ref('dim_clients') }} as c
    on o.client_id = c.client_id
left join {{ ref('dim_stores') }} as s
    on o.store_id = s.store_id
left join {{ ref('int_order_value_rollup') }} as v
    on o.order_id = v.order_id
left join {{ ref('int_orders_latest_status') }} as ls
    on o.order_id = ls.order_id
