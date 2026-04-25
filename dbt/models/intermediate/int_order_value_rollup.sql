select
    order_id,
    sum(quantity) as ordered_quantity,
    sum(quantity * sales_price) as line_sales_amount,
    count(*) as line_count,
    max(_bronze_last_run_id) as _bronze_last_run_id,
    max(_bronze_last_apply_ts) as _bronze_last_apply_ts,
    max(_bronze_last_manifest_path) as _bronze_last_manifest_path,
    {{ ampere_silver_lineage_columns() }}
from {{ ref('stg_order_product') }}
group by order_id
