select
    concat_ws('|', cast(op.order_id as varchar), cast(op.product_id as varchar), cast(op.order_date as varchar)) as fact_order_product_key,
    op.order_id,
    fo.order_date,
    fo.client_id,
    fo.store_id,
    op.product_id,
    p.product_name,
    p.category_id,
    p.category_name,
    op.quantity,
    op.sales_price,
    cast(op.quantity * op.sales_price as decimal(14, 4)) as line_sales_amount,
    {{ ampere_silver_lineage_columns() }}
from {{ ref('stg_order_product') }} as op
left join {{ ref('fact_orders') }} as fo
    on op.order_id = fo.order_id
left join {{ ref('dim_products') }} as p
    on op.product_id = p.product_id
