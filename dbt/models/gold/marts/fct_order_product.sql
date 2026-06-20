{{ config(alias='fct_order_product', tags=['fact', 'publish']) }}

select
    fop.order_id,
    fop.product_id,
    fop.store_id,
    fop.order_date,
    fop.line_sales_amount,    
    {{ ampere_gold_lineage_columns() }}
from {{ ampere_gold_silver_relation('fact_order_product') }} as fop
left join {{ ampere_gold_silver_relation('fact_orders') }} as fo
    on fop.order_id = fo.order_id
where fo.latest_order_status_id = 3