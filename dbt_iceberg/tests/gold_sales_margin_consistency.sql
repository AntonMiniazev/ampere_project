{{ config(tags=['gold']) }}

select
    coalesce(s.order_id, m.order_id) as order_id
from {{ ref('fct_orders_sales_mart') }} as s
full outer join {{ ref('fct_order_margin_mart') }} as m
    on s.order_id = m.order_id
where s.order_id is null
    or m.order_id is null
    or s.total_amount is distinct from m.total_amount
