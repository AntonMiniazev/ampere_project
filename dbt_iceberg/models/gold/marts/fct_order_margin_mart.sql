{{ config(alias='fct_order_margin', tags=['fact', 'publish']) }}

with product_cost as (
    select
        order_id,
        sum(total_cost) as product_cost_amount
    from {{ ref('dim_costing_mart') }}
    group by order_id
),

delivery_cost as (
    select
        order_id,
        sum(tariff) as delivery_cost_amount
    from {{ ref('dim_delivery_cost_mart') }}
    group by order_id
)

select
    sales.order_id,
    sales.order_date,
    sales.client_id,
    orders.store_id,
    sales.total_amount,
    cast(coalesce(product_cost.product_cost_amount, 0) as decimal(18, 4)) as product_cost_amount,
    cast(coalesce(delivery_cost.delivery_cost_amount, 0) as decimal(18, 4)) as delivery_cost_amount,
    cast(
        sales.total_amount
        - coalesce(product_cost.product_cost_amount, 0)
        - coalesce(delivery_cost.delivery_cost_amount, 0)
        as decimal(18, 4)
    ) as gross_profit_amount,
    cast(
        case
            when sales.total_amount = 0 then null
            else (
                sales.total_amount
                - coalesce(product_cost.product_cost_amount, 0)
                - coalesce(delivery_cost.delivery_cost_amount, 0)
            ) / sales.total_amount
        end
        as decimal(10, 4)
    ) as gross_margin_pct,
    {{ ampere_gold_lineage_columns() }}
from {{ ref('fct_orders_sales_mart') }} as sales
left join {{ ampere_gold_silver_relation('fact_orders') }} as orders
    on sales.order_id = orders.order_id
left join product_cost
    on sales.order_id = product_cost.order_id
left join delivery_cost
    on sales.order_id = delivery_cost.order_id
