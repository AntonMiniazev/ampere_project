{{ config(alias='dim_costing', tags=['allocation', 'publish']) }}

with order_products as (
    select
        order_id,
        order_date,
        product_id,
        quantity,
        store_id
    from {{ ampere_gold_silver_relation('fact_order_product') }}
    where {{ ampere_gold_date_window_predicate('order_date') }}
),

effective_costing as (
    select
        op.order_id,
        op.order_date,
        op.product_id,
        op.quantity,
        op.store_id,
        c.avg_cost
    from order_products as op
    left join {{ ampere_gold_silver_relation('dim_costing') }} as c
        on op.product_id = c.product_id
        and op.store_id = c.store_id
        and op.order_date >= c.valid_from
        and (c.valid_to is null or op.order_date < c.valid_to)
)

select
    order_id,
    order_date,
    product_id,
    quantity,
    store_id,
    avg_cost,
    cast(quantity * avg_cost as decimal(18, 4)) as total_cost,
    {{ ampere_gold_lineage_columns() }}
from effective_costing
