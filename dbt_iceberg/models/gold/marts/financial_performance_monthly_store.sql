{{ config(alias='financial_performance_monthly_store', tags=['gold', 'publish']) }}

with product_costs as (
    select
        lines.order_id,
        lines.order_date,
        lines.store_id,
        cast(case when count(lines.product_cost_amount) = count(*)
                  then sum(lines.product_cost_amount) end as decimal(14, 4)) as product_cost_amount
    from {{ ref('int_product_line_costs') }} as lines
    group by 1, 2, 3
),
delivery_costs as (
    select order_id, delivery_cost_amount from {{ ref('int_delivery_order_costs') }}
),
actuals as (
    select
        cast(date_trunc('month', orders.order_date) as date) as month,
        orders.store_id,
        cast(sum(orders.total_amount) as decimal(14, 4)) as revenue_amount,
        cast(case when count(product_costs.product_cost_amount) = count(*)
                  then sum(product_costs.product_cost_amount) end as decimal(14, 4)) as product_cost_amount,
        cast(case when count(delivery_costs.delivery_cost_amount) = count(*)
                  then sum(delivery_costs.delivery_cost_amount) end as decimal(14, 4)) as delivery_cost_amount,
        cast(case when count(product_costs.product_cost_amount) = count(*)
                   and count(delivery_costs.delivery_cost_amount) = count(*)
                  then sum(orders.total_amount - product_costs.product_cost_amount
                           - delivery_costs.delivery_cost_amount) end as decimal(14, 4)) as gross_profit_amount
    from {{ ref('fact_orders') }} as orders
    left join product_costs
        on orders.order_id = product_costs.order_id
       and orders.order_date = product_costs.order_date
       and orders.store_id = product_costs.store_id
    left join delivery_costs on orders.order_id = delivery_costs.order_id
    where orders.latest_order_status_id = 3
      and orders.order_date is not null
      and {{ ampere_gold_month_predicate("date_trunc('month', orders.order_date)") }}
    group by 1, 2
),
budgets as (
    select
        cast(date_trunc('month', budget_date) as date) as month,
        store_id,
        cast(sum(sales_amount_daily) as decimal(14, 4)) as budget_revenue_amount
    from {{ ref('silver_budget_orders_sales') }}
    where budget_date is not null
      and {{ ampere_gold_month_predicate("date_trunc('month', budget_date)") }}
    group by 1, 2
)
select
    coalesce(actuals.month, budgets.month) as month,
    coalesce(actuals.store_id, budgets.store_id) as store_id,
    coalesce(stores.store_name, 'Unknown') as store_name,
    stores.city,
    cast(coalesce(actuals.revenue_amount, 0) as decimal(14, 4)) as revenue_amount,
    cast(case when actuals.month is null then 0 else actuals.product_cost_amount end as decimal(14, 4)) as product_cost_amount,
    cast(case when actuals.month is null then 0 else actuals.delivery_cost_amount end as decimal(14, 4)) as delivery_cost_amount,
    cast(case when actuals.month is null then 0 else actuals.gross_profit_amount end as decimal(14, 4)) as gross_profit_amount,
    budgets.budget_revenue_amount
from actuals
full outer join budgets on actuals.month = budgets.month and actuals.store_id = budgets.store_id
left join {{ ref('dim_stores') }} as stores
    on coalesce(actuals.store_id, budgets.store_id) = stores.store_id
