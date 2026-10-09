{{ config(alias='curie_financial_performance_monthly_store', tags=['gold', 'publish']) }}

with product_costs as (
    select
        lines.order_id,
        lines.order_date,
        lines.store_id,
        cast(sum(lines.quantity * costing.avg_cost) as decimal(14, 4)) as product_cost_amount
    from {{ ref('fact_order_product') }} as lines
    left join {{ ref('dim_costing') }} as costing
        on lines.product_id = costing.product_id
       and lines.store_id = costing.store_id
       and lines.order_date >= costing.valid_from
       and (costing.valid_to is null or lines.order_date < costing.valid_to)
    group by 1, 2, 3
),
delivery_costs as (
    select tracking.order_id, max(costing.tariff) as delivery_cost_amount
    from {{ ref('fact_delivery_tracking') }} as tracking
    inner join {{ ref('fact_orders') }} as orders on tracking.order_id = orders.order_id
    left join {{ ref('dim_delivery_resource') }} as resource
        on tracking.courier_id = resource.delivery_resource_id
    left join {{ ref('dim_zones') }} as zones on orders.store_id = zones.store_id
    left join {{ ref('dim_delivery_costing') }} as costing
        on zones.zone_id = costing.zone_id and resource.delivery_type_id = costing.delivery_type_id
    where tracking.delivery_status_id = 2
    group by tracking.order_id
),
actuals as (
    select
        cast(date_trunc('month', orders.order_date) as date) as month,
        orders.store_id,
        cast(sum(orders.total_amount) as decimal(14, 4)) as revenue_amount,
        cast(sum(coalesce(product_costs.product_cost_amount, 0)) as decimal(14, 4)) as product_cost_amount,
        cast(sum(coalesce(delivery_costs.delivery_cost_amount, 0)) as decimal(14, 4)) as delivery_cost_amount,
        cast(sum(orders.total_amount - coalesce(product_costs.product_cost_amount, 0)
            - coalesce(delivery_costs.delivery_cost_amount, 0)) as decimal(14, 4)) as gross_profit_amount
    from {{ ref('fact_orders') }} as orders
    left join product_costs
        on orders.order_id = product_costs.order_id
       and orders.order_date = product_costs.order_date
       and orders.store_id = product_costs.store_id
    left join delivery_costs on orders.order_id = delivery_costs.order_id
    where orders.latest_order_status_id = 3
      and orders.order_date is not null
      and {{ ampere_gold_month_window_predicate('orders.order_date') }}
    group by 1, 2
),
budgets as (
    select
        cast(date_trunc('month', budget_date) as date) as month,
        store_id,
        cast(sum(sales_amount_daily) as decimal(14, 4)) as budget_revenue_amount
    from {{ ref('silver_budget_orders_sales') }}
    where budget_date is not null
      and {{ ampere_gold_month_window_predicate('budget_date') }}
    group by 1, 2
)
select
    coalesce(actuals.month, budgets.month) as month,
    coalesce(actuals.store_id, budgets.store_id) as store_id,
    coalesce(stores.store_name, 'Unknown') as store_name,
    stores.city,
    cast(coalesce(actuals.revenue_amount, 0) as decimal(14, 4)) as revenue_amount,
    cast(coalesce(actuals.product_cost_amount, 0) as decimal(14, 4)) as product_cost_amount,
    cast(coalesce(actuals.delivery_cost_amount, 0) as decimal(14, 4)) as delivery_cost_amount,
    cast(coalesce(actuals.gross_profit_amount, 0) as decimal(14, 4)) as gross_profit_amount,
    budgets.budget_revenue_amount
from actuals
full outer join budgets on actuals.month = budgets.month and actuals.store_id = budgets.store_id
left join {{ ref('dim_stores') }} as stores
    on coalesce(actuals.store_id, budgets.store_id) = stores.store_id
