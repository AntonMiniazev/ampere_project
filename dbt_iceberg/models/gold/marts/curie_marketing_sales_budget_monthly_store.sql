{{ config(alias='curie_marketing_sales_budget_monthly_store', tags=['gold', 'publish']) }}

with actual_sales as (
    select
        cast(date_trunc('month', order_date) as date) as month,
        store_id,
        cast(sum(total_amount) as decimal(14, 4)) as sales_amount,
        count(distinct order_id)::bigint as order_count
    from {{ ref('fact_orders') }}
    where latest_order_status_id = 3
      and order_date is not null
      and {{ ampere_gold_month_window_predicate('order_date') }}
    group by 1, 2
),

monthly_budget as (
    select
        cast(date_trunc('month', budget_date) as date) as month,
        store_id,
        cast(sum(sales_amount_daily) as decimal(14, 4)) as budget_sales_amount,
        cast(sum(orders_budget_daily) as decimal(14, 4)) as budget_order_count
    from {{ ref('silver_budget_orders_sales') }}
    where budget_date is not null
      and {{ ampere_gold_month_window_predicate('budget_date') }}
    group by 1, 2
),

store_labels as (
    select store_id, store_name, city
    from {{ ref('dim_stores') }}
)

select
    coalesce(actual_sales.month, monthly_budget.month) as month,
    coalesce(actual_sales.store_id, monthly_budget.store_id) as store_id,
    coalesce(store_labels.store_name, 'Unknown') as store_name,
    store_labels.city,
    cast(coalesce(actual_sales.sales_amount, 0) as decimal(14, 4)) as sales_amount,
    coalesce(actual_sales.order_count, 0)::bigint as order_count,
    monthly_budget.budget_sales_amount,
    monthly_budget.budget_order_count
from actual_sales
full outer join monthly_budget
    on actual_sales.month = monthly_budget.month
   and actual_sales.store_id = monthly_budget.store_id
left join store_labels
    on coalesce(actual_sales.store_id, monthly_budget.store_id) = store_labels.store_id
