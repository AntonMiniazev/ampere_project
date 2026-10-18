{{ config(alias='financial_product_margin_monthly_store', tags=['gold', 'publish']) }}

select
    cast(date_trunc('month', lines.order_date) as date) as month,
    lines.store_id,
    coalesce(stores.store_name, 'Unknown') as store_name,
    stores.city,
    lines.product_id,
    coalesce(products.product_name, 'Unknown') as product_name,
    products.category_name,
    cast(sum(lines.line_sales_amount) as decimal(14, 4)) as revenue_amount,
    cast(case when count(lines.product_cost_amount) = count(*)
              then sum(lines.product_cost_amount) end as decimal(14, 4)) as product_cost_amount
from {{ ref('int_product_line_costs') }} as lines
left join {{ ref('dim_products') }} as products on lines.product_id = products.product_id
left join {{ ref('dim_stores') }} as stores on lines.store_id = stores.store_id
where lines.order_date is not null
  and {{ ampere_gold_month_predicate("date_trunc('month', lines.order_date)") }}
group by 1, 2, 3, 4, 5, 6, 7
