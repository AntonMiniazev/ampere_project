{{ config(alias='curie_financial_product_margin_monthly_store', tags=['gold', 'publish']) }}

select
    cast(date_trunc('month', lines.order_date) as date) as month,
    lines.store_id,
    coalesce(stores.store_name, 'Unknown') as store_name,
    stores.city,
    lines.product_id,
    coalesce(products.product_name, 'Unknown') as product_name,
    products.category_name,
    cast(sum(lines.line_sales_amount) as decimal(14, 4)) as revenue_amount,
    cast(coalesce(sum(lines.quantity * costing.avg_cost), 0) as decimal(14, 4)) as product_cost_amount
from {{ ref('fact_order_product') }} as lines
left join {{ ref('dim_costing') }} as costing
    on lines.product_id = costing.product_id
   and lines.store_id = costing.store_id
   and lines.order_date >= costing.valid_from
   and (costing.valid_to is null or lines.order_date < costing.valid_to)
left join {{ ref('dim_products') }} as products on lines.product_id = products.product_id
left join {{ ref('dim_stores') }} as stores on lines.store_id = stores.store_id
where lines.order_date is not null
  and {{ ampere_gold_month_window_predicate('lines.order_date') }}
group by 1, 2, 3, 4, 5, 6, 7
