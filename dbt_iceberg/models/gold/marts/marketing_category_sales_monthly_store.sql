{{ config(alias='marketing_category_sales_monthly_store', tags=['gold', 'publish']) }}

select
    cast(date_trunc('month', lines.order_date) as date) as month,
    lines.store_id,
    coalesce(stores.store_name, 'Unknown') as store_name,
    stores.city,
    coalesce(products.category_name, 'Unknown') as category_name,
    cast(sum(lines.line_sales_amount) as decimal(14, 4)) as sales_amount,
    count(distinct lines.order_id)::bigint as order_count
from {{ ref('fact_order_product') }} as lines
left join {{ ref('dim_products') }} as products
    on lines.product_id = products.product_id
left join {{ ref('dim_stores') }} as stores
    on lines.store_id = stores.store_id
where lines.order_date is not null
group by 1, 2, 3, 4, 5
