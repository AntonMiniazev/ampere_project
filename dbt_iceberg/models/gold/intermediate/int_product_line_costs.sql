{{ config(database='ampere_work', schema='gold_intermediate', materialized='table', tags=['gold', 'intermediate']) }}

select lines.fact_order_product_key, lines.order_id, lines.order_date, lines.store_id,
       lines.product_id, lines.line_sales_amount,
       count(costing.costing_key) as cost_match_count,
       cast(case when count(costing.costing_key) = 1 and count(costing.avg_cost) = 1
                 then sum(lines.quantity * costing.avg_cost) end as decimal(14, 4)) as product_cost_amount
from {{ ref('fact_order_product') }} as lines
left join {{ ref('dim_costing') }} as costing
  on lines.product_id = costing.product_id and lines.store_id = costing.store_id
  and lines.order_date >= costing.valid_from
  and (costing.valid_to is null or lines.order_date < costing.valid_to)
where lines.order_date is not null
  and {{ ampere_gold_month_predicate("date_trunc('month', lines.order_date)") }}
group by lines.fact_order_product_key, lines.order_id, lines.order_date, lines.store_id,
         lines.product_id, lines.line_sales_amount
