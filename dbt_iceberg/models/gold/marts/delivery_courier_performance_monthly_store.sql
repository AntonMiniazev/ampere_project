{{ config(alias='delivery_courier_performance_monthly_store', tags=['gold', 'publish']) }}

select
    cast(date_trunc('month', costs.order_date) as date) as month,
    costs.store_id,
    coalesce(stores.store_name, 'Unknown') as store_name,
    stores.city,
    costs.courier_id,
    resource.delivery_resource_name as fullname,
    coalesce(resource.courier_type, 'Unknown') as courier_type,
    count(*)::bigint as delivered_order_count,
    cast(case when count(costs.delivery_cost_amount) = count(*)
              then sum(costs.delivery_cost_amount) end as decimal(14, 4)) as delivery_cost_amount
from {{ ref('int_delivery_order_costs') }} as costs
left join {{ ref('dim_delivery_resource') }} as resource
  on costs.courier_id = resource.delivery_resource_id
left join {{ ref('dim_stores') }} as stores on costs.store_id = stores.store_id
where costs.courier_id is not null
  and {{ ampere_gold_month_predicate("date_trunc('month', costs.order_date)") }}
group by 1, 2, 3, 4, 5, 6, 7
