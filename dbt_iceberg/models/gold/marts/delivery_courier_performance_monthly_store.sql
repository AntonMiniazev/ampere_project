{{ config(alias='delivery_courier_performance_monthly_store', tags=['gold', 'publish']) }}

with delivered_orders as (
    select
        tracking.order_id,
        tracking.courier_id,
        orders.order_date,
        orders.store_id,
        orders.store_name,
        row_number() over (
            partition by tracking.order_id
            order by tracking.status_datetime desc, tracking.courier_id asc
        ) as assignment_rank
    from {{ ref('fact_delivery_tracking') }} as tracking
    inner join {{ ref('fact_orders') }} as orders on tracking.order_id = orders.order_id
    where tracking.delivery_status_id = 2
      and tracking.courier_id is not null
      and orders.latest_order_status_id = 3
      and orders.order_date is not null
    qualify assignment_rank = 1
),
order_tariffs as (
    select delivered_orders.order_id, max(costing.tariff) as delivery_cost_amount
    from delivered_orders
    left join {{ ref('dim_delivery_resource') }} as resource
        on delivered_orders.courier_id = resource.delivery_resource_id
    left join {{ ref('dim_zones') }} as zones on delivered_orders.store_id = zones.store_id
    left join {{ ref('dim_delivery_costing') }} as costing
        on zones.zone_id = costing.zone_id and resource.delivery_type_id = costing.delivery_type_id
    group by delivered_orders.order_id
)
select
    cast(date_trunc('month', delivered_orders.order_date) as date) as month,
    delivered_orders.store_id,
    coalesce(delivered_orders.store_name, 'Unknown') as store_name,
    stores.city,
    delivered_orders.courier_id,
    resource.delivery_resource_name as fullname,
    coalesce(resource.courier_type, 'Unknown') as courier_type,
    count(distinct delivered_orders.order_id)::bigint as delivered_order_count,
    cast(coalesce(sum(order_tariffs.delivery_cost_amount), 0) as decimal(14, 4)) as delivery_cost_amount
from delivered_orders
left join order_tariffs on delivered_orders.order_id = order_tariffs.order_id
left join {{ ref('dim_delivery_resource') }} as resource
    on delivered_orders.courier_id = resource.delivery_resource_id
left join {{ ref('dim_stores') }} as stores on delivered_orders.store_id = stores.store_id
group by 1, 2, 3, 4, 5, 6, 7
