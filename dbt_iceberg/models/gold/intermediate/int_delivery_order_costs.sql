{{ config(database='ampere_work', schema='gold_intermediate', materialized='table', tags=['gold', 'intermediate']) }}

with last_events as (
    select tracking.order_id, tracking.courier_id, tracking.status_datetime
    from {{ ref('fact_delivery_tracking') }} as tracking
    join {{ ref('fact_orders') }} as orders on tracking.order_id = orders.order_id
    where tracking.delivery_status_id = 2
      and orders.latest_order_status_id = 3
      and {{ ampere_gold_month_predicate("date_trunc('month', orders.order_date)") }}
    qualify dense_rank() over (partition by tracking.order_id order by tracking.status_datetime desc) = 1
),
assignments as (
    select order_id, min(courier_id) as courier_id, count(distinct courier_id) as assignment_count
    from last_events group by order_id
),
resources as (
    select delivery_resource_id, count(*) as resource_count,
           case when count(*) = 1 then min(delivery_type_id) end as delivery_type_id
    from {{ ref('dim_delivery_resource') }} group by delivery_resource_id
),
tariffs as (
    select zone_id, delivery_type_id, count(*) as tariff_count,
           case when count(*) = 1 then sum(tariff) end as tariff
    from {{ ref('dim_delivery_costing') }} group by zone_id, delivery_type_id
),
store_tariffs as (
    -- Resolve the amount without assigning an arbitrary zone to the order.
    -- Every covered zone must have exactly one non-null, identical tariff.
    select zones.store_id, types.delivery_type_id,
        count(*) filter (where tariffs.tariff_count > 1) as duplicate_zone_tariffs,
        count(*) filter (where tariffs.tariff_count is null or tariffs.tariff is null) as missing_zone_tariffs,
        count(distinct tariffs.tariff) as distinct_tariffs,
        case when count(distinct tariffs.tariff) = 1 then min(tariffs.tariff) end as tariff
    from {{ ref('dim_zones') }} as zones
    cross join (select distinct delivery_type_id from resources) as types
    left join tariffs on zones.zone_id = tariffs.zone_id
      and types.delivery_type_id = tariffs.delivery_type_id
    group by zones.store_id, types.delivery_type_id
),
resolved as (
    select orders.order_id, orders.order_date, orders.store_id, assignments.courier_id,
        case
            when assignments.assignment_count is null or assignments.assignment_count = 0 then 'missing_delivery'
            when assignments.assignment_count <> 1 then 'ambiguous_assignment'
            when resources.resource_count is null then 'missing_resource'
            when resources.resource_count <> 1 then 'ambiguous_resource'
            when store_tariffs.store_id is null then 'missing_store_zones'
            when store_tariffs.duplicate_zone_tariffs > 0 or store_tariffs.distinct_tariffs > 1 then 'ambiguous_tariff'
            when store_tariffs.missing_zone_tariffs > 0 or store_tariffs.tariff is null then 'missing_tariff'
            else 'valid'
        end as cost_status,
        store_tariffs.tariff
    from {{ ref('fact_orders') }} as orders
    left join assignments on orders.order_id = assignments.order_id
    left join resources on assignments.courier_id = resources.delivery_resource_id
    left join store_tariffs on orders.store_id = store_tariffs.store_id
      and resources.delivery_type_id = store_tariffs.delivery_type_id
    where orders.latest_order_status_id = 3
      and orders.order_date is not null
      and {{ ampere_gold_month_predicate("date_trunc('month', orders.order_date)") }}
)
select order_id, order_date, store_id, courier_id, cost_status,
       cast(case when cost_status = 'valid' then tariff end as decimal(14, 4)) as delivery_cost_amount
from resolved
