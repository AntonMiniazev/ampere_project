{{ config(tags=['gold'], severity='warn') }}
-- Retain unknown historical costs as NULL and report the affected orders.
select cost_status, count(*) as affected_orders
from {{ ref('int_delivery_order_costs') }}
where cost_status <> 'valid'
group by cost_status
