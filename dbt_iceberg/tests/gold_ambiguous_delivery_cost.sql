{{ config(tags=['gold']) }}
select order_id, cost_status from {{ ref('int_delivery_order_costs') }}
where cost_status in ('ambiguous_tariff', 'ambiguous_resource', 'ambiguous_assignment', 'invalid_store_zone')
