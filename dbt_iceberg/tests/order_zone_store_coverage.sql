-- NULL zones are historical unknowns; known zones must belong to the selected store.
select orders.order_id, orders.store_id, orders.order_zone_id
from {{ ref('fact_orders') }} as orders
left join {{ ref('dim_zones') }} as zones
  on orders.order_zone_id = zones.zone_id and orders.store_id = zones.store_id
where orders.order_zone_id is not null and zones.zone_id is null
