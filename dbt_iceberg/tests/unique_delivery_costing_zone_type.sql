select zone_id, delivery_type_id
from {{ ref('stg_delivery_costing') }}
group by zone_id, delivery_type_id
having count(*) <> 1
