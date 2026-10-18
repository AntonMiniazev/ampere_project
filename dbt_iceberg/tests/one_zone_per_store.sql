select stores.store_id
from {{ ref('dim_stores') }} as stores
left join {{ ref('stg_zones') }} as zones on zones.store_id = stores.store_id
group by stores.store_id
having count(zones.zone_id) <> 1
