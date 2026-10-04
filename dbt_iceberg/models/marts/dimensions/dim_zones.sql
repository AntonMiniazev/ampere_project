{{ config(tags=['snapshot_dimension']) }}

select
    z.zone_id,
    z.store_id,
    s.store_name,
    s.city as store_city,
    z.zone_name,
    {{ ampere_silver_lineage_columns() }}
from {{ ref('stg_zones') }} as z
left join {{ ref('dim_stores') }} as s
    on z.store_id = s.store_id
