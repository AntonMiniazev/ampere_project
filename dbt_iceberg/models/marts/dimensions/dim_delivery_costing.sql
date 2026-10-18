{{ config(tags=['snapshot_dimension']) }}

select
    concat_ws('|', cast(dc.zone_id as varchar), cast(dc.delivery_type_id as varchar)) as delivery_costing_key,
    dc.zone_id,
    z.zone_name,
    dc.delivery_type_id,
    dt.courier_type,
    dc.tariff,
    {{ ampere_silver_lineage_columns() }}
from {{ ref('stg_delivery_costing') }} as dc
left join {{ ref('dim_zones') }} as z
    on dc.zone_id = z.zone_id
left join {{ ref('dim_delivery_type') }} as dt
    on dc.delivery_type_id = dt.delivery_type_id
