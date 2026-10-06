{{ config(alias='dim_delivery_cost', tags=['helper', 'publish']) }}

select
    dt.order_id,
    max(dc.tariff) as tariff,
    {{ ampere_gold_lineage_columns() }}
from {{ ampere_gold_silver_relation('fact_delivery_tracking') }} as dt
left join {{ ampere_gold_silver_relation('dim_delivery_resource') }} as dr
    on dt.courier_id = dr.delivery_resource_id
left join {{ ampere_gold_silver_relation('dim_zones') }} as z
    on dr.store_id = z.store_id
left join {{ ampere_gold_silver_relation('dim_delivery_costing') }} as dc
    on z.zone_id = dc.zone_id
    and dr.delivery_type_id = dc.delivery_type_id
where dt.delivery_status_id = 2
group by dt.order_id
