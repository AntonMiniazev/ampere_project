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
{% if env_var('GOLD_WINDOW_START', '') and env_var('GOLD_WINDOW_END', '') %}
  and dt.order_id in (
      select order_id
      from {{ ampere_gold_silver_relation('fact_orders') }}
      where {{ ampere_gold_date_window_predicate('order_date') }}
  )
{% endif %}
group by dt.order_id
