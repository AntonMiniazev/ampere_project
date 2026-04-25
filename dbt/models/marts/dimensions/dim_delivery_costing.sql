with latest_delivery_costing as (
    select *
    from {{ ref('stg_delivery_costing') }}
    qualify row_number() over (
        partition by zone_id, delivery_type_id
        order by
            try_cast(_bronze_last_apply_ts as timestamp) desc nulls last,
            _bronze_last_run_id desc nulls last
    ) = 1
)
select
    concat_ws('|', cast(dc.zone_id as varchar), cast(dc.delivery_type_id as varchar)) as delivery_costing_key,
    dc.zone_id,
    z.zone_name,
    dc.delivery_type_id,
    dt.courier_type,
    dc.tariff,
    dc._bronze_last_run_id,
    dc._bronze_last_apply_ts,
    dc._bronze_last_manifest_path,
    {{ ampere_silver_lineage_columns() }}
from latest_delivery_costing as dc
left join {{ ref('dim_zones') }} as z
    on dc.zone_id = z.zone_id
left join {{ ref('dim_delivery_type') }} as dt
    on dc.delivery_type_id = dt.delivery_type_id
