with ranked as (
    select
        osh.order_id,
        osh.order_status_id as latest_order_status_id,
        os.order_status as latest_order_status,
        osh.status_datetime as latest_status_datetime,
        osh._bronze_last_run_id,
        osh._bronze_last_apply_ts,
        osh._bronze_last_manifest_path
    from {{ ref('stg_order_status_history') }} as osh
    left join {{ ref('stg_order_statuses') }} as os
        on osh.order_status_id = os.order_status_id
    qualify row_number() over (
        partition by osh.order_id
        order by osh.status_datetime desc, osh._bronze_last_apply_ts desc
    ) = 1
)
select
    *,
    {{ ampere_silver_lineage_columns() }}
from ranked
