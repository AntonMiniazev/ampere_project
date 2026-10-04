{{ config(tags=['snapshot_dimension']) }}

select
    order_status_id,
    order_status,
    {{ ampere_silver_lineage_columns() }}
from {{ ref('stg_order_statuses') }}
