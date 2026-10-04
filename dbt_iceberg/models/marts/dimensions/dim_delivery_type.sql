{{ config(tags=['snapshot_dimension']) }}

select
    delivery_type_id,
    courier_type,
    {{ ampere_silver_lineage_columns() }}
from {{ ref('stg_delivery_type') }}
