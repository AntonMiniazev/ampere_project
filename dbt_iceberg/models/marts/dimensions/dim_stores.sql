{{ config(tags=['snapshot_dimension']) }}

select
    store_id,
    city,
    store_name,
    {{ ampere_silver_lineage_columns() }}
from {{ ref('stg_stores') }}
