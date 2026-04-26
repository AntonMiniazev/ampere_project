{{ config(tags=['snapshot_dimension']) }}

select
    store_id,
    city,
    store_name,
    {{ ampere_bronze_lineage_columns() }},
    {{ ampere_silver_lineage_columns() }}
from {{ ref('stg_stores') }}
