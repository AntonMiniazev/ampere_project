select
    delivery_type_id,
    courier_type,
    {{ ampere_bronze_lineage_columns() }},
    {{ ampere_silver_lineage_columns() }}
from {{ ref('stg_delivery_type') }}
