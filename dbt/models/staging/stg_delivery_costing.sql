with source_data as (
    select * from {{ source('bronze', 'delivery_costing') }}
)
select
    cast(zone_id as smallint) as zone_id,
    cast(delivery_type_id as smallint) as delivery_type_id,
    cast(tariff as decimal(10, 2)) as tariff,
    {{ ampere_bronze_lineage_columns() }},
    {{ ampere_silver_lineage_columns() }}
from source_data
