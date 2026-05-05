{{ config(alias='dim_resource', tags=['dimension', 'publish']) }}

select
    delivery_resource_id as courier_id,
    delivery_resource_name as fullname,
    courier_type,
    {{ ampere_gold_lineage_columns() }}
from {{ ampere_gold_silver_relation('dim_delivery_resource') }}
