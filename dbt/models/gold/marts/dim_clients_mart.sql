{{ config(alias='dim_clients', tags=['dimension', 'publish']) }}

select
    client_id,
    client_full_name as fullname,
    preferred_store_id,
    registration_date,
    {{ ampere_gold_lineage_columns() }}
from {{ ampere_gold_silver_relation('dim_clients') }}
