{{ config(tags=['mutable_dimension']) }}

select
    c.client_id,
    c.client_full_name,
    c.preferred_store_id,
    s.store_name as preferred_store_name,
    c.registration_date,
    c.updated_at,
    c.is_churned,
    {{ ampere_silver_lineage_columns() }}
from {{ ref('stg_clients') }} as c
left join {{ ref('dim_stores') }} as s
    on c.preferred_store_id = s.store_id
