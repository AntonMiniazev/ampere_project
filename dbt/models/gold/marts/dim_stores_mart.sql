{{ config(alias='dim_stores', tags=['dimension', 'publish']) }}

select
    s.store_id,
    s.city,
    s.store_name,
    z.zone_name,
    {{ ampere_gold_lineage_columns() }}
from {{ ampere_gold_silver_relation('dim_stores') }} as s
left join {{ ampere_gold_silver_relation('dim_zones') }} as z
    on s.store_id = z.store_id
