{{ config(alias='dim_client_preference_history', tags=['silver', 'publish']) }}

select
    cast(id as integer) as client_id,
    cast(preferred_store_id as integer) as preferred_store_id,
    cast(registration_date as date) as registration_date,
    cast(updated_at as date) as updated_at,
    cast(churned as boolean) as is_churned,
    cast(_business_date as date) as effective_from,
    cast(_batch_sequence as bigint) as source_batch_sequence,
    cast(_change_id as bigint) as source_change_id,
    cast(_op = 'D' as boolean) as is_deleted,
    {{ ampere_silver_lineage_columns() }}
from {{ source('raw', 'clients') }}
where {{ ampere_raw_dataset_predicate() }}
  and _batch_sequence <= {{ env_var('SILVER_DELTA_TO_SEQUENCE', '0') | int }}
{% if ampere_silver_run_mode() == 'daily_refresh' %}
  and _batch_sequence > {{ env_var('SILVER_DELTA_FROM_SEQUENCE', '0') | int }}
{% endif %}
