{% set full_history_batches = ampere_order_status_history_batch_count() | int %}
{{ config(materialized='ampere_batched_table') }}

with source_data as (
    select *
    from {{ ref('stg_order_status_history_source') }}
    {% if ampere_silver_run_mode() == 'full_history' %}
    where coalesce(
        hash(cast(order_id as integer)) % {{ full_history_batches }},
        0
    ) = cast({{ ampere_order_status_history_batch_placeholder() }} as integer)
    {% endif %}
),
deduped as (
    select
        cast(order_id as integer) as order_id,
        cast(order_status_id as integer) as order_status_id,
        cast(status_datetime as timestamp) as status_datetime,
        {{ ampere_bronze_lineage_columns() }}
    from source_data
    qualify row_number() over (
        partition by order_id, status_datetime
        order by _bronze_last_apply_ts desc
    ) = 1
)
select
    *,
    {{ ampere_silver_lineage_columns() }}
from deduped
