{% if ampere_silver_run_mode() == 'full_history' %}
    {{ config(materialized='table') }}
{% else %}
    {{ config(materialized='view') }}
{% endif %}

select
    order_id,
    order_status_id,
    status_datetime,
    {{ ampere_bronze_lineage_columns() }}
from {{ source('bronze', 'order_status_history') }}
where {{ ampere_silver_related_order_predicate('order_id') }}
