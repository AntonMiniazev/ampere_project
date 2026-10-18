{% macro ampere_raw_dataset_predicate(alias='') -%}
  {%- set dataset = env_var('ICEBERG_DATASET_ID') -%}
  {{ alias }}_dataset_id = '{{ dataset | replace("'", "''") }}'
{%- endmacro %}

{% macro ampere_raw_delta_predicate(alias='') -%}
  {{ ampere_raw_dataset_predicate(alias) }}
  and {{ alias }}_batch_sequence > {{ env_var('SILVER_DELTA_FROM_SEQUENCE', '0') | int }}
  and {{ alias }}_batch_sequence <= {{ env_var('SILVER_DELTA_TO_SEQUENCE', '0') | int }}
{%- endmacro %}
