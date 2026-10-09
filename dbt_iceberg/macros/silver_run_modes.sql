{% macro ampere_silver_run_mode() -%}
  {{ return(var('silver_run_mode', env_var('SILVER_RUN_MODE', 'daily_refresh'))) }}
{%- endmacro %}

{% macro ampere_silver_logical_date() -%}
  {{ return(var('silver_logical_date', env_var('LOGICAL_DATE', run_started_at.strftime('%Y-%m-%d')))) }}
{%- endmacro %}

{% macro ampere_silver_date_window_predicate(date_expression) -%}
  {%- if ampere_silver_run_mode() == 'daily_refresh' -%}
    cast({{ date_expression }} as date) >=
      cast(date_trunc('month', cast('{{ ampere_silver_logical_date() }}' as date)) as date)
      - interval '1 month'
  {%- elif ampere_silver_run_mode() == 'full_history' -%}
    true
  {%- else -%}
    {{ exceptions.raise_compiler_error('Unsupported Iceberg Silver run mode: ' ~ ampere_silver_run_mode()) }}
  {%- endif -%}
{%- endmacro %}

{% macro ampere_silver_related_order_predicate(order_expression) -%}
  {%- if ampere_silver_run_mode() == 'daily_refresh' -%}
    {{ order_expression }} in (select order_id from {{ ref('stg_orders') }})
  {%- elif ampere_silver_run_mode() == 'full_history' -%}
    true
  {%- else -%}
    {{ exceptions.raise_compiler_error('Unsupported Iceberg Silver run mode: ' ~ ampere_silver_run_mode()) }}
  {%- endif -%}
{%- endmacro %}
