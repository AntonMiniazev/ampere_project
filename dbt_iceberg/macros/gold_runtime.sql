{% macro ampere_gold_lineage_columns(model_name=none) -%}
  cast('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as timestamp) as _gold_build_ts,
  '{{ model_name or this.name }}' as _gold_model_name,
  '{{ invocation_id }}' as _gold_run_id
{%- endmacro %}

{% macro ampere_gold_run_mode() -%}
  {{ return(var('gold_run_mode', env_var('GOLD_RUN_MODE', env_var('SILVER_RUN_MODE', 'daily_refresh')))) }}
{%- endmacro %}

{% macro ampere_gold_logical_date() -%}
  {{ return(var('gold_logical_date', env_var('LOGICAL_DATE', run_started_at.strftime('%Y-%m-%d')))) }}
{%- endmacro %}

{% macro ampere_gold_month_window_predicate(date_expression) -%}
  {%- if ampere_gold_run_mode() == 'daily_refresh' -%}
    cast(date_trunc('month', {{ date_expression }}) as date) >=
      cast(date_trunc('month', cast('{{ ampere_gold_logical_date() }}' as date)) as date)
      - interval '1 month'
  {%- elif ampere_gold_run_mode() == 'full_history' -%}
    true
  {%- else -%}
    {{ exceptions.raise_compiler_error('Unsupported Iceberg Gold run mode: ' ~ ampere_gold_run_mode()) }}
  {%- endif -%}
{%- endmacro %}

{% macro ampere_gold_silver_relation(table_name, ref_name=none) -%}
  {% if table_name == 'budget_orders_sales' %}
    {{ return(ref('silver_budget_orders_sales')) }}
  {% else %}
    {{ return(ref(ref_name or table_name)) }}
  {% endif %}
{%- endmacro %}
