{% macro ampere_gold_lineage_columns(model_name=none) -%}
  cast('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as timestamp) as _gold_build_ts,
  '{{ model_name or this.name }}' as _gold_model_name,
  '{{ invocation_id }}' as _gold_run_id
{%- endmacro %}

{% macro ampere_gold_run_mode() -%}
  {{ return(var('gold_run_mode', env_var('GOLD_RUN_MODE', env_var('SILVER_RUN_MODE', 'daily_refresh')))) }}
{%- endmacro %}

{% macro ampere_gold_lookback_days() -%}
  {{ return(var('gold_lookback_days', env_var('GOLD_LOOKBACK_DAYS', env_var('SILVER_LOOKBACK_DAYS', '7')))) }}
{%- endmacro %}

{% macro ampere_gold_logical_date() -%}
  {{ return(var('gold_logical_date', env_var('LOGICAL_DATE', run_started_at.strftime('%Y-%m-%d')))) }}
{%- endmacro %}

{% macro ampere_gold_date_window_predicate(date_expression) -%}
  {%- if ampere_gold_run_mode() == 'daily_refresh' -%}
    cast({{ date_expression }} as date) >=
      cast('{{ ampere_gold_logical_date() }}' as date) - interval '{{ ampere_gold_lookback_days() }} day'
  {%- else -%}
    true
  {%- endif -%}
{%- endmacro %}

{% macro ampere_gold_silver_relation(table_name, ref_name=none) -%}
  {{ return(source('silver', table_name)) }}
{%- endmacro %}
