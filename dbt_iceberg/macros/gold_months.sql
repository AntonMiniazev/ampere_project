{% macro ampere_gold_month_predicate(date_expression) -%}
  {%- set mode = var('gold_run_mode', env_var('GOLD_RUN_MODE', env_var('ICEBERG_RUN_MODE', 'daily_refresh'))) -%}
  {%- if mode == 'full_history' -%}
    true
  {%- elif mode != 'daily_refresh' or env_var('GOLD_AFFECTED_MONTHS', '') == '*' -%}
    {{ exceptions.raise_compiler_error('Gold daily requires an explicit affected-month set; use full_history for all months') }}
  {%- else -%}
    cast({{ date_expression }} as date) in (
      select try_cast(month_value as date)
      from unnest(string_split('{{ env_var('GOLD_AFFECTED_MONTHS', '') }}', ',')) as months(month_value)
      where month_value <> ''
    )
  {%- endif -%}
{%- endmacro %}
