{% macro ampere_gold_month_predicate(date_expression) -%}
  {%- if env_var('GOLD_AFFECTED_MONTHS', '*') == '*' -%}
    true
  {%- else -%}
    cast({{ date_expression }} as date) in (
      select try_cast(month_value as date)
      from unnest(string_split('{{ env_var('GOLD_AFFECTED_MONTHS', '') }}', ',')) as months(month_value)
      where month_value <> ''
    )
  {%- endif -%}
{%- endmacro %}
