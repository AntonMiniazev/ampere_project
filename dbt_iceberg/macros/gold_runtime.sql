{% macro ampere_gold_lineage_columns(model_name=none) -%}
  cast('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as timestamp) as _gold_build_ts,
  '{{ model_name or this.name }}' as _gold_model_name,
  '{{ invocation_id }}' as _gold_run_id
{%- endmacro %}

{% macro ampere_gold_silver_relation(table_name, ref_name=none) -%}
  {% if table_name == 'budget_orders_sales' %}
    {{ return(ref('silver_budget_orders_sales')) }}
  {% else %}
    {{ return(ref(ref_name or table_name)) }}
  {% endif %}
{%- endmacro %}
