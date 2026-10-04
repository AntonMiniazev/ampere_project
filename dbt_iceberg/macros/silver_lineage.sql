{% macro ampere_silver_lineage_columns(model_name=none) -%}
  cast('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as timestamp) as _silver_build_ts,
  '{{ model_name or this.name }}' as _silver_model_name,
  '{{ invocation_id }}' as _silver_run_id
{%- endmacro %}

{% macro ampere_bronze_lineage_columns() -%}
  _bronze_last_run_id,
  _bronze_last_apply_ts,
  _bronze_last_manifest_path
{%- endmacro %}
