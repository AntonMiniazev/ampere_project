{% materialization ampere_batched_table, adapter='duckdb' %}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing_relation = load_cached_relation(target_relation) -%}
  {%- set run_mode = var('silver_run_mode', env_var('SILVER_RUN_MODE', 'daily_refresh')) -%}
  {%- set batch_count = ampere_order_status_history_batch_count() | int -%}

  {%- if run_mode not in ['daily_refresh', 'full_history'] -%}
    {{ exceptions.raise_compiler_error('Unsupported Iceberg Silver run mode: ' ~ run_mode) }}
  {%- endif -%}
  {{ run_hooks(pre_hooks) }}
  {% do adapter.create_schema(target_relation) %}

  {%- if existing_relation is not none -%}
    {% do adapter.drop_relation(existing_relation) %}
  {%- endif -%}

  {%- if run_mode == 'full_history' -%}
    {%- for batch_index in range(batch_count) -%}
      {%- set batch_sql = sql | replace("'__AMPERE_ORDER_STATUS_BATCH__'", batch_index | string) -%}
      {%- if batch_sql == sql -%}
        {{ exceptions.raise_compiler_error('Full-history status model is missing its batch placeholder') }}
      {%- endif -%}
      {{ log('Building ' ~ this ~ ' full-history batch ' ~ (batch_index + 1) ~ '/' ~ batch_count, info=true) }}
      {% call statement('main', fetch_result=false) %}
        {% if loop.first %}
          CREATE TABLE {{ target_relation }} AS ({{ batch_sql }})
        {% else %}
          INSERT INTO {{ target_relation }} ({{ batch_sql }})
        {% endif %}
      {% endcall %}
    {%- endfor -%}
  {%- else -%}
    {% call statement('main', fetch_result=false) %}
      CREATE TABLE {{ target_relation }} AS ({{ sql }})
    {% endcall %}
  {%- endif -%}

  {{ run_hooks(post_hooks) }}
  {% do adapter.commit() %}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
