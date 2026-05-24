{% macro ampere_escape_sql_literal(value) -%}
  {{ return((value | string).replace("'", "''")) }}
{%- endmacro %}


{% macro ampere_quote_identifier(value) -%}
  {{ return('"' ~ (value | string).replace('"', '""') ~ '"') }}
{%- endmacro %}


{% macro ampere_configure_minio_access() -%}
  {% if not execute %}
    {{ return('') }}
  {% endif %}

  {% set endpoint_raw = env_var('MINIO_S3_ENDPOINT', 'minio.ampere.svc.cluster.local:9000') | trim %}
  {% set use_ssl = (env_var('MINIO_S3_USE_SSL', 'false') | lower) in ['1', 'true', 'yes', 'y', 'on'] %}
  {% if endpoint_raw.startswith('https://') %}
    {% set endpoint = endpoint_raw[8:] %}
    {% set use_ssl = true %}
  {% elif endpoint_raw.startswith('http://') %}
    {% set endpoint = endpoint_raw[7:] %}
    {% set use_ssl = false %}
  {% else %}
    {% set endpoint = endpoint_raw %}
  {% endif %}
  {% set region = env_var('MINIO_S3_REGION', 'us-east-1') | trim %}
  {% if not region %}
    {% set region = 'us-east-1' %}
  {% endif %}
  {% set access_key = env_var('MINIO_ACCESS_KEY', '') | trim %}
  {% set secret_key = env_var('MINIO_SECRET_KEY', '') | trim %}

  {% do run_query("set s3_region='" ~ ampere_escape_sql_literal(region) ~ "'") %}
  {% do run_query("set s3_url_style='path'") %}
  {% do run_query("set s3_endpoint='" ~ ampere_escape_sql_literal(endpoint) ~ "'") %}
  {% do run_query("set s3_use_ssl=" ~ ('true' if use_ssl else 'false')) %}
  {% if access_key %}
    {% do run_query("set s3_access_key_id='" ~ ampere_escape_sql_literal(access_key) ~ "'") %}
  {% endif %}
  {% if secret_key %}
    {% do run_query("set s3_secret_access_key='" ~ ampere_escape_sql_literal(secret_key) ~ "'") %}
  {% endif %}

  {% if access_key and secret_key %}
    {% set create_secret_sql %}
      create or replace secret ampere_minio_s3 (
        type s3,
        provider config,
        key_id '{{ ampere_escape_sql_literal(access_key) }}',
        secret '{{ ampere_escape_sql_literal(secret_key) }}',
        region '{{ ampere_escape_sql_literal(region) }}',
        endpoint '{{ ampere_escape_sql_literal(endpoint) }}',
        use_ssl {{ 'true' if use_ssl else 'false' }},
        url_style 'path'
      )
    {% endset %}
    {% do run_query(create_secret_sql) %}
  {% endif %}

  {% do log(
    'Configured MinIO access for DuckDB: endpoint=' ~ endpoint
    ~ ' use_ssl=' ~ ('true' if use_ssl else 'false')
    ~ ' access_key_set=' ~ ('true' if access_key else 'false')
    ~ ' secret_key_set=' ~ ('true' if secret_key else 'false'),
    info=True
  ) %}
  {{ return('') }}
{%- endmacro %}


{% macro ampere_required_source_tables(source_name='bronze') -%}
  {% set required = [] %}
  {% for source_node in graph.sources.values() %}
    {% if source_node.package_name == project_name and source_node.source_name == source_name %}
      {% do required.append(source_node.name) %}
    {% endif %}
  {% endfor %}
  {{ return(required | sort) }}
{%- endmacro %}


{% macro ampere_prepare_mapped_sources(source_name, source_schema, mapping_path, label='source') -%}
  {% if not execute %}
    {{ return('') }}
  {% endif %}
  {% do ampere_configure_minio_access() %}
  {% set required_tables = ampere_required_source_tables(source_name) %}
  {% set escaped_mapping_path = ampere_escape_sql_literal(mapping_path) %}
  {% set escaped_source_name = ampere_escape_sql_literal(source_name) %}

  {% do run_query('create schema if not exists ' ~ ampere_quote_identifier(source_schema)) %}

  {% set mapping_sql %}
    select table_name, storage_location
    from read_json_auto('{{ escaped_mapping_path }}')
    where source_name = '{{ escaped_source_name }}'
    order by table_name
  {% endset %}
  {% set mapping_result = run_query(mapping_sql) %}

  {% if mapping_result is none %}
    {% do exceptions.raise_compiler_error(
      'Unable to read UC source mapping from ' ~ mapping_path
    ) %}
  {% endif %}

  {% set mapped_tables = [] %}
  {% for row in mapping_result.rows %}
    {% set table_name = (row[0] | string).strip() %}
    {% set storage_location = (row[1] | string).strip() %}
    {% if table_name and storage_location %}
      {% do mapped_tables.append(table_name) %}
      {% set escaped_storage_location = ampere_escape_sql_literal(storage_location) %}
      {% set create_view_sql %}
        create or replace view {{ ampere_quote_identifier(source_schema) }}.{{ ampere_quote_identifier(table_name) }} as
        select *
        from delta_scan('{{ escaped_storage_location }}')
      {% endset %}
      {% do run_query(create_view_sql) %}
    {% endif %}
  {% endfor %}

  {% set missing_tables = [] %}
  {% for table_name in required_tables %}
    {% if table_name not in mapped_tables %}
      {% do missing_tables.append(table_name) %}
    {% endif %}
  {% endfor %}
  {% if missing_tables | length > 0 %}
    {% do exceptions.raise_compiler_error(
      label ~ ' UC source mapping is missing required tables: '
      ~ (missing_tables | join(', '))
      ~ '. mapping_path=' ~ mapping_path
    ) %}
  {% endif %}

  {% do log(
    'Prepared ' ~ (mapped_tables | length)
    ~ ' ' ~ label ~ ' source views from live UC metadata mapping '
    ~ mapping_path,
    info=True
  ) %}
  {{ return('') }}
{%- endmacro %}


{% macro ampere_prepare_bronze_sources() -%}
  {% if env_var('RUN_BRONZE_SOURCE_PREPARE', 'true') != 'true' %}
    {{ return('') }}
  {% endif %}
  {% set source_name = env_var('BRONZE_SOURCE_NAME', var('bronze_source_name', 'bronze')) %}
  {% set source_schema = env_var('BRONZE_SOURCE_SCHEMA', var('bronze_source_schema', 'bronze')) %}
  {% set mapping_path = env_var(
    'BRONZE_SOURCE_MAPPING_PATH',
    var('bronze_source_mapping_path', 'dbt/.dbt_local/bronze_source_mapping.json')
  ) %}
  {{ ampere_prepare_mapped_sources(source_name, source_schema, mapping_path, 'Bronze') }}
{%- endmacro %}


{% macro ampere_prepare_silver_sources() -%}
  {% set source_name = env_var('SILVER_SOURCE_NAME', 'silver') %}
  {% set source_schema = env_var('SILVER_SOURCE_SCHEMA', 'silver') %}
  {% set mapping_path = env_var('SILVER_SOURCE_MAPPING_PATH', 'dbt/.dbt_local/silver_source_mapping.json') %}
  {{ ampere_prepare_mapped_sources(source_name, source_schema, mapping_path, 'Silver') }}
{%- endmacro %}
