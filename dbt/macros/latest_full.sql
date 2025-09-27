-- Returns SELECT * FROM latest full snapshot under given S3 path
{% macro latest_full(path) -%}
with src as (
  select *
  from read_parquet('{{ path }}')
  where snapshot_type = 'full'
),
latest as (
  select *
  from src
  qualify load_date = max(load_date) over ()
)
select * from latest
{%- endmacro %}
