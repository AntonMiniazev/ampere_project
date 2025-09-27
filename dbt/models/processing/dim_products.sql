{% set _load_date = var("load_date", run_started_at.strftime("%Y-%m-%d")) %}

{{ config(
    partition_by=['_load_date'],
    location='s3://ampere-prod-silver/dimension/' ~ this.identifier ~ '/load_date=' ~ _load_date ~ '/' ~ this.identifier ~ '.parquet'
) }}
-- Read latest full snapshot and standardize types /<snapshot_type=xxx>/<load_date=yyy>/<part-zzz>.parquet
with latest as (
  select * from read_parquet('s3://ampere-prod-raw/source/core/products/*/*/*.parquet')
),
clean as (
  select
    cast(id as INT)                  as id,
    cast(product_name as VARCHAR(255)) as product_name,
    cast(price as DECIMAL(10,2))     as price,
    cast(unit_type as VARCHAR(50))   as unit_type,
    cast(category_id as SMALLINT)    as category_id,
    cast('{{ _load_date }}' as DATE) as _load_date,
    now() as _ingested_at
  from latest
  qualify row_number() over (partition by id order by _ingested_at desc) = 1
)
select * from clean
