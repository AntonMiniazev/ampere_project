{% set _load_date = var("load_date", run_started_at.strftime("%Y-%m-%d")) %}

{{ config(
    partition_by=['_load_date'],
    location='s3://ampere-prod-silver/dimension/' ~ this.identifier ~ '/load_date=' ~ _load_date ~ '/' ~ this.identifier ~ '.parquet'
) }}
-- Read latest full snapshot and standardize types /<snapshot_type=xxx>/<load_date=yyy>/<part-zzz>.parquet
with latest as (
  select * from read_parquet('s3://ampere-prod-raw/source/core/order_product/*/*/*.parquet')
),
clean as (
  select
    cast(order_id as INT)          as order_id,
    cast(product_id as INT)        as product_id,
    cast(quantity as DECIMAL(6,2)) as quantity,
    cast('{{ _load_date }}' as DATE) as _load_date,
    now() as _ingested_at
  from latest
  qualify row_number() over (partition by order_id, product_id order by _ingested_at desc) = 1
)
select * from clean
