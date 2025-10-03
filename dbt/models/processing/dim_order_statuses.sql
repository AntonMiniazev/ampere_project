{% set _load_date = var("load_date", run_started_at.strftime("%Y-%m-%d")) %}

{{ config(
    partition_by=['_load_date'],
    location='s3://ampere-prod-silver/dimension/' ~ this.identifier ~ '/load_date=' ~ _load_date ~ '/' ~ this.identifier ~ '.parquet'
) }}
-- Read latest full snapshot and standardize types /<snapshot_type=xxx>/<load_date=yyy>/<part-zzz>.parquet
with latest as (
  select * from read_parquet('s3://ampere-prod-raw/source/core/order_statuses/*/*/*.parquet')
),
clean as (
  select
    cast(id as TINYINT)                as id,
    cast(order_status as VARCHAR(50))  as order_status,
    now() as _ingested_at
  from latest
  qualify row_number() over (partition by id order by _ingested_at desc) = 1
)
select * from clean
