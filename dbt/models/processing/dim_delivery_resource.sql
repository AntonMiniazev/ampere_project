{% set _load_date = var("load_date", run_started_at.strftime("%Y-%m-%d")) %}

{{ config(
    partition_by=['_load_date'],
    location='s3://ampere-prod-silver/dimension/' ~ this.identifier ~ '/load_date=' ~ _load_date ~ '/' ~ this.identifier ~ '.parquet'
) }}
-- Read latest full snapshot and standardize types /<snapshot_type=xxx>/<load_date=yyy>/<part-zzz>.parquet
with latest as (
  select * from read_parquet('s3://ampere-prod-raw/source/core/delivery_resource/*/*/*.parquet')
),
clean as (
  select
    cast(id as SMALLINT)            as id,
    cast(fullname as VARCHAR(50))   as fullname,
    cast(delivery_type_id as TINYINT) as delivery_type_id,
    cast(store_id as TINYINT)       as store_id,
    cast(active_flag as BOOLEAN)    as active_flag,
    now() as _ingested_at
  from latest
  qualify row_number() over (partition by id order by _ingested_at desc) = 1
)
select * from clean
