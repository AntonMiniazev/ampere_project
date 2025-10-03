{% set _load_date = var("load_date", run_started_at.strftime("%Y-%m-%d")) %}

{{ config(
    partition_by=['_load_date'],
    location='s3://ampere-prod-silver/dimension/' ~ this.identifier ~ '/load_date=' ~ _load_date ~ '/'~ this.identifier ~'.parquet',
) }}
-- Read latest full snapshot and standardize types /<snapshot_type=xxx>/<load_date=yyy>/<part-zzz>.parquet
with latest as (
  {{ latest_full('s3://ampere-prod-raw/source/core/clients/*/*/*.parquet') }}
),


clean as (
  select
    cast(id as INT)                         as id,
    cast(fullname as VARCHAR(255))          as fullname,
    cast(preferred_store_id  as TINYINT)    as preferred_store_id,    
    cast(registration_date  as DATE)        as registration_date,
    now()                                   as _ingested_at,
  from latest
  qualify row_number() over (partition by id order by _ingested_at desc) = 1
)

select * from clean
