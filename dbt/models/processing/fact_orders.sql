{% set _load_date = var("load_date", run_started_at.strftime("%Y-%m-%d")) %}

{{ config(
    partition_by=['_load_date'],
    location='s3://ampere-prod-silver/fact/' ~ this.identifier ~ '/load_date=' ~ _load_date ~ '/'~ this.identifier ~'.parquet',
) }}
-- Read latest full snapshot and standardize types /<snapshot_type=xxx>/<load_date=yyy>/<part-zzz>.parquet
with latest as (
  {{ latest_full('s3://ampere-prod-raw/source/core/orders/*/*/*.parquet') }}
),


clean as (
  select
    cast(id as INT)                        as id,
    cast(client_id as INT)                 as client_id,
    cast(order_date as DATE)               as order_date,    
    cast(order_source_id as TINYINT)       as order_source_id,
    cast(total_amount as DECIMAL(12,4))    as total_amount,
    now()                                  as _ingested_at,
  from latest
  qualify row_number() over (partition by id order by _ingested_at desc) = 1
)

select * from clean
