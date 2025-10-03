{% set _load_date = var("load_date", run_started_at.strftime("%Y-%m-%d")) %}

{{ config(
    partition_by=['_load_date'],
    location='s3://ampere-prod-silver/dimension/' ~ this.identifier ~ '/load_date=' ~ _load_date ~ '/'~ this.identifier ~'.parquet',
) }}
-- Read latest full snapshot and standardize types /<snapshot_type=xxx>/<load_date=yyy>/<part-zzz>.parquet
with latest as (
  {{ latest_full('s3://ampere-prod-raw/source/core/payments/*/*/*.parquet') }}
),


clean as (
  select
    cast(order_id as INT)               as order_id,
    cast(amount as DECIMAL(12,4))       as amount,
    cast(method as VARCHAR(50))         as method,    
    cast(payment_status as VARCHAR(50)) as payment_status,
    cast(payment_date as DATE)          as payment_date,
    now()                               as _ingested_at,
  from latest
  qualify row_number() over (partition by order_id order by _ingested_at desc) = 1
)

select * from clean
