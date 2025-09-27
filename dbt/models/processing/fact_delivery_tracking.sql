{% set _load_date = var("load_date", run_started_at.strftime("%Y-%m-%d")) %}

{{ config(
    partition_by=['_load_date'],
    location='s3://ampere-prod-silver/fact/' ~ this.identifier ~ '/load_date=' ~ _load_date ~ '/'~ this.identifier ~'.parquet',
) }}
-- Read latest full snapshot and standardize types /<snapshot_type=xxx>/<load_date=yyy>/<part-zzz>.parquet
with latest as (
  {{ latest_full('s3://ampere-prod-raw/source/core/delivery_tracking/*/*/*.parquet') }}
),


clean as (
  select
    cast(order_id as INT)                                as order_id,
    cast(courier_id as SMALLINT)                         as courier_id,
    cast(coalesce(delivery_status_id, 9) as TINYINT)     as delivery_status_id,    
    cast(coalesce(status, 'Unassigned') as VARCHAR(50))  as status,
    cast(status_datetime as datetime)                    as status_datetime,
    cast('{{ _load_date }}' as DATE)                     as _load_date,
    now()                                                as _ingested_at,
  from latest
)

select * from clean
