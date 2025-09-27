{% set _load_date = var("load_date", run_started_at.strftime("%Y-%m-%d")) %}

{{ config(
    partition_by=['_load_date'],
    location='s3://ampere-prod-silver/dimension/' ~ this.identifier ~ '/load_date=' ~ _load_date ~ '/' ~ this.identifier ~ '.parquet'
) }}
-- Read latest full snapshot and standardize types /<snapshot_type=xxx>/<load_date=yyy>/<part-zzz>.parquet
with latest as (
  select * from read_parquet('s3://ampere-prod-raw/source/core/costing/*/*/*.parquet')
),
clean as (
  select
    cast(product_id as SMALLINT)      as product_id,
    cast(store_id as TINYINT)         as store_id,
    cast(avg_cost as DECIMAL(10,2))   as avg_cost,
    cast(cost_active_period as DATE)  as cost_active_period,
    cast('{{ _load_date }}' as DATE)  as _load_date,
    now() as _ingested_at
  from latest
  qualify row_number() over (partition by product_id, store_id, cost_active_period order by _ingested_at desc) = 1
)
select * from clean
