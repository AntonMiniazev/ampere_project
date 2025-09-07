{% set _load_date = var("load_date", run_started_at.strftime("%Y-%m-%d")) %}

{{ config(
    materialized='external',
    format='parquet',
    location='s3://ampere-prod-silver/{{ this.name }}/stg_orders',
    partition_by=['load_date'],
    tags=['test-silver']
) }}

-- Read raw partition once and normalize the row-level fields
WITH src AS (
    SELECT
        client_id,
        COALESCE(total_amount, 0.0) AS order_amount,
        CAST('{{ _load_date }}' AS VARCHAR) AS load_date
    FROM read_parquet(
        's3://ampere-prod-raw/source/core/orders/snapshot_type=full/load_date={{ _load_date }}/*.parquet'
    )
)
SELECT * FROM src
