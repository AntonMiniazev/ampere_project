{% set _load_date = var("load_date", run_started_at.strftime("%Y-%m-%d")) %}

{{ config(
    materialized='external',
    format='parquet',
    location='s3://ampere-prod-silver/{{ this.name }}/core_orders_basic',
    partition_by=['load_date'],
    tags=['test-silver']
) }}

-- Aggregate by client for the given partition
WITH src AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
    WHERE load_date = '{{ _load_date }}'
),
agg AS (
    SELECT
        client_id,
        SUM(order_amount) AS total_amount,
        MAX(load_date)    AS load_date
    FROM src
    GROUP BY client_id
)
SELECT * FROM agg
