{{ config(alias='marketing_active_client_month', tags=['gold', 'publish']) }}

select distinct
    cast(date_trunc('month', order_date) as date) as month,
    store_id,
    client_id
from {{ ref('fact_orders') }}
where latest_order_status_id = 3
  and order_date is not null
  and client_id is not null
