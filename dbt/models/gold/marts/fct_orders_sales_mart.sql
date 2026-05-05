{{ config(alias='fct_orders_sales', tags=['fact', 'publish']) }}

select
    order_id,
    client_id,
    order_date,
    order_source_id,
    total_amount,
    {{ ampere_gold_lineage_columns() }}
from {{ ampere_gold_silver_relation('fact_orders') }}
where latest_order_status_id = 3
  and {{ ampere_gold_date_window_predicate('order_date') }}
