{{ config(alias='fct_deliveries', tags=['fact', 'publish']) }}

select
    order_id,
    courier_id,
    delivery_status_id,
    delivery_status as status,
    status_datetime,
    {{ ampere_gold_lineage_columns() }}
from {{ ampere_gold_silver_relation('fact_delivery_tracking') }}
where {{ ampere_gold_date_window_predicate('status_datetime') }}
