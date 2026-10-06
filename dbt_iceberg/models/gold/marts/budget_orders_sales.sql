{{ config(alias='budget_orders_sales', tags=['budget', 'publish']) }}

select
    budget_name,
    budget_date,
    store_id,
    orders_budget_daily,
    sales_amount_daily,
    {{ ampere_gold_lineage_columns() }}
from {{ ampere_gold_silver_relation('budget_orders_sales') }}
