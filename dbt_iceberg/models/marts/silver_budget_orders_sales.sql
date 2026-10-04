{{ config(alias='budget_orders_sales', tags=['budget', 'publish']) }}

select
    cast(budget_name as varchar) as budget_name,
    cast(budget_date as date) as budget_date,
    cast(store_id as smallint) as store_id,
    cast(orders_budget_daily as decimal(12, 4)) as orders_budget_daily,
    cast(sales_amount_daily as decimal(12, 4)) as sales_amount_daily
from read_csv('{{ env_var("BUDGET_DAILY_CSV_PATH", "/app/budget_parameters_daily.csv") }}', header = true)
where nullif(trim(budget_name), '') is not null
