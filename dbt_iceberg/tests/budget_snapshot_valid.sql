-- Reject malformed rows instead of filtering them into a destructive empty snapshot.
select budget_name, budget_date, store_id
from {{ ref('silver_budget_orders_sales') }}
where nullif(trim(budget_name), '') is null
   or budget_date is null or store_id is null
   or orders_budget_daily is null or sales_amount_daily is null
