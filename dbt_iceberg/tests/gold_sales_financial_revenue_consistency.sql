{{ config(tags=['gold']) }}

select
    coalesce(marketing.month, financial.month) as month,
    coalesce(marketing.store_id, financial.store_id) as store_id,
    marketing.sales_amount,
    financial.revenue_amount
from {{ ref('curie_marketing_sales_budget_monthly_store') }} as marketing
full outer join {{ ref('curie_financial_performance_monthly_store') }} as financial
    on marketing.month = financial.month
   and marketing.store_id = financial.store_id
where coalesce(marketing.sales_amount, 0) <> coalesce(financial.revenue_amount, 0)
