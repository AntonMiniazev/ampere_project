{{ config(alias='marketing_client_metrics_monthly_store', tags=['gold', 'publish']) }}

with monthly_orders as (
    select
        cast(date_trunc('month', order_date) as date) as month,
        store_id,
        count(distinct client_id)::bigint as active_client_count
    from {{ ref('fact_orders') }}
    where latest_order_status_id = 3
      and order_date is not null
      and client_id is not null
    group by 1, 2
),

clients as (
    select
        client_id,
        preferred_store_id,
        registration_date,
        updated_at,
        is_churned
    from {{ ref('dim_clients') }}
),

months as (
    select distinct cast(date_trunc('month', order_date) as date) as month
    from {{ ref('fact_orders') }}
    where order_date is not null
    union
    select distinct cast(date_trunc('month', registration_date) as date)
    from clients
    where registration_date is not null
    union
    select distinct cast(date_trunc('month', updated_at) as date)
    from clients
    where is_churned = true
      and updated_at is not null
),

store_months as (
    select months.month, stores.store_id, stores.store_name, stores.city
    from months
    cross join {{ ref('dim_stores') }} as stores
),

preferred_store_metrics as (
    select
        store_months.month,
        store_months.store_id,
        count(distinct case
            when clients.registration_date is not null
             and cast(date_trunc('month', clients.registration_date) as date) = store_months.month
            then clients.client_id
        end)::bigint as new_client_count,
        count(distinct case
            when clients.registration_date < store_months.month
             and (clients.is_churned is distinct from true or clients.updated_at >= store_months.month)
            then clients.client_id
        end)::bigint as client_base_start,
        count(distinct case
            when clients.is_churned = true
             and clients.updated_at is not null
             and cast(date_trunc('month', clients.updated_at) as date) = store_months.month
            then clients.client_id
        end)::bigint as churned_clients
    from store_months
    left join clients
        on clients.preferred_store_id = store_months.store_id
    group by 1, 2
)

select
    store_months.month,
    store_months.store_id,
    coalesce(store_months.store_name, 'Unknown') as store_name,
    store_months.city,
    coalesce(monthly_orders.active_client_count, 0)::bigint as active_client_count,
    coalesce(preferred_store_metrics.new_client_count, 0)::bigint as new_client_count,
    coalesce(preferred_store_metrics.client_base_start, 0)::bigint as client_base_start,
    coalesce(preferred_store_metrics.churned_clients, 0)::bigint as churned_clients
from store_months
left join monthly_orders
    on store_months.month = monthly_orders.month
   and store_months.store_id = monthly_orders.store_id
left join preferred_store_metrics
    on store_months.month = preferred_store_metrics.month
   and store_months.store_id = preferred_store_metrics.store_id
