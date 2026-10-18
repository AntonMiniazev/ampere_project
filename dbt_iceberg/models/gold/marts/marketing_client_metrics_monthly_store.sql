{{ config(alias='marketing_client_metrics_monthly_store', tags=['gold', 'publish']) }}

with client_history as (
    select * from {{ ref('dim_client_preference_history') }}
),
months as (
    select distinct cast(date_trunc('month', order_date) as date) as month
    from {{ ref('fact_orders') }}
    where order_date is not null
    union
    select distinct cast(date_trunc('month', registration_date) as date)
    from client_history
    where registration_date is not null
    union
    select distinct cast(date_trunc('month', updated_at) as date)
    from client_history
    where is_churned = true and updated_at is not null
),
store_months as (
    select months.month, stores.store_id, stores.store_name, stores.city
    from months
    cross join {{ ref('dim_stores') }} as stores
    where {{ ampere_gold_month_predicate('months.month') }}
),
client_state_by_month as (
    select
        store_months.month,
        history.client_id,
        history.preferred_store_id,
        history.registration_date,
        history.updated_at,
        history.is_churned,
        history.is_deleted
    from (select distinct month from store_months) as store_months
    join client_history as history
      on history.effective_from <= cast(last_day(store_months.month) as date)
    qualify row_number() over (
        partition by store_months.month, history.client_id
        order by history.effective_from desc,
                 history.source_batch_sequence desc,
                 history.source_change_id desc
    ) = 1
),
history_start as (
    select min(effective_from) as first_available_date from client_history
),
monthly_orders as (
    select
        cast(date_trunc('month', order_date) as date) as month,
        store_id,
        count(distinct client_id)::bigint as active_client_count
    from {{ ref('fact_orders') }}
    where latest_order_status_id = 3
      and order_date is not null
      and client_id is not null
      and {{ ampere_gold_month_predicate("date_trunc('month', order_date)") }}
    group by 1, 2
),
preferred_store_metrics as (
    select
        client_state_by_month.month,
        client_state_by_month.preferred_store_id as store_id,
        count(distinct case
            when client_state_by_month.registration_date is not null
             and cast(date_trunc('month', client_state_by_month.registration_date) as date)
                 = client_state_by_month.month
            then client_state_by_month.client_id
        end)::bigint as new_client_count,
        count(distinct case
            when client_state_by_month.registration_date < client_state_by_month.month
             and (client_state_by_month.is_churned is distinct from true
                  or client_state_by_month.updated_at >= client_state_by_month.month)
            then client_state_by_month.client_id
        end)::bigint as client_base_start,
        count(distinct case
            when client_state_by_month.is_churned = true
             and client_state_by_month.updated_at is not null
             and cast(date_trunc('month', client_state_by_month.updated_at) as date)
                 = client_state_by_month.month
            then client_state_by_month.client_id
        end)::bigint as churned_clients
    from client_state_by_month
    where client_state_by_month.is_deleted is distinct from true
      and client_state_by_month.preferred_store_id is not null
    group by 1, 2
)
select
    store_months.month,
    store_months.store_id,
    coalesce(store_months.store_name, 'Unknown') as store_name,
    store_months.city,
    coalesce(monthly_orders.active_client_count, 0)::bigint as active_client_count,
    case when store_months.month < history_start.first_available_date then null
         else coalesce(preferred_store_metrics.new_client_count, 0)::bigint end as new_client_count,
    case when store_months.month < history_start.first_available_date then null
         else coalesce(preferred_store_metrics.client_base_start, 0)::bigint end as client_base_start,
    case when store_months.month < history_start.first_available_date then null
         else coalesce(preferred_store_metrics.churned_clients, 0)::bigint end as churned_clients
from store_months
cross join history_start
left join monthly_orders
    on store_months.month = monthly_orders.month
   and store_months.store_id = monthly_orders.store_id
left join preferred_store_metrics
    on store_months.month = preferred_store_metrics.month
   and store_months.store_id = preferred_store_metrics.store_id
