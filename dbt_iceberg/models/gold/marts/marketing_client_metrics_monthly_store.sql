{{ config(alias='marketing_client_metrics_monthly_store', tags=['gold', 'publish']) }}

with daily_history as (
    -- Silver history is scoped to ICEBERG_DATASET_ID. Multiple same-day
    -- records collapse before interval expansion, never after month joins.
    select * from {{ ref('dim_client_preference_history') }}
    qualify row_number() over (
        partition by client_id, effective_from
        order by source_batch_sequence desc, source_change_id desc
    ) = 1
),
client_intervals as (
    select *, lead(effective_from) over (
        partition by client_id order by effective_from
    ) as effective_to
    from daily_history
),
months as (
    select distinct cast(date_trunc('month', order_date) as date) as month
    from {{ ref('fact_orders') }} where order_date is not null
    union
    select distinct cast(date_trunc('month', registration_date) as date)
    from daily_history where registration_date is not null
    union
    select distinct cast(date_trunc('month', updated_at) as date)
    from daily_history where is_churned = true and updated_at is not null
),
selected_months as (
    select month from months where {{ ampere_gold_month_predicate('month') }}
),
snapshots as (
    select month, month as as_of_date, 'start' as snapshot_kind from selected_months
    union all
    select month, cast(last_day(month) as date), 'end' from selected_months
),
client_states as (
    select snapshots.month, snapshots.snapshot_kind, history.*
    from snapshots
    join client_intervals as history
      on history.effective_from <= snapshots.as_of_date
     and (history.effective_to is null or snapshots.as_of_date < history.effective_to)
    where history.is_deleted is distinct from true
),
preferred_store_metrics as (
    select month, preferred_store_id as store_id,
        count(distinct case when snapshot_kind = 'start'
            and registration_date < month and is_churned is distinct from true
            then client_id end)::bigint as client_base_start,
        count(distinct case when snapshot_kind = 'end'
            and date_trunc('month', registration_date)::date = month
            then client_id end)::bigint as new_client_count,
        count(distinct case when snapshot_kind = 'end' and is_churned = true
            and date_trunc('month', updated_at)::date = month
            then client_id end)::bigint as churned_clients
    from client_states where preferred_store_id is not null group by 1, 2
),
history_start as (
    select min(effective_from) as first_available_date from daily_history
),
monthly_orders as (
    select date_trunc('month', order_date)::date as month, store_id,
           count(distinct client_id)::bigint as active_client_count
    from {{ ref('fact_orders') }}
    where latest_order_status_id = 3 and client_id is not null
      and {{ ampere_gold_month_predicate("date_trunc('month', order_date)") }}
    group by 1, 2
)
select months.month, stores.store_id, coalesce(stores.store_name, 'Unknown') as store_name,
       stores.city, coalesce(orders.active_client_count, 0)::bigint as active_client_count,
       case when history.first_available_date is null or months.month < history.first_available_date
            then null else coalesce(metrics.new_client_count, 0)::bigint end as new_client_count,
       case when history.first_available_date is null or months.month < history.first_available_date
            then null else coalesce(metrics.client_base_start, 0)::bigint end as client_base_start,
       case when history.first_available_date is null or months.month < history.first_available_date
            then null else coalesce(metrics.churned_clients, 0)::bigint end as churned_clients
from selected_months as months
cross join {{ ref('dim_stores') }} as stores
cross join history_start as history
left join monthly_orders as orders on months.month = orders.month and stores.store_id = orders.store_id
left join preferred_store_metrics as metrics on months.month = metrics.month and stores.store_id = metrics.store_id
