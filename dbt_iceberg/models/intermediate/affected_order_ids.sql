{{ config(database='ampere_work', schema='silver_control', materialized='table', tags=['silver', 'intermediate']) }}

{% if ampere_silver_run_mode() == 'full_history' %}
select cast(id as integer) as order_id from {{ source('bronze', 'orders') }}
union
select cast(order_id as integer) from {{ source('bronze', 'order_product') }}
union
select cast(order_id as integer) from {{ source('bronze', 'payments') }}
union
select cast(order_id as integer) from {{ source('bronze', 'order_status_history') }}
union
select cast(order_id as integer) from {{ source('bronze', 'delivery_tracking') }}
{% else %}
with changed_orders as (
    select cast(id as integer) as order_id from {{ source('raw', 'orders') }}
    where {{ ampere_raw_delta_predicate('') }}
    union
    select cast(order_id as integer) from {{ source('raw', 'order_product') }}
    where {{ ampere_raw_delta_predicate('') }}
    union
    select cast(order_id as integer) from {{ source('raw', 'payments') }}
    where {{ ampere_raw_delta_predicate('') }}
    union
    select cast(order_id as integer) from {{ source('raw', 'order_status_history') }}
    where {{ ampere_raw_delta_predicate('') }}
    union
    select cast(order_id as integer) from {{ source('raw', 'delivery_tracking') }}
    where {{ ampere_raw_delta_predicate('') }}
),
changed_clients as (
    select distinct cast(id as integer) as client_id from {{ source('raw', 'clients') }}
    where {{ ampere_raw_delta_predicate('') }}
),
changed_stores as (
    select cast(id as integer) as store_id from {{ source('raw', 'stores') }}
    where {{ ampere_raw_delta_predicate('') }}
),
changed_products as (
    select cast(id as integer) as product_id from {{ source('raw', 'products') }}
    where {{ ampere_raw_delta_predicate('') }}
),
changed_statuses as (
    select cast(id as integer) as order_status_id from {{ source('raw', 'order_statuses') }}
    where {{ ampere_raw_delta_predicate() }}
),
changed_categories as (
    select cast(id as integer) as category_id from {{ source('raw', 'product_categories') }}
    where {{ ampere_raw_delta_predicate() }}
),
changed_resources as (
    select cast(id as integer) as delivery_resource_id from {{ source('raw', 'delivery_resource') }}
    where {{ ampere_raw_delta_predicate('') }}
),
changed_delivery_types as (
    select cast(id as integer) as delivery_type_id from {{ source('raw', 'delivery_type') }}
    where {{ ampere_raw_delta_predicate('') }}
),
changed_zone_ids as (
    select cast(zone_id as integer) as zone_id from {{ source('raw', 'delivery_costing') }}
    where {{ ampere_raw_delta_predicate('') }}
    union
    select cast(id as integer) from {{ source('raw', 'zones') }}
    where {{ ampere_raw_delta_predicate('') }}
),
changed_zone_stores as (
    select z.store_id from {{ source('bronze', 'zones') }} z
    join changed_zone_ids cz on z.id=cz.zone_id
    union
    -- Include previous owners when a zone is moved or deleted.
    select z.store_id from {{ source('raw', 'zones') }} z
    join changed_zone_ids cz on z.id=cz.zone_id
    where {{ ampere_raw_dataset_predicate('z.') }}
      and z._batch_sequence <= {{ env_var('SILVER_DELTA_TO_SEQUENCE', '0') | int }}
),
affected as (
    select order_id from changed_orders
    union
    select cast(o.id as integer) from {{ source('bronze', 'orders') }} o
    join changed_clients c on o.client_id=c.client_id
    union
    select cast(o.id as integer) from {{ source('bronze', 'orders') }} o
    join changed_stores s on o.store_id=s.store_id
    union
    select cast(o.id as integer) from {{ source('bronze', 'orders') }} o
    join {{ source('bronze', 'order_product') }} op on op.order_id=o.id
    join changed_products p on op.product_id=p.product_id
    union
    select cast(o.id as integer) from {{ source('bronze', 'orders') }} o
    join {{ source('bronze', 'order_product') }} op on op.order_id=o.id
    join {{ source('raw', 'costing') }} c
      on c.product_id=op.product_id and c.store_id=o.store_id
    where {{ ampere_raw_delta_predicate('c.') }}
    union
    select cast(op.order_id as integer) from {{ source('bronze', 'order_product') }} op
    join {{ source('bronze', 'products') }} p on op.product_id=p.id
    join changed_categories c on p.category_id=c.category_id
    union
    select cast(h.order_id as integer) from {{ source('bronze', 'order_status_history') }} h
    join changed_statuses s on h.order_status_id=s.order_status_id
    union
    select cast(t.order_id as integer) from {{ source('bronze', 'delivery_tracking') }} t
    join changed_resources r on t.courier_id=r.delivery_resource_id
    union
    select cast(t.order_id as integer) from {{ source('bronze', 'delivery_tracking') }} t
    join {{ source('bronze', 'delivery_resource') }} r on t.courier_id=r.id
    join changed_delivery_types dt on r.delivery_type_id=dt.delivery_type_id
    union
    select cast(o.id as integer) from {{ source('bronze', 'orders') }} o
    join changed_zone_stores zs on zs.store_id=o.store_id
)
select distinct order_id from affected where order_id is not null
{% endif %}
