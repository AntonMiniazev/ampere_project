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
    where _batch_sequence > {{ env_var('SILVER_DELTA_FROM_SEQUENCE', '0') }}
      and _batch_sequence <= {{ env_var('SILVER_DELTA_TO_SEQUENCE', '0') }}
    union
    select cast(order_id as integer) from {{ source('raw', 'order_product') }}
    where _batch_sequence > {{ env_var('SILVER_DELTA_FROM_SEQUENCE', '0') }}
      and _batch_sequence <= {{ env_var('SILVER_DELTA_TO_SEQUENCE', '0') }}
    union
    select cast(order_id as integer) from {{ source('raw', 'payments') }}
    where _batch_sequence > {{ env_var('SILVER_DELTA_FROM_SEQUENCE', '0') }}
      and _batch_sequence <= {{ env_var('SILVER_DELTA_TO_SEQUENCE', '0') }}
    union
    select cast(order_id as integer) from {{ source('raw', 'order_status_history') }}
    where _batch_sequence > {{ env_var('SILVER_DELTA_FROM_SEQUENCE', '0') }}
      and _batch_sequence <= {{ env_var('SILVER_DELTA_TO_SEQUENCE', '0') }}
    union
    select cast(order_id as integer) from {{ source('raw', 'delivery_tracking') }}
    where _batch_sequence > {{ env_var('SILVER_DELTA_FROM_SEQUENCE', '0') }}
      and _batch_sequence <= {{ env_var('SILVER_DELTA_TO_SEQUENCE', '0') }}
),
changed_clients as (
    select distinct cast(id as integer) as client_id from {{ source('raw', 'clients') }}
    where _batch_sequence > {{ env_var('SILVER_DELTA_FROM_SEQUENCE', '0') }}
      and _batch_sequence <= {{ env_var('SILVER_DELTA_TO_SEQUENCE', '0') }}
),
changed_stores as (
    select cast(id as integer) as store_id from {{ source('raw', 'stores') }}
    where _batch_sequence > {{ env_var('SILVER_DELTA_FROM_SEQUENCE', '0') }}
      and _batch_sequence <= {{ env_var('SILVER_DELTA_TO_SEQUENCE', '0') }}
    union
    select cast(store_id as integer) from {{ source('raw', 'zones') }}
    where _batch_sequence > {{ env_var('SILVER_DELTA_FROM_SEQUENCE', '0') }}
      and _batch_sequence <= {{ env_var('SILVER_DELTA_TO_SEQUENCE', '0') }}
),
changed_products as (
    select cast(id as integer) as product_id from {{ source('raw', 'products') }}
    where _batch_sequence > {{ env_var('SILVER_DELTA_FROM_SEQUENCE', '0') }}
      and _batch_sequence <= {{ env_var('SILVER_DELTA_TO_SEQUENCE', '0') }}
    union
    select cast(product_id as integer) from {{ source('raw', 'costing') }}
    where _batch_sequence > {{ env_var('SILVER_DELTA_FROM_SEQUENCE', '0') }}
      and _batch_sequence <= {{ env_var('SILVER_DELTA_TO_SEQUENCE', '0') }}
),
changed_resources as (
    select cast(id as integer) as delivery_resource_id from {{ source('raw', 'delivery_resource') }}
    where _batch_sequence > {{ env_var('SILVER_DELTA_FROM_SEQUENCE', '0') }}
      and _batch_sequence <= {{ env_var('SILVER_DELTA_TO_SEQUENCE', '0') }}
),
changed_delivery_types as (
    select cast(id as integer) as delivery_type_id from {{ source('raw', 'delivery_type') }}
    where _batch_sequence > {{ env_var('SILVER_DELTA_FROM_SEQUENCE', '0') }}
      and _batch_sequence <= {{ env_var('SILVER_DELTA_TO_SEQUENCE', '0') }}
),
changed_zone_ids as (
    select cast(zone_id as integer) as zone_id from {{ source('raw', 'delivery_costing') }}
    where _batch_sequence > {{ env_var('SILVER_DELTA_FROM_SEQUENCE', '0') }}
      and _batch_sequence <= {{ env_var('SILVER_DELTA_TO_SEQUENCE', '0') }}
    union
    select cast(id as integer) from {{ source('raw', 'zones') }}
    where _batch_sequence > {{ env_var('SILVER_DELTA_FROM_SEQUENCE', '0') }}
      and _batch_sequence <= {{ env_var('SILVER_DELTA_TO_SEQUENCE', '0') }}
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
    where c._batch_sequence > {{ env_var('SILVER_DELTA_FROM_SEQUENCE', '0') }}
      and c._batch_sequence <= {{ env_var('SILVER_DELTA_TO_SEQUENCE', '0') }}
    union
    select cast(t.order_id as integer) from {{ source('bronze', 'delivery_tracking') }} t
    join changed_resources r on t.courier_id=r.delivery_resource_id
    union
    select cast(t.order_id as integer) from {{ source('bronze', 'delivery_tracking') }} t
    join {{ source('bronze', 'delivery_resource') }} r on t.courier_id=r.id
    join changed_delivery_types dt on r.delivery_type_id=dt.delivery_type_id
    union
    select cast(o.id as integer) from {{ source('bronze', 'orders') }} o
    join {{ source('bronze', 'zones') }} z on z.store_id=o.store_id
    join changed_zone_ids cz on cz.zone_id=z.id
)
select distinct order_id from affected where order_id is not null
{% endif %}
