with source_data as (
    select * from {{ source('bronze', 'zones') }}
)
,
typed as (
    select
        cast(id as integer) as zone_id,
        cast(store_id as integer) as store_id,
        cast(zone_name as varchar) as zone_name,
        {{ ampere_bronze_lineage_columns() }},
        {{ ampere_silver_lineage_columns() }}
    from source_data
),
latest as (
    select *
    from typed
    qualify row_number() over (
        partition by zone_id
        order by
            try_cast(_bronze_last_apply_ts as timestamp) desc nulls last,
            _bronze_last_run_id desc nulls last
    ) = 1
)
select * from latest
