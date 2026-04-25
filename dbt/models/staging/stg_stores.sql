with source_data as (
    select * from {{ source('bronze', 'stores') }}
)
,
typed as (
    select
        cast(id as smallint) as store_id,
        cast(city as varchar) as city,
        cast(store_name as varchar) as store_name,
        {{ ampere_bronze_lineage_columns() }},
        {{ ampere_silver_lineage_columns() }}
    from source_data
),
latest as (
    select *
    from typed
    qualify row_number() over (
        partition by store_id
        order by
            try_cast(_bronze_last_apply_ts as timestamp) desc nulls last,
            _bronze_last_run_id desc nulls last
    ) = 1
)
select * from latest
