ALTER VIEW reporting.vw_dim_stores
AS

    SELECT

        s.id as store_id,
        s.city,
        s.store_name,
        z.zone_name,
        CURRENT_TIMESTAMP as _updated_at

    FROM stg.dim_stores s

        JOIN stg.dim_zones z ON z.store_id = s.id



