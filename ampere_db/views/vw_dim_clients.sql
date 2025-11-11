ALTER VIEW reporting.vw_dim_clients
AS

    SELECT

        id as client_id,
        fullname,
        preferred_store_id,
        registration_date,
        CURRENT_TIMESTAMP as _updated_at

    FROM stg.dim_clients
