ALTER VIEW reporting.vw_dim_costing
AS

    SELECT

        product_id,
        store_id,
        avg_cost,
        CURRENT_TIMESTAMP as _updated_at

    FROM stg.dim_costing