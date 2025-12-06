ALTER VIEW reporting.vw_dim_costing
AS


    SELECT
        op.order_id,
        op.product_id,
        op.quantity,
        c.preferred_store_id AS store_id,
        cost.avg_cost,
        op.quantity * cost.avg_cost AS total_cost,
        CURRENT_TIMESTAMP as _updated_at
    FROM [stg].[dim_order_product] AS op
        JOIN [stg].[fact_orders] AS o
        ON o.id = op.order_id
        JOIN [stg].[dim_clients] AS c
        ON c.id = o.client_id
        JOIN [stg].[dim_costing] AS cost
        ON cost.product_id = op.product_id
            AND cost.store_id = c.preferred_store_id