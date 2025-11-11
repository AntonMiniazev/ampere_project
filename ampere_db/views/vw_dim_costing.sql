ALTER VIEW reporting.vw_dim_costing
AS

    SELECT

        с.[product_id],
        c.[store_id],
        c.[avg_cost]
CURRENT_TIMESTAMP as _updated_at

    FROM stg.dim_costing c

        JOIN stg.dim_product_categories pc ON pc.id = p.category_id
