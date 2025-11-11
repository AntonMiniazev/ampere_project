ALTER VIEW reporting.vw_dim_products
AS

    SELECT

        p.id,
        p.product_name,
        pc.category_name,
        CURRENT_TIMESTAMP as _updated_at

    FROM stg.dim_products p

        JOIN stg.dim_product_categories pc ON pc.id = p.category_id
