ALTER VIEW reporting.vw_dim_resource
AS

    SELECT

        dr.id as courier_id,
        dr.fullname,
        del_type.courier_type,
        CURRENT_TIMESTAMP as _updated_at

    FROM stg.dim_delivery_resource dr

        JOIN stg.dim_delivery_type del_type ON del_type.id = dr.delivery_type_id