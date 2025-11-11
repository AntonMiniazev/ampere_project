ALTER VIEW reporting.vw_dim_delivery_cost
AS

    WITH
        store_delivery_cost
        AS
        (

            SELECT
                z.store_id,
                dc.delivery_type_id,
                MAX(dc.tariff) tariff
            FROM stg.dim_delivery_costing dc

                JOIN stg .dim_zones z ON z.id = dc.zone_id

            GROUP BY z.store_id,dc.delivery_type_id

        )

    SELECT

        dt.order_id,
        sdc.tariff,
        CURRENT_TIMESTAMP as _updated_at

    FROM stg.fact_delivery_tracking dt

        JOIN stg .dim_delivery_resource dr ON dr.id = dt.courier_id
        JOIN stg .dim_delivery_type del_type ON del_type.id = dr.delivery_type_id
        JOIN store_delivery_cost sdc ON sdc.store_id = dr.store_id AND sdc.delivery_type_id = dr.delivery_type_id

    WHERE dt.delivery_status_id = 2