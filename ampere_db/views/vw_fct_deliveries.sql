ALTER VIEW reporting.vw_fct_deliveries
AS

    -- SELECTING ALL DELIVERIES 

    SELECT

        order_id,
        courier_id,
        delivery_status_id,
        status,
        status_datetime,
        CURRENT_TIMESTAMP as _updated_at

    FROM stg.fact_delivery_tracking fdt