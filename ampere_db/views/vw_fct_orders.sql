ALTER VIEW reporting.vw_fct_orders
AS


    -- SELECTING ONLY DELIVERED ORDERS FOR FACT TABLE

    SELECT

        id as order_id,
        client_id,
        order_date,
        order_source_id,
        total_amount,
        CURRENT_TIMESTAMP as _updated_at

    FROM stg.fact_orders fo

        JOIN stg.dim_order_status_history osh ON osh.order_id = fo.id

    WHERE osh.order_status_id = 3