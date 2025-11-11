ALTER PROCEDURE [reporting].[Populate_bussiness_logic_tables]
AS
  BEGIN
    SET nocount ON;
    SET xact_abort ON;

    BEGIN TRANSACTION;

    BEGIN try
          -- Step 1: popilating dimension tables
          -- Step 1.1; populating dim_clients for Marketing and Financial Performance reporting
          EXEC [reporting].[Populate_and_index_table]
            @target = 'dim_clients',
            @source = 'vw_dim_clients';

          -- Step 1.2; populating dim_stores for Marketing and Financial Performance and Delivery reporting
          EXEC [reporting].[Populate_and_index_table]
            @target = 'dim_stores',
            @source = 'vw_dim_stores';

          -- Step 1.3; populating dim_costing for Financial Performance reporting
          EXEC [reporting].[Populate_and_index_table]
            @target = 'dim_costing',
            @source = 'vw_dim_costing';

          -- Step 1.4; populating dim_delivery_cost for Financial Performance and Delivery reporting
          EXEC [reporting].[Populate_and_index_table]
            @target = 'dim_delivery_cost',
            @source = 'vw_dim_delivery_cost';

          -- Step 1.5; populating dim_products for Marketing reporting
          EXEC [reporting].[Populate_and_index_table]
            @target = 'dim_products',
            @source = 'vw_dim_products';

          -- Step 1.6; populating dim_resource for Delivery reporting
          EXEC [reporting].[Populate_and_index_table]
            @target = 'dim_resource',
            @source = 'vw_dim_resource';

          -- Commit if success
          COMMIT TRANSACTION
      END try

      BEGIN catch
          ROLLBACK TRANSACTION

          DECLARE @error_message  NVARCHAR(4000) = Error_message(),
                  @error_severity INT = Error_severity(),
                  @error_state    INT = Error_state(),
                  @error_line     INT = Error_line();

          RAISERROR(@error_message,@error_severity,@error_state,@error_line);
      END catch
END; 