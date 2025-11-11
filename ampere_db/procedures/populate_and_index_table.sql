ALTER PROCEDURE [reporting].[Populate_and_index_table]
    @target NVARCHAR(128),
    @source NVARCHAR(128)
AS
BEGIN
    SET nocount ON;
    SET xact_abort ON;

    DECLARE @sql  NVARCHAR(max),
              @path NVARCHAR(100),
              @name NVARCHAR(100);

    SET @sql = '';
    SET @path = '[Ampere].[reporting].';
    SET @name = @target;
    SET @target = Concat(@path, Quotename(@target));
    SET @source = Concat(@path, Quotename(@source));
    -- Step 1: Drop the target table if it exists
    SET @sql = @sql + Concat('     DROP TABLE IF EXISTS ', @target, ';     ');
    -- Step 2: Populate the target table with data from the source view
    SET @sql = @sql + Concat('     SELECT *     INTO ', @target, '     FROM ', @source, '     OPTION (USE HINT (''ENABLE_PARALLEL_PLAN_PREFERENCE''));     ');
    -- Step 3: Create CCI on the target table
    SET @sql = @sql
                 + Concat('     CREATE CLUSTERED COLUMNSTORE INDEX cci_', @name,
                 ' ON ',
                 @target, ';');

    EXEC Sp_executesql
        @sql
END; 