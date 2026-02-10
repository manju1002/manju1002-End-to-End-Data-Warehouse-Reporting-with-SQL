
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME  
	BEGIN TRY
		SET @start_time =GETDATE();
		PRINT '>> Insert Data into bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM  'C:\Users\VISHRUTH M S\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		 );
		
		PRINT '>> Insert Data into bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM  'C:\Users\VISHRUTH M S\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		 );

		PRINT '>> Insert Data into bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM  'C:\Users\VISHRUTH M S\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		 );


		PRINT '>> Insert Data into bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM  'C:\Users\VISHRUTH M S\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		 );

		PRINT '>> Insert Data into bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM  'C:\Users\VISHRUTH M S\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		 );

		PRINT '>> Insert Data into bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM  'C:\Users\VISHRUTH M S\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		 );
		 SET @end_time =GETDATE();
		 PRINT 'VISH TIMINGS: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR);
	END TRY
	BEGIN CATCH
		PRINT ERROR_MESSAGE();
		PRINT CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT CAST(ERROR_STATE() AS NVARCHAR);
	END CATCH
END
