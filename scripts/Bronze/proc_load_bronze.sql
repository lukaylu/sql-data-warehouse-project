/*
	THIS STORED PROCEDURE LOADS DATA INTO THE BRONZE SCHEMA FROM AN EXTERNAL CSV FILE.

	WHAT IT DOES:
	* TRUNCATES THE TABLES BEFORE LOADING DATA
	* USES BULK COMMAND TO INSERT DATA FROM THE EXTERNAL CSV FILE

	PARAMETERS:
		NONE
		DOES NOT ACCEPT OR RETURN ANY VALUE

	USAGE EXAMPLE:
	EXEC BRONZE.LOAD_BRONZE;
*/



CREATE OR ALTER PROCEDURE Bronze.load_Bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=================================================';

		PRINT '-------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------------';

		PRINT '>> Truncating table: Bronze.crm_cust_info';
		PRINT '>> Inserting Data into Bronze.crm_cust_info';

		SET @start_time = GETDATE();
		TRUNCATE TABLE Bronze.crm_cust_info;

		BULK INSERT Bronze.crm_cust_info
		FROM 'C:\Users\ONUOHA LUCKY\Desktop\oop\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: Bronze.crm_prd_info';
		PRINT '>> Inserting Data into Bronze.crm_prd_info';

		TRUNCATE TABLE Bronze.crm_prd_info;

		BULK INSERT Bronze.crm_prd_info
		FROM 'C:\Users\ONUOHA LUCKY\Desktop\oop\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: Bronze.crm_sales_details';
		PRINT '>> Inserting Data into Bronze.crm_sales_details';

		TRUNCATE TABLE Bronze.crm_sales_details;
		BULK INSERT Bronze.crm_sales_details
		FROM 'C:\Users\ONUOHA LUCKY\Desktop\oop\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------';

		PRINT '-------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: Bronze.erp_cust_az12';
		PRINT '>> Inserting Data into Bronze.erp_cust_az12';

		TRUNCATE TABLE Bronze.erp_cust_az12;
		BULK INSERT Bronze.erp_cust_az12
		FROM 'C:\Users\ONUOHA LUCKY\Desktop\oop\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: Bronze.erp_loc_a101';
		PRINT '>> Inserting Data into Bronze.erp_loc_a101';

		TRUNCATE TABLE Bronze.erp_loc_a101;
		BULK INSERT Bronze.erp_loc_a101
		FROM 'C:\Users\ONUOHA LUCKY\Desktop\oop\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: Bronze.erp_px_cat_g1v2';
		PRINT '>> Inserting Data into Bronze.erp_px_cat_g1v2';

		TRUNCATE TABLE Bronze.erp_px_cat_g1v2;
		BULK INSERT Bronze.erp_px_cat_g1v2
		FROM 'C:\Users\ONUOHA LUCKY\Desktop\oop\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------';

		SET @batch_end_time = GETDATE();
		PRINT 'LOADING BRONZE LAYER COMPLETED!'
		PRINT 'TOTAL LOADING TIME: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds'
		PRINT '-----------------------------------------------------'
	END TRY
	BEGIN CATCH
		PRINT '=============================================================';

		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'ERROR MESSAGE:' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE:' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE:' + CAST(ERROR_STATE() AS NVARCHAR);

		PRINT '=============================================================';
	END CATCH
END
