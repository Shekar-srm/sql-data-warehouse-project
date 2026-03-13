/*
-----------------------------------------------------------------------
Procedure Name : bronze.load_bronze
Description :
    Loads raw data from CRM and ERP CSV files into the bronze layer
    tables using BULK INSERT. Each table is truncated before loading
    to ensure a fresh full load.

Process:
    1. Truncate existing data in bronze tables
    2. Bulk load data from source CSV files
    3. Log load duration for each table and the entire batch

WARNING:
    - This procedure TRUNCATES all bronze tables before loading data.
    - Any existing data in these tables will be permanently deleted.
    - Ensure source files exist at the specified file paths before
      executing the procedure.
-----------------------------------------------------------------------
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
BEGIN TRY

	DECLARE 
		@start_time DATETIME,
		@end_time DATETIME,
		@batch_start_time DATETIME,
		@batch_end_time DATETIME;

	SET @batch_start_time = GETDATE();

	PRINT '========================================================';
	PRINT 'loading bronze layer';
	PRINT '========================================================';

	PRINT '--------------------------------------------------------';
	PRINT 'loading CRM tables';
	PRINT '--------------------------------------------------------';


	-- Load CRM Customer Info
	SET @start_time = GETDATE();
	PRINT '>>truncating table : bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;

	PRINT '>>inserting data into : bronze.crm_cust_info';
	BULK INSERT bronze.crm_cust_info
	FROM 'C:\Users\rajes\OneDrive\Desktop\SQL Rep\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT '>>load duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(50)) + ' seconds';
	PRINT '___________________________________________________________________';


	-- Load CRM Product Info
	SET @start_time = GETDATE();
	PRINT '>>truncating table : bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;

	PRINT '>>inserting data into : bronze.crm_prd_info';
	BULK INSERT bronze.crm_prd_info
	FROM 'C:\Users\rajes\OneDrive\Desktop\SQL Rep\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT '>>load duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(50)) + ' seconds';
	PRINT '___________________________________________________________________';


	-- Load CRM Sales Details
	SET @start_time = GETDATE();
	PRINT '>>truncating table : bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;

	PRINT '>>inserting data into : bronze.crm_sales_details';
	BULK INSERT bronze.crm_sales_details
	FROM 'C:\Users\rajes\OneDrive\Desktop\SQL Rep\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT '>>load duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(50)) + ' seconds';
	PRINT '___________________________________________________________________';


	PRINT '--------------------------------------------------------';
	PRINT 'loading ERP tables';
	PRINT '--------------------------------------------------------';


	-- Load ERP Customer Data
	SET @start_time = GETDATE();
	PRINT '>>truncating table : bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;

	PRINT '>>inserting data into : bronze.erp_cust_az12';
	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\Users\rajes\OneDrive\Desktop\SQL Rep\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT '>>load duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(50)) + ' seconds';
	PRINT '___________________________________________________________________';


	-- Load ERP Location Data
	SET @start_time = GETDATE();
	PRINT '>>truncating table : bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;

	PRINT '>>inserting data into : bronze.erp_loc_a101';
	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\Users\rajes\OneDrive\Desktop\SQL Rep\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT '>>load duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(50)) + ' seconds';
	PRINT '___________________________________________________________________';


	-- Load ERP Product Category Data
	SET @start_time = GETDATE();
	PRINT '>>truncating table : bronze.erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;

	PRINT '>>inserting data into : bronze.erp_px_cat_g1v2';
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'C:\Users\rajes\OneDrive\Desktop\SQL Rep\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT '>>load duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(50)) + ' seconds';
	PRINT '___________________________________________________________________';


	-- Total batch load duration
	SET @batch_end_time = GETDATE();
	PRINT '>>total load duration: ' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR(50)) + ' seconds';
	PRINT '___________________________________________________________________';


END TRY

BEGIN CATCH

	-- Basic error logging
	PRINT '++++++++++++++++++++++++++++++++++++++++++';
	PRINT 'error occurred during loading bronze layer';
	PRINT 'error message : ' + ERROR_MESSAGE();
	PRINT 'error NO : ' + CAST(ERROR_NUMBER() AS NVARCHAR(50));
	PRINT 'error state : ' + CAST(ERROR_STATE() AS NVARCHAR(50));
	PRINT '++++++++++++++++++++++++++++++++++++++++++';

END CATCH

END
