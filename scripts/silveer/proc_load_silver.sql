/*
-----------------------------------------------------------------------
Procedure Name : silver.load_silver
Description :
    Loads cleaned and transformed data from the bronze layer into the
    silver layer. Applies data cleansing, standardization, and basic
    transformations before inserting into silver tables.

Process:
    1. Truncate silver tables
    2. Clean and transform bronze data
    3. Load into corresponding silver tables
    4. Log execution time for each step and full batch

WARNING:
    - This procedure TRUNCATES all silver tables before loading.
    - Existing data in silver layer will be permanently deleted.
    - Ensure bronze layer data is validated before execution.
-----------------------------------------------------------------------
*/

EXEC silver.load_silver;

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
BEGIN TRY

	DECLARE 
		@start_time DATETIME, 
		@end_time DATETIME, 
		@batch_start_time DATETIME, 
		@batch_end_time DATETIME;

	SET @batch_start_time = GETDATE();

	PRINT '========================================================';
	PRINT 'loading silver layer';
	PRINT '========================================================';

	PRINT '--------------------------------------------------------';
	PRINT 'loading CRM tables';
	PRINT '--------------------------------------------------------';


	-- Load customer data with deduplication and standardization
	SET @start_time = GETDATE();
	PRINT '>>truncating table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;

	PRINT '>>inserting data into: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info
	(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)
	SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname),
		TRIM(cst_lastname),

		CASE 
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'n/a'
		END,

		CASE 
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
		END,

		cst_create_date
	FROM (
			SELECT *,
				   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
	) t
	WHERE flag_last = 1;

	SET @end_time = GETDATE();
	PRINT '>>load duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(50)) + ' seconds';
	PRINT '___________________________________________________________________';


	-- Load product data with derived columns and transformations
	SET @start_time = GETDATE();
	PRINT '>>truncating table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;

	PRINT '>>inserting data into: silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info
	(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_'), -- derive category id
		SUBSTRING(prd_key,7,LEN(prd_key)),       -- extract product key
		prd_nm,
		COALESCE(prd_cost,0),                    -- handle null cost
		CASE UPPER(prd_line)
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END,
		CAST(prd_start_dt AS DATE),
		CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE)
	FROM bronze.crm_prd_info;

	SET @end_time = GETDATE();
	PRINT '>>load duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(50)) + ' seconds';
	PRINT '___________________________________________________________________';


	-- Load sales data with data quality handling
	SET @start_time = GETDATE();
	PRINT '>>truncating table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;

	PRINT '>>inserting data into: silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details
	(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)
	SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,

		-- Convert integer dates to DATE, handle invalid values
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) <> 8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END,

		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END,

		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) <> 8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END,

		-- Fix incorrect or missing sales values
		CASE 
			WHEN sls_sales IS NULL OR sls_sales <= 0 
			     OR sls_sales <> sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END,

		sls_quantity,

		-- Derive price if missing/invalid
		CASE 
			WHEN sls_price IS NULL OR sls_price <= 0 
			THEN sls_sales / NULLIF(sls_quantity,0)
			ELSE sls_price
		END

	FROM bronze.crm_sales_details;

	SET @end_time = GETDATE();
	PRINT '>>load duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(50)) + ' seconds';
	PRINT '___________________________________________________________________';


	PRINT '--------------------------------------------------------';
	PRINT 'loading ERP tables';
	PRINT '--------------------------------------------------------';


	-- Load ERP customer data with normalization
	SET @start_time = GETDATE();
	PRINT '>>truncating table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;

	PRINT '>>inserting data into: silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
	SELECT
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) ELSE cid END, -- remove prefix
		CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,                 -- future date handling
		CASE 
			WHEN TRIM(UPPER(gen)) IN ('F','FEMALE') THEN 'Female'
			WHEN TRIM(UPPER(gen)) IN ('M','MALE') THEN 'Male'
			ELSE 'n/a'
		END
	FROM bronze.erp_cust_az12;

	SET @end_time = GETDATE();
	PRINT '>>load duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(50)) + ' seconds';
	PRINT '___________________________________________________________________';


	-- Load ERP location data
	SET @start_time = GETDATE();
	PRINT '>>truncating table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;

	PRINT '>>inserting data into: silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101 (cid, cntry)
	SELECT
		REPLACE(cid,'-',''), -- remove special characters
		CASE 
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END
	FROM bronze.erp_loc_a101;

	SET @end_time = GETDATE();
	PRINT '>>load duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(50)) + ' seconds';
	PRINT '___________________________________________________________________';


	-- Load ERP product category data (no transformation required)
	SET @start_time = GETDATE();
	PRINT '>>truncating table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;

	PRINT '>>inserting data into: silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
	SELECT id, cat, subcat, maintenance
	FROM bronze.erp_px_cat_g1v2;

	SET @end_time = GETDATE();
	PRINT '>>load duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(50)) + ' seconds';
	PRINT '___________________________________________________________________';


	-- Total batch duration
	SET @batch_end_time = GETDATE();
	PRINT '>>total load duration: ' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR(50)) + ' seconds';
	PRINT '___________________________________________________________________';


END TRY

BEGIN CATCH

	-- Basic error logging
	PRINT '++++++++++++++++++++++++++++++++++++++++++';
	PRINT 'error occurred during loading silver layer';
	PRINT 'error message : ' + ERROR_MESSAGE();
	PRINT 'error NO : ' + CAST(ERROR_NUMBER() AS NVARCHAR(50));
	PRINT 'error state : ' + CAST(ERROR_STATE() AS NVARCHAR(50));
	PRINT '++++++++++++++++++++++++++++++++++++++++++';

END CATCH

END
