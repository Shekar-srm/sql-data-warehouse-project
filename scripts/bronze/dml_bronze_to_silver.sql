/*
-----------------------------------------------------------------------
Script Name : CRM Customer Data Validation & Transformation
Description :
    Performs basic data quality checks on the bronze.crm_cust_info table
    and loads cleaned data into the silver.crm_cust_info table.

Process:
    1. Validate source data (duplicates, nulls, unwanted spaces, values)
    2. Standardize gender and marital status values
    3. Remove duplicates by keeping the most recent record
    4. Load cleaned data into the silver layer

WARNING:
    - This script assumes the bronze layer contains raw source data.
    - Duplicate records are removed using the latest cst_create_date.
    - Records with NULL customer IDs will be excluded from the load.
-----------------------------------------------------------------------
*/


-- Check for duplicate customer records (should return no rows)
SELECT *
FROM (
		SELECT *,
			   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
	) t
WHERE flag_last > 1 OR cst_id IS NULL;


-- Check for unwanted spaces in customer key
SELECT cst_key
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key);


-- Check for unwanted spaces in first name
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);


-- Check for unwanted spaces in last name
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);


-- Check for unwanted spaces in gender column
SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);


-- Review distinct gender values for consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;


-- Review distinct marital status values for consistency
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;


-- Review cleaned data in silver layer
SELECT *
FROM silver.crm_cust_info;


-- Load cleaned and standardized data into silver layer
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
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,

	-- Standardize marital status values
	CASE 
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'n/a'
	END AS cst_marital_status,

	-- Standardize gender values
	CASE 
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'n/a'
	END AS cst_gndr,

	cst_create_date
FROM (
		SELECT *,
			   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
	) t
WHERE flag_last = 1;
