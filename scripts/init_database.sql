/*
-----------------------------------------------------------------------
Script Name : Data Warehouse Initialization Script
Description :
    This script recreates the DataWareHouse database from scratch.
    If the database already exists, it will terminate active connections,
    drop the database, recreate it, and then create the required schemas
    for the data warehouse layers.

Schemas:
    bronze  - Raw ingested data
    silver  - Cleaned and transformed data
    gold    - Business-ready data for reporting and analytics

WARNING:
    This script will permanently DELETE the existing DataWareHouse database
    along with all its data. Ensure backups are taken before executing in
    production or shared environments.
-----------------------------------------------------------------------
*/

USE master;

-- Check if the DataWareHouse database already exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWareHouse')
BEGIN
    -- Force close active connections before dropping the database
    ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

    -- Drop the existing database
    DROP DATABASE DataWareHouse;
END;

-- Create a fresh DataWareHouse database
CREATE DATABASE DataWareHouse;

USE DataWareHouse;

-- Create schemas representing different layers of the warehouse
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
