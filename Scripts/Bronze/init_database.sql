/*
CREATE DATABASE SCHEMAS
*/

USE master;
GO

--Drop and recreate 'warehouse
IF EXISTS (SELECT 1 FROM sys.databases WHERE name='DataWarehouse')
BEGIN 
	ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDINATE;
	DROP DATABASE DataWarehouse;
END;
GO

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
