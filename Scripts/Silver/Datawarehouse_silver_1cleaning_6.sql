/*
SILVER LAYER TRANSFORMATIONS
*/



--crm_cust_info : duplicates in primary key

SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) >1 OR cst_id IS NULL;
/*
29449	2
29473	2
29433	2
NULL	3
29483	2
29466	3
*/

SELECT * FROM  bronze.crm_cust_info
WHERE cst_id =29466;

SELECT *, ROW_NUMBER() OVER ( PARTITION BY cst_id ORDER BY cst_create_date DESC ) AS FLAGS_LAST
FROM  bronze.crm_cust_info
WHERE cst_id =29466;
/*
29466	AW00029466	Lance	Jimenez	M	M	2026-01-27	1
29466	AW00029466	Lance	Jimenez	M	NULL	2026-01-26	2
29466	AW00029466	NULL	NULL	NULL	NULL	2026-01-25	3
*/

SELECT * FROM (
SELECT *, ROW_NUMBER() OVER ( PARTITION BY cst_id ORDER BY cst_create_date DESC ) AS flag_last
FROM  bronze.crm_cust_info) t
WHERE flag_last =1 and cst_id = 29473;


-- Unwanted Spaces
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);


SELECT cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
			
		FROM (
SELECT *, ROW_NUMBER() OVER ( PARTITION BY cst_id ORDER BY cst_create_date DESC ) AS flag_last
FROM  bronze.crm_cust_info) t
WHERE flag_last =1

--DATA Stadardization & Consistency
SELECT DISTINCT cst_gndr 
FROM bronze.crm_cust_info

SELECT cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) ='S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) ='M' THEN 'Married'
			ELSE 'n/a'
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) ='F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) ='M' THEN 'Male'
			ELSE 'n/a'
		END cst_gndr,
		cst_create_date
			
		FROM (
SELECT *, ROW_NUMBER() OVER ( PARTITION BY cst_id ORDER BY cst_create_date DESC ) AS flag_last
FROM  bronze.crm_cust_info) t
WHERE flag_last =1


--Insert into SILVER Layer
INSERT INTO silver.crm_cust_info (cst_id,
									cst_key,
									cst_firstname,
									cst_lastname,
									cst_marital_status,
									cst_gndr, 
									cst_create_date)
SELECT cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) ='S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) ='M' THEN 'Married'
			ELSE 'n/a'
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) ='F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) ='M' THEN 'Male'
			ELSE 'n/a'
		END cst_gndr,
		cst_create_date
			
		FROM (
SELECT *, ROW_NUMBER() OVER ( PARTITION BY cst_id ORDER BY cst_create_date DESC ) AS flag_last
FROM  bronze.crm_cust_info) t
WHERE flag_last =1

--Verify
SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) >1 OR cst_id IS NULL;

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT DISTINCT cst_gndr 
FROM silver.crm_cust_info;

SELECT * FROM silver.crm_cust_info;


--crm_prd_info

SELECT * FROM bronze.crm_prd_info;

SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) >1 OR prd_id IS NULL;

--Derived Columns
SELECT 
SUBSTRING(prd_key,1,5) as cat_id,
REPLACE(SUBSTRING(prd_key,1,5), '-','_') as cat_id
FROM bronze.crm_prd_info

--cat_id: devived column to connect with epr
SELECT DISTINCT id from bronze.erp_px_cat_g1v2;

SELECT *,
SUBSTRING(prd_key,1,5) as cat_id,
REPLACE(SUBSTRING(prd_key,1,5), '-','_') as cat_id
FROM bronze.crm_prd_info

WHERE REPLACE(SUBSTRING(prd_key,1,5), '-','_') NOT IN (
SELECT DISTINCT id from bronze.erp_px_cat_g1v2);


--prd_key: derived col to connect with erp
SELECT *,
SUBSTRING(prd_key,7,len(prd_key)) as prd_key2
FROM bronze.crm_prd_info

WHERE SUBSTRING(prd_key,7,len(prd_key)) IN (
SELECT DISTINCT sls_prd_key from bronze.crm_sales_details);

--prd_cost check NEGATIVE or NULLS
SELECT prd_cost, ISNULL(prd_cost, 0) as prd_cost2
FROM bronze.crm_prd_info
WHERE prd_cost <0 or prd_cost IS NULL;

SELECT *,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) as prd_end_dt2,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 as prd_end_dt3,
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) as prd_end_dt4
FROM bronze.crm_prd_info;

--make changes in silverDDL for Derived columns
IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info
CREATE TABLE silver.crm_prd_info (
    prd_id INT,
	cat_id NVARCHAR(50),
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

--Insert into SilverLayer
INSERT INTO silver.crm_prd_info( prd_id,
									cat_id,
									prd_key,
									prd_nm,
									prd_cost,
									prd_line,
									prd_start_dt,
									prd_end_dt)
SELECT prd_id,
		REPLACE(SUBSTRING(prd_key, 1,5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7,len(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST( LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;

SELECT * FROM silver.crm_prd_info;


--crm_sales_details
SELECT 
sls_order_dt,
NULLIF(sls_order_dt,0) as sls_order_dt2
FROM bronze.crm_sales_details
WHERE sls_order_dt <=0 or LEN(sls_order_dt) !=8;

--Invalide Order Dates
SELECT * FROM bronze.crm_sales_details
WHERE sls_order_dt >sls_ship_dt
	OR sls_order_dt >sls_due_dt

SELECT DISTINCT
sls_sales as old_sales, 
sls_quantity, 
sls_price as old_price,
CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales !=sls_quantity *ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price<=0
		THEN sls_sales/NULLIF(sls_quantity, 0)
	ELSE sls_price
END AS sls_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity *sls_price
OR sls_sales IS NOT NULL OR sls_quantity IS NOT NULL OR sls_price IS NOT NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price;


IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details
CREATE TABLE silver.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

INSERT INTO silver.crm_sales_details (sls_ord_num,
										sls_prd_key,
										sls_cust_id,
										sls_order_dt,
										sls_ship_dt,
										sls_due_dt,
										sls_sales,
										sls_quantity,
										sls_price)
SELECT sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE	
			WHEN sls_order_dt =0 OR len(sls_order_dt) !=8 THEN NULL
			ELSE CAST(CAST( sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE	
			WHEN sls_ship_dt =0 OR len(sls_ship_dt) !=8 THEN NULL
			ELSE CAST(CAST( sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE	
			WHEN sls_due_dt =0 OR len(sls_due_dt) !=8 THEN NULL
			ELSE CAST(CAST( sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		
		CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales !=sls_quantity *ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,

		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price<=0
				THEN sls_sales/NULLIF(sls_quantity, 0)
			ELSE sls_price
		END AS sls_price
FROM bronze.crm_sales_details;

SELECT * FROM silver.crm_sales_details;


--erp_cust_az12
SELECT cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
END AS cid,
bdate,
gen
FROM bronze.erp_cust_az12	
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);

INSERT INTO silver.erp_cust_az12( cid, bdate, gen)
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
END AS cid,
CASE WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;

SELECT * FROM silver.erp_cust_az12;

INSERT INTO silver.erp_loc_a101(cid, cntry)
SELECT
REPLACE(cid, '-', '') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry is NULL THEN 'n/a'
		ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101;


SELECT * FROM silver.erp_loc_a101;

INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
SELECT id, 
		cat, 
		subcat, 
		maintenance
FROM bronze.erp_px_cat_g1v2

SELECT * FROM silver.erp_px_cat_g1v2;
