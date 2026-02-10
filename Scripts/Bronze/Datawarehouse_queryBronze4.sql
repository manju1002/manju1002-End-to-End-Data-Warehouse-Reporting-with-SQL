SELECT TOP (1000) [cst_id]
      ,[cst_key]
      ,[cst_firstname]
      ,[cst_lastname]
      ,[cst_marital_status]
      ,[cst_gndr]
      ,[cst_create_date]
  FROM [DataWarehouse].[bronze].[crm_cust_info]

  SELECT TOP (1000) [prd_id]
      ,[prd_key]
      ,[prd_nm]
      ,[prd_cost]
      ,[prd_line]
      ,[prd_start_dt]
      ,[prd_end_dt]
  FROM [DataWarehouse].[bronze].[crm_prd_info]

  SELECT TOP 1000 * FROM bronze.crm_sales_details

  SELECT TOP 1000 * FROM bronze.erp_cust_az12

  SELECT TOP 1000 * FROM bronze.erp_loc_a101

  SELECT TOP 1000 * FROM bronze.erp_px_cat_g1v2
