/*
==========================================================================================
Stored Procedure : Load Silver Layer ( Bronze -> Silver)
==========================================================================================
Script Purpose : 
  This stored procedure performs ETL (Extract , Transform , Load) process to populate 
  the silver schema tables from the bronze schema . 
Actions Performed :
  --Truncate silver tables 
  -- Inserts transformed and cleansed data from bronze to silver tables . 

Parameters : 
  None 
 This stored procedure does not acept any parameters or return any values .

Usage example :
  EXEC Silver.Load_silver 
========================================================================================

*/


CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN 

--Declare the varaibles 
	DECLARE @start_time DATETIME , @end_time DATETIME ,@batch_start_time DATETIME , @batch_end_time DATETIME 

	BEGIN TRY 
		SET @batch_start_time =GETDATE() ; 
		PRINT '=====================================================================================';
		PRINT 'Loading Silver  Layer' ;
		PRINT '=====================================================================================';

--==============================================================================================
		PRINT '-----------------------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables ' ;
		PRINT '-----------------------------------------------------------------------------------------';

	--CRM : silver.crm_cust_info 
	
	SET @start_time =GETDATE() ;

	PRINT'>> Truncating : silver.crm_cust_info  ';
	TRUNCATE TABLE silver.crm_cust_info 

	PRINT '>>Inserting data into : silver.crm_cust_info ';

	INSERT INTO silver.crm_cust_info (
		cst_id, 
		cst_key ,
		cst_firstname, 
		cst_lastname,
		cst_marital_status,  
		cst_gndr,
		cst_create_date
		) 
	
	SELECT 
		cst_id ,
		cst_key ,
		TRIM(cst_firstname) AS cst_firstname , 
		TRIM(cst_lastname) AS cst_lastname, 
		CASE WHEN UPPER(TRIM(cst_marital_status)) ='S'THEN 'Single' 
			 WHEN UPPER(TRIM(cst_marital_status)) ='M' THEN 'Married' 
			ELSE 'n/a' 
		END cst_marital_status, --normalize maritial staus top readable format 

		CASE WHEN UPPER(TRIM(cst_gndr)) ='F'THEN 'Female' 
			 WHEN UPPER(TRIM(cst_gndr)) ='M' THEN 'Male' 
			ELSE 'n/a' 
		END cst_gndr, --normalize gender to readable format 
		cst_create_date
	FROM (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	)t WHERE flag_last =1 --select the most recent record per customer 

	SET @end_time =GETDATE() ;
	PRINT'>> Load Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds'
	PRINT '------------------------------------------------------------' 

--***************************************************************************************************

	--CRM : silver.crm_prd_info

	SET @start_time =GETDATE() ;

	PRINT'>> Truncating : silver.crm_prd_info  ';
	TRUNCATE TABLE silver.crm_prd_info

	PRINT '>>Inserting data into : silver.crm_prd_info ';

	INSERT INTO silver.crm_prd_info
	(
		prd_id	,
		cat_id , 
		prd_key	,
		prd_nm	,
		prd_cost  , 
		prd_line  ,
		prd_start_dt  ,
		prd_end_dt  
	) 

	SELECT 
		prd_id, 
		REPLACE(SUBSTRING (prd_key,1,5)  , '-' ,'_') AS cat_id , -- Extraxt category id 
		SUBSTRING (prd_key,7,LEN(prd_key)) AS prd_key ,  -- Extract product key 
		prd_nm, 
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line)) 
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line , --map product line codes to descriptive value 
		CAST(prd_start_dt AS DATE ) AS prd_start_dt ,
		CAST(
			LEAD (prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) -1
			AS DATE
		) AS prd_end_dt --calculate end date as one day before the next start date 
	FROM bronze.crm_prd_info 

	SET @end_time =GETDATE() ;
	PRINT'>> Load Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds'
	PRINT '------------------------------------------------------------' 

--***************************************************************************************************

	--CRM : silver.crm_sales_details 

	SET @start_time =GETDATE() ;

	PRINT'>> Truncating : silver.crm_sales_details  ';
	TRUNCATE TABLE silver.crm_sales_details 

	PRINT '>>Inserting data into : silver.crm_sales_details ';

	INSERT INTO silver.crm_sales_details(
		sls_ord_num ,
		sls_prd_key	,
		sls_cust_id	,
		sls_order_dt ,
		sls_ship_dt ,
		sls_due_dt ,
		sls_sales ,
		sls_quantity ,
		sls_price 
	)


	SELECT 
		  sls_ord_num , 
		  sls_prd_key,
		  sls_cust_id,

		  --sales_order_dt ;
		  CASE 
		   WHEN sls_order_dt =0 OR LEN(sls_order_dt)!= 8 THEN NULL 
		   ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE ) 
		   END AS sls_order_dt,

		  --sls_ship_dt ;
		  CASE 
			WHEN sls_ship_dt =0 OR LEN(sls_ship_dt)!=8 THEN NULL 
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
			END AS sls_ship_dt ,

		--sls_due_dt 
		  CASE 
			WHEN sls_due_dt =0 OR LEN(sls_due_dt)!=8 THEN NULL 
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
			END AS sls_due_dt ,
      
		  CASE 
			  WHEN sls_sales IS NULL OR sls_sales<=0  OR sls_sales != sls_quantity * ABS(sls_price)
			  THEN sls_quantity * ABS(sls_price)
			  ELSE sls_sales 
		END AS sls_sales ,
    
			 sls_quantity,

		  CASE 
			WHEN sls_price IS NULL OR sls_price <=0 
			THEN sls_sales/NULLIF(sls_quantity,0)  --ensure it do not divide by zero 
			ELSE sls_price 
		END AS sls_price

	FROM bronze.crm_sales_details

	SET @end_time =GETDATE() ;
	PRINT'>> Load Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds'
	PRINT '------------------------------------------------------------' 

--==============================================================================================

	--ERP : silver.erp_cust_az12

		PRINT '-----------------------------------------------------------------------------------------';
		PRINT 'Loading ERP  Tables ' ;
		PRINT '-----------------------------------------------------------------------------------------';

	SET @start_time =GETDATE() ;


	PRINT '>> Truncating : silver.erp_cust_az12 ';
	TRUNCATE TABLE silver.erp_cust_az12

	PRINT 'Inserting data into :silver.erp_cust_az12';

	INSERT INTO silver.erp_cust_az12(
		cid, 
		bdate,
		gen 
	)


	SELECT 
		--cid 
		CASE 
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) --remove 'NAS' prefix if present 
			ELSE cid 
		END cid , 
		--bdate 
		CASE 
			WHEN bdate >GETDATE() THEN NULL 
			ELSE bdate 
		END AS bdate , --set future birthdates to NULL 
		--gen 
		CASE 
		WHEN UPPER(TRIM(gen)) IN ('M','Male')  THEN 'Male' 
		WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
		ELSE 'n/a' 
	END gen --normalize gender values and handle unknown values 
	FROM bronze.erp_cust_az12

	SET @end_time =GETDATE() ;
	PRINT'>> Load Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds'
	PRINT '------------------------------------------------------------' 

--***************************************************************************************************

	--ERP : silver.erp_loc_a101 

	SET @start_time =GETDATE() ;


	PRINT '>> Truncating : silver.erp_loc_a101 ';
	TRUNCATE TABLE silver.erp_loc_a101

	PRINT 'Inserting data into : silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry
	)

	SELECT 
		REPLACE (cid,'-','') AS cid , 
		CASE
			WHEN TRIM(cntry) = '' OR cntry  IS NULL THEN 'n/a'
			WHEN TRIM(cntry) = 'DE' THEN 'Germany' 
			WHEN TRIM(cntry) IN ('US' ,'USA') THEN 'United States'
			ELSE cntry 
		END AS cntry --Normalize and handle missing or blank country codes  
	FROM bronze.erp_loc_a101

	SET @end_time =GETDATE() ;
	PRINT'>> Load Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds'
	PRINT '------------------------------------------------------------' 

--***************************************************************************************************

	--ERP : silver.erp_px_cat_g1_v2 

	SET @start_time =GETDATE() ;


	PRINT '>> Truncating : silver.erp_px_cat_g1_v2';
	TRUNCATE TABLE silver.erp_px_cat_g1_v2 

	PRINT '>> Inserting Data Into : silver.crm_cust_info';
	INSERT INTO silver.erp_px_cat_g1_v2(
		id ,
		cat ,
		subcat ,
		maintenance 
	)

	SELECT 
		id , 
		cat, 
		subcat, 
		maitenance 
	FROM bronze.erp_px_cat_g1_v2

	SET @end_time =GETDATE() ;
	PRINT'>> Load Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds'
	PRINT '------------------------------------------------------------' 

--==============================================================================================

		SET @batch_end_time = GETDATE() ; 

		PRINT '===================================================='
		PRINT 'Loading Siver layer is completed' 
		PRINT '	Total Load Duration :' + CAST(DATEDIFF(second , @batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT'============================================================'

	END TRY 		

	BEGIN CATCH 
		PRINT '========================================================================================'
		PRINT'ERROR OCCURED DURING LOADING SILVER LAYER '
		PRINT 'Error Message' +ERROR_MESSAGE(); 
		PRINT 'Error Number'+ CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State'+ CAST(ERROR_STATE() AS NVARCHAR ) ; 
		PRINT '========================================================================================'
	END CATCH


END  
