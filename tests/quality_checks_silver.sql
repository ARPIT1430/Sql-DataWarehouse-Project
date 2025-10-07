/*

========================================================================================================
Quality Checks 
========================================================================================================

Script Purpose : 

  This script performs various quality checks for data consistency , accuracy ,
  and standardization across the 'silver' schema . It include checks for :
  -- Null or duplicate primary keys 
  -- Unwanted spaces in string fields 
  -- Data standardisation and consistency 
  --Invalid date ranges and orders 
  -- Data consistency between related fields 

Usage Notes :
  - Run these checls for data loading silver layer 
  - Investigate and resolve any discrepancies found during the checks 

========================================================================================================
*/


--================================================================================
--Checking 'silver.crm_cust_info'
--================================================================================

--quality check 1 : 

SELECT 
cst_id , 
COUNT(*) 
FROM silver.crm_cust_info 
GROUP BY cst_id 
HAVING COUNT(*) >1 OR cst_id IS NULL 




--quality check 2 : 

SELECT cst_firstname 
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)


--quality check 3 : 
SELECT DISTINCT cst_gndr 
FROM silver.crm_cust_info

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info



--================================================================================
--Checking 'silver.crm_prd_info'
--================================================================================


--Check for Nulls or Duplicates in Primary Key 
--Expectations : No result 

SELECT 
prd_id ,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) >1 


SELECT 
prd_key 
FROM silver.crm_prd_info
WHERE prd_key != TRIM(prd_key) 
--no unwanted spaces errors 

--Check for unwanted spaces 
--Expectations : No results 
SELECT 
prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm) 


--Check for NULLS or Negative Numbers 
--Expectation : No results 
SELECT 
prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost<0 


--Data Standardisation & Consistency 
SELECT DISTINCT 
prd_line 
FROM silver.crm_prd_info


--Check for Invalid Date Orders 
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt  



--================================================================================
--Checking 'silver.crm_prd_sales_details'
--================================================================================


--check for unwanted spaces :
SELECT 
sls_ord_num 
FROM silver.crm_sales_details
WHERE TRIM(sls_ord_num)!= sls_ord_num 

--check for invalid date orders 
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_ship_dt 


--Check Data consistency : Between sales , quanity and price 
-->> Sales = Quantity * Price 
-->> Values must not be NULL , zero or negative  

SELECT DISTINCT 
sls_sales,
sls_quantity, 
sls_price
FROM silver.crm_sales_details
WHERE sls_sales!=sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL 
OR sls_sales <= 0  OR sls_quantity <= 0 OR sls_price <= 0 
ORDER BY sls_sales, sls_quantity , sls_price 


--================================================================================
--Checking 'silver.erp_loc_a101'
--================================================================================

SELECT DISTINCT
cntry 
FROM silver.erp_loc_a101


--================================================================================
--Checking 'silver.erp_px_cat_g1_v2
--================================================================================


SELECT  * FROM silver.erp_px_cat_g1_v2
WHERE cat!=TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

SELECT DISTINCT 
cat 
FROM silver.erp_px_cat_g1_v2
--perfect 

SELECT DISTINCT 
subcat 
FROM silver.erp_px_cat_g1_v2
--perfect 


SELECT DISTINCT 
maintenance
FROM silver.erp_px_cat_g1_v2



