/*
===============================================================================
                            Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
*/

-----------------------------------------------------------------------
-- Checking 'silver.crm_cust_info'
-----------------------------------------------------------------------
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    cust_id,
    COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cust_id
HAVING COUNT(*) > 1 OR cust_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    cust_key 
FROM silver.crm_cust_info
WHERE cust_key <> TRIM(cust_key);

-- Data Standardization & Consistency
SELECT DISTINCT 
    cust_marital_status 
FROM silver.crm_cust_info;

-----------------------------------------------------------------------
-- Checking 'silver.crm_prod_info'
-----------------------------------------------------------------------
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    prod_id,
    COUNT(*) 
FROM silver.crm_prod_info
GROUP BY prod_id
HAVING COUNT(*) > 1 OR prod_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    prod_name 
FROM silver.crm_prod_info
WHERE prod_name <> TRIM(prod_name);

-- Check for NULLs or Negative Values in Cost
-- Expectation: No Results
SELECT 
    prod_cost 
FROM silver.crm_prod_info
WHERE prod_cost < 0 OR prod_cost IS NULL;

-- Data Standardization & Consistency
SELECT DISTINCT 
    prod_line 
FROM silver.crm_prod_info;

-- Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results
SELECT 
    * 
FROM silver.crm_prod_info
WHERE prod_end_date < prod_start_date;

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================
-- Check for Invalid Dates
-- Expectation: No Invalid Dates
SELECT 
    NULLIF(sale_due_date, 0) AS sales_due_date 
FROM bronze.crm_sales_details
WHERE sale_due_date <= 0 
    OR LENGTH(sale_due_date) <> 8 
    OR sale_due_date > 20500101 
    OR sale_due_date < 19000101;

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results
SELECT 
    * 
FROM silver.crm_sales_details
WHERE sales_order_date > sales_ship_date 
   OR sales_order_date > sales_due_date;

-- Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results
SELECT DISTINCT 
    sales,
    sales_quantity,
    sales_price 
FROM silver.crm_sales_details
WHERE sales != sales_quantity * sales_price
   OR sales IS NULL
   OR sales_quantity IS NULL
   OR sales_price IS NULL
   OR sales_sales <= 0 
   OR sales_quantity <= 0 
   OR sales_price <= 0
ORDER BY sales, sales_quantity, sales_price;

-- ====================================================================
-- Checking 'silver.erp_cust'
-- ====================================================================
-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
SELECT DISTINCT 
    dob 
FROM silver.erp_cust
WHERE dob < '1924-01-01' 
   OR dob > CURRENT_DATE;

-- Data Standardization & Consistency
SELECT DISTINCT 
    sex 
FROM silver.erp_cust;

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
-- Data Standardization & Consistency
SELECT DISTINCT 
    country 
FROM silver.erp_loc
ORDER BY country;

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    * 
FROM silver.erp_prod_cat
WHERE category != TRIM(category) 
   OR sub_category != TRIM(sub_category) 
   OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT 
    maintenance 
FROM silver.erp_prod_cat;
