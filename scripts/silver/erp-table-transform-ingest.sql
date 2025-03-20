/*
======================================================================================================================================================
                                          TRANSFORMING DATA & INSERTING INTO SILVER LAYER FOR CRM TABLES


NOTE: This transformatioN will not effect original raw data. It will transform on real time basis and ingest into silver layer ERP tables.
======================================================================================================================================================
*/

-- TRANSFORMATION & INSERTION OF erp_cust TABLE INTO SILVER LAYER
TRUNCATE TABLE silver.erp_cust;
INSERT INTO silver.erp_cust (cid, dob, sex)
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
	 ELSE cid
END cid,
CASE WHEN dob > CURRENT_DATE THEN NULL
	 ELSE dob
END dob,
CASE WHEN UPPER(TRIM(sex)) IN ('M', 'MALE') THEN 'Male'
	 WHEN UPPER(TRIM(sex)) IN ('F', 'FEMALE') THEN 'Female'
	 ELSE 'N/A'
END sex
FROM bronze.erp_cust;


-- TRANSFORMATION & INSERTION OF erp_loc TABLE INTO SILVER LAYER
TRUNCATE TABLE silver.erp_loc;
INSERT INTO silver.erp_loc (cid, country)
SELECT
REPLACE(cid, '-', '') cid,
CASE WHEN country IS NULL OR TRIM(country) = '' THEN 'N/A'
	 WHEN TRIM(country) = 'DE' THEN 'Germany'
	 WHEN TRIM(country) IN ('US', 'United States') THEN 'USA'
	 ELSE country
END country
FROM bronze.erp_loc;


-- TRANSFORMATION & INSERTION OF erp_prod_cat TABLE INTO SILVER LAYER
TRUNCATE TABLE silver.erp_prod_cat;
INSERT INTO silver.erp_prod_cat (id, category, sub_category, maintenance)
SELECT id,
category,
sub_category,
maintenance
FROM bronze.erp_prod_cat;
