/*
======================================================================================================================================================
                                          TRANSFORMING DATA & INSERTING INTO SILVER LAYER
======================================================================================================================================================
*/

-- TRANSFORMATION & INSERTION OF crm_cust_id TABLE TO SILVER LAYER
INSERT INTO silver.crm_cust_info(cust_id, cust_key, cust_firstname, cust_lastname, cust_marital_status,
cust_gndr, cust_create_date)
SELECT
cust_id,
cust_key,
TRIM(cust_firstname) AS cust_firstname,
TRIM(cust_lastname) AS cust_lastname,
CASE WHEN UPPER(TRIM(cust_marital_status)) = 'M' THEN 'Married'
	 WHEN UPPER(TRIM(cust_marital_status)) = 'S' THEN 'Single'
	 ELSE 'N/A'
END cust_marital_status,
CASE WHEN UPPER(TRIM(cust_gndr)) = 'M' THEN 'Male'
	 WHEN UPPER(TRIM(cust_gndr)) = 'F' THEN 'Female'
	 ELSE 'N/A'
END cust_gndr,
cust_create_date
FROM (
SELECT *, ROW_NUMBER() OVER(PARTITION BY cust_id ORDER BY cust_create_date DESC) AS flag_count
FROM bronze.crm_cust_info
WHERE cust_id IS NOT NULL
) flag_tab
WHERE flag_count = 1;
