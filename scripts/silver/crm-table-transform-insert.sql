/*
======================================================================================================================================================
                                          TRANSFORMING DATA & INSERTING INTO SILVER LAYER

NOTE: This transformatio will not effect original raw data. It will transform on real time basis and ingest into silver layer crm tables.
======================================================================================================================================================
*/

-- TRANSFORMATION & INSERTION OF crm_cust_id TABLE INTO SILVER LAYER
TRUNCATE TABLE silver.crm_cust_info
INSERT INTO silver.crm_cust_info(cust_id, cust_key, cust_firstname, cust_lastname, cust_marital_status, cust_gndr, cust_create_date)
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


-- TRANSFORMATION & INSERTION OF crm_prod_info TABLE INTO SILVER LAYER
TRUNCATE TABLE silver.crm_prod_info
INSERT INTO silver.crm_prod_info (prod_id, prod_key, prod_cat_id, prod_cat_key, prod_name, prod_cost, prod_line, prod_start_date, prod_end_date)
SELECT prod_id, prod_key,
REPLACE(SUBSTRING(prod_key, 1, 5), '-', '_') AS prod_cat_id,
SUBSTRING(prod_key, 7, LENGTH(prod_key)) AS prod_cat_key,
prod_name,
COALESCE(prod_cost, 0) AS prod_cost,
CASE UPPER(TRIM(prod_line))
	 WHEN 'M' THEN 'Mountain'
	 WHEN 'R' THEN 'Road'
	 WHEN 'S' THEN 'Other Sales'
	 WHEN 'T' THEN 'Touring'
	 ELSE 'N/A'
END AS prod_line,
prod_start_date,
LEAD(prod_start_date) OVER(PARTITION BY prod_key ORDER BY prod_start_date)-1 AS prod_end_date
FROM bronze.crm_prod_info;


-- TRANSFORMATION & INSERTION OF crm_sales_details TABLE INTO SILVER LAYER
TRUNCATE TABLE silver.crm_sales_details
INSERT INTO silver.crm_sales_details (sales_ord_num, sales_prod_key, sales_cust_id,
sales_ord_date, sales_ship_date, sales_due_date, sales, quantity, sales_price)
SELECT sale_ord_num,
sale_prod_key,
sale_cust_id,
CASE WHEN sale_ord_date = 0 OR LENGTH(sale_ord_date::VARCHAR) <> 8 THEN NULL
	 ELSE CAST(CAST(sale_ord_date AS VARCHAR) AS DATE)
END sale_ord_date,
CASE WHEN sale_ship_date = 0 OR LENGTH(sale_ship_date::VARCHAR) <> 8 THEN NULL
	 ELSE CAST(CAST(sale_ship_date AS VARCHAR) AS DATE)
END sale_ship_date,
CASE WHEN sale_due_date = 0 OR LENGTH(sale_due_date::VARCHAR) <> 8 THEN NULL
	 ELSE CAST(CAST(sale_due_date AS VARCHAR) AS DATE)
END sale_due_date,
CASE WHEN sales IS NULL OR sales <= 0 OR sales <> ABS(quantity) * ABS(sale_price)
	 THEN ABS(quantity) * ABS(sale_price)
	 ELSE sales
END new_sales,
quantity,
CASE WHEN sale_price IS NULL OR sale_price <= 0
	 THEN sales / NULLIF(quantity, 0)
	 ELSE sale_price
END new_price
FROM bronze.crm_sales_details;
