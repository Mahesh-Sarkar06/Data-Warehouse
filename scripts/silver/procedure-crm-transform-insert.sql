/*
===============================================================================
Stored Procedure: Load CRM Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
	

Usage Example:
    CALL silver.crm_transform_and_insert();
===============================================================================
*/


CREATE OR REPLACE PROCEDURE silver.crm_transform_and_insert()
 LANGUAGE plpgsql
AS $$
DECLARE
  start_time TIMESTAMP;
  end_time TIMESTAMP;
  batch_start_time TIMESTAMP;
  batch_end_time TIMESTAMP;
  p_duration INTERVAL;
  b_duration INTERVAL;
BEGIN

    -- Batch start time
    batch_start_time := clock_timestamp();

--------------------------------------------------------------- CUSTOMER INFO TABLE -----------------------------------------------------------

    -- Start Time Logging for customer info table
    start_time := clock_timestamp();

  -- Step 1: Truncate Table
    RAISE NOTICE 'Truncating customer info table...';
    TRUNCATE TABLE silver.crm_cust_info;

  -- Step 2: TRANSFORMING & INSERTING into customer info table
    RAISE NOTICE 'Inserting data to customer info table...';
    INSERT INTO silver.crm_cust_info(
        cust_id, 
        cust_key, 
        cust_firstname, 
        cust_lastname, 
        cust_marital_status, 
        cust_gndr, 
        cust_create_date
    )
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

    -- Step 3: Completion Message
    RAISE NOTICE 'Data insertion completed successfully.';

    -- Step 4: Duration Calculation
    end_time := clock_timestamp();
    p_duration := end_time - start_time;
    RAISE NOTICE 'Customer info load duration: % seconds', EXTRACT(EPOCH FROM p_duration);

--------------------------------------------------------------- PRODUCT INFO TABLE -----------------------------------------------------------
    -- Start Time Logging for product info table
    start_time := clock_timestamp();

    -- Step 1: Truncate Table
    RAISE NOTICE 'Truncating product info table...';
    TRUNCATE TABLE silver.crm_prod_info;

    -- Step 2: TRANSFORMING & INSERTING into product info table
    RAISE NOTICE 'Inserting data to product info table...';
    INSERT INTO silver.crm_prod_info (
        prod_id, 
        prod_key, 
        prod_cat_id, 
        prod_cat_key, 
        prod_name, 
        prod_cost, 
        prod_line, 
        prod_start_date, 
        prod_end_date
    )
    SELECT 
        prod_id, 
        prod_key,
        REPLACE(SUBSTRING(prod_key, 1, 5), '-', '_') AS prod_cat_id,
        SUBSTRING(prod_key, 7, LENGTH(prod_key)) AS prod_cat_key,
        TRIM(prod_name) AS prod_name,
        COALESCE(prod_cost, 0) AS prod_cost,
        CASE
            WHEN UPPER(prod_line) = 'M' THEN 'Mountain'
            WHEN UPPER(prod_line) = 'R' THEN 'Road'
            WHEN UPPER(prod_line) = 'S' THEN 'Other Sales'
            WHEN UPPER(prod_line) = 'T' THEN 'Touring'
            ELSE 'N/A'
        END prod_line,
        prod_start_date,
        LEAD(prod_start_date) OVER(PARTITION BY prod_key ORDER BY prod_start_date)-1 AS prod_end_date
    FROM bronze.crm_prod_info;


    -- Step 3: Completion Message
    RAISE NOTICE 'Data insertion completed successfully.';
    
    -- Step 4: Duration Calculation
    end_time := clock_timestamp();
    p_duration := end_time - start_time;
    RAISE NOTICE 'Product info load duration: % seconds', EXTRACT(EPOCH FROM p_duration);

--------------------------------------------------------------- SALES DETAILS TABLE -----------------------------------------------------------
    -- Start Time Logging for sales details table
    start_time := clock_timestamp();

    -- Step 1: Truncate Table
    RAISE NOTICE 'Truncating sales details table...';
    TRUNCATE TABLE silver.crm_sales_details;

    -- Step 2: TRANSFORMING & INSERTING into sales details table
    RAISE NOTICE 'Inserting data to sales details table...';
    INSERT INTO silver.crm_sales_details
    (
        sales_ord_num, 
        sales_prod_key, 
        sales_cust_id, 
        sales_ord_date, 
        sales_ship_date, 
        sales_due_date, 
        sales, 
        quantity, 
        sales_price
    )
    SELECT 
        TRIM(sale_ord_num) AS sales_ord_num, 
        TRIM(sale_prod_key) AS sales_prod_key, 
        sale_cust_id,
        CASE
            WHEN sale_ord_date = 0 OR LENGTH(sale_ord_date::VARCHAR) < 8 THEN NULL
            ELSE CAST(CAST(sale_ord_date AS VARCHAR) AS DATE)
        END sales_ord_date,
        CASE
            WHEN sale_ship_date = 0 OR LENGTH(sale_ship_date::VARCHAR) < 8 THEN NULL
            ELSE CAST(CAST(sale_ship_date AS VARCHAR) AS DATE)
        END sales_ship_date,
        CASE
            WHEN sale_due_date = 0 OR LENGTH(sale_due_date::VARCHAR) < 8 THEN NULL
            ELSE CAST(CAST(sale_due_date AS VARCHAR) AS DATE)
        END sales_due_date,
        CASE
            WHEN sales IS NULL OR sales <=0 OR sales <> ABS(quantity) * ABS(sale_price)
            THEN ABS(quantity) * ABS(sale_price)
            ELSE sales
        END sales,
        ABS(quantity) AS quantity,
        CASE
            WHEN sale_price IS NULL OR sale_price <= 0
            THEN ABS(sales) / NULLIF(ABS(quantity), 0)
            ELSE sale_price
        END sales_price
    FROM bronze.crm_sales_details;

    -- Step 3: Completion Message
    RAISE NOTICE 'Data insertion completed successfully.';

    -- Step 4: Duration Calculation
    end_time := clock_timestamp();
    p_duration := end_time - start_time;
    RAISE NOTICE 'Sales Details load duration: % seconds', EXTRACT(EPOCH FROM p_duration);



    -- Batch Completion Time
    batch_end_time := clock_timestamp();
    b_duration := batch_end_time - batch_start_time;
    RAISE NOTICE 'Batch load duration: % seconds', EXTRACT(EPOCH FROM b_duration);

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE NOTICE '❌ Data insertion failed. All the changes are rolled back.';
        RAISE NOTICE 'Error occurred: %', SQLERRM;
        RAISE NOTICE 'Error code: %', SQLSTATE;
        RAISE NOTICE 'Error code: %', SQLSTATE;
END;
$$;
