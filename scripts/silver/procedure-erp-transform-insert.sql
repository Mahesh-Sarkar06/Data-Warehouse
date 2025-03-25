/*
===============================================================================
Stored Procedure: Load ERP Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		

Usage Example:
    CALL silver.erp_transform_and_insert();
===============================================================================
*/


CREATE OR REPLACE PROCEDURE silver.erp_transform_and_insert()
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

--------------------------------------------------------------- CUSTOMER TABLE -----------------------------------------------------------

    -- Start Time Logging for customer info table
    start_time := clock_timestamp();

    -- Step 1: Truncate Table
    RAISE NOTICE 'Truncating customer table...';
    TRUNCATE TABLE silver.erp_cust;

    -- Step 2: TRANSFORMING & INSERTING into customer info table
    RAISE NOTICE 'Inserting data to customer table...';
    INSERT INTO silver.erp_cust(
        cid, 
        dob, 
        sex
    )
    SELECT
    CASE
        WHEN cid LIKE "NAS%" THEN SUBSTRING(cid, 4, LENGTH(cid))
        ELSE cid
    END cid,
    CASE
        WHEN dob > CURRENT_DATE THEN NULL
        ELSE dob
    END dob,
    CASE
        WHEN UPPER(sex) IN ('M', 'MALE') THEN 'Male'
        WHEN UPPER(sex) IN ('F', 'FEMALE') THEN 'Female'
        ELSE 'N/A'
    END sex
    FROM bronze.erp_cust;


    -- Step 3: Completion Message
    RAISE NOTICE 'Data insertion completed successfully.';

    -- Step 4: Duration Calculation
    end_time := clock_timestamp();
    p_duration := end_time - start_time;
    RAISE NOTICE 'Customer load duration: % seconds', EXTRACT(EPOCH FROM p_duration);

--------------------------------------------------------------- CUSTOMER LOCATION TABLE -----------------------------------------------------------
    -- Start Time Logging for product info table
    start_time := clock_timestamp();

    -- Step 1: Truncate Table
    RAISE NOTICE 'Truncating product table...';
    TRUNCATE TABLE silver.erp_loc;

    -- Step 2: TRANSFORMING & INSERTING into product info table
    RAISE NOTICE 'Inserting data to product table...';
    INSERT INTO silver.erp_loc (
        cid,
        country
    )
    SELECT 
        REPLACE(cid, '-', '') AS cid,
        CASE
            WHEN country IS NULL or TRIM(country) = '' THEN 'N/A'
            WHEN UPPER(TRIM(country)) = 'DE' THEN 'Germany'
            WHEN UPPER(TRIM(country)) IN ('US', 'UNITED STATES') THEN 'USA'
            ELSE TRIM(country)
        END country
    FROM bronze.erp_loc;


    -- Step 3: Completion Message
    RAISE NOTICE 'Data insertion completed successfully.';
    
    -- Step 4: Duration Calculation
    end_time := clock_timestamp();
    p_duration := end_time - start_time;
    RAISE NOTICE 'Customer location load duration: % seconds', EXTRACT(EPOCH FROM p_duration);



--------------------------------------------------------------- PRODUCT CATEGORY TABLE -----------------------------------------------------------
    -- Start Time Logging for product category table
    start_time := clock_timestamp();

    -- Step 1: Truncate Table
    RAISE NOTICE 'Truncating product category table...';
    TRUNCATE TABLE silver.erp_prod_cat;

    -- Step 2: TRANSFORMING & INSERTING into product category table
    RAISE NOTICE 'Inserting data to product category table...';
    INSERT INTO silver.erp_prod_cat (
        id,
        category,
        sub_category,
        maintenance
    )
    SELECT
        id,
        category,
        sub_category,
        maintenance
    FROM bronze.erp_prod_cat;

    -- Step 3: Completion Message
    RAISE NOTICE 'Data insertion completed successfully.';

    -- Step 4: Duration Calculation
    end_time := clock_timestamp();
    p_duration := end_time - start_time;
    RAISE NOTICE 'Product Category load duration: % seconds', EXTRACT(EPOCH FROM p_duration);



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
