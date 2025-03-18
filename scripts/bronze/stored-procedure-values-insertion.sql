/*
===============================================================================================================================================================
                                                INSERTING VALUES INTO TABLES USING PGADMIN
===============================================================================================================================================================

        -- CREATE STORED PROCEDURE FOR MULTIPLE TIMES LOADING DATA FROM THE SOURCE AND RUN SINGLE COMMAND.
        -- CHECK IF THERE EXISTS ANY VALUE IN THE TABLE THEN DELETE THE VALUES AND AGAIN UPLOAD THE VALUES.
        -- THIS FLOW CAN BE USED IN PGADMIN WORKSPACE.
        -- ENSURE ALL THE DATASET ARE LOCATED IN THE FOLDER WHERE PG USERS HAVE ACCESS TO READ.

---------------------------------------------- STEPS FOR MACBOOK USERS FOR PG USER ACCESS --------------------------------------------------------------------
        -- Open the TERMINAL
        -- Type   ls -ld /Library/PostgreSQL/<your current pg version>/data/
        -- If you get drwx------ then you don't have access to read the files as an external user.
        -- Now change the permission type for temporary usage.
        -- Type sudo chmod 750 /Library/PostgreSQL/<your current pg version>/data/
        -- Now again type   ls -ld /Library/PostgreSQL/<your current pg version>/data/
        -- This time you will have drwxr-x--- Now you can have read access to PG files.
        -- Now copy the datasets to PG folder.
        -- Type sudo cp -R <CURRENT LOCATION OF DATASET> <DESTINATION LOCATION>
        -- sudo is required because this time we are writing the folder.
        -- If you are not transferring the complete dataset folder then you can remove -R as it is used for recursive copy the files within a folder.

This whole procedure is done for bulk insertion of data from local system using PG Server. The /COPY is used on psql terminal as CLIENT SIDE command.
And cannot be used on PGADMIN Workspace.

Execution: CALL bronze.safe_bulk_insert();
*/

CREATE OR REPLACE PROCEDURE bronze.safe_bulk_insert()
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
    TRUNCATE TABLE bronze.crm_cust_info;

  -- Step 2: Check if CSV File Exists
    IF NOT pg_stat_file('<YOUR DATASET FOLDER LOCATION>/source_crm/cust_info.csv') IS NULL THEN
        RAISE NOTICE 'Bulk inserting data to customer info table...';

        -- Step 3: Bulk Insert Data
        COPY bronze.crm_cust_info 
        FROM '<YOUR DATASET FOLDER LOCATION>/source_crm/cust_info.csv'
        DELIMITER ',' CSV HEADER;

        -- Step 4: Completion Message
        RAISE NOTICE 'Data insertion completed successfully.';
    ELSE
        RAISE EXCEPTION 'CSV file not found at specified path.';
    END IF;

    -- Step 5: Duration Calculation
    end_time := clock_timestamp();
    p_duration := end_time - start_time;
    RAISE NOTICE 'Customer info load duration: % seconds', EXTRACT(EPOCH FROM p_duration);



------------------------------------------------------------- PRODUCT INFO TABLE ------------------------------------------------------------

	-- Start Time Logging for product info table
    start_time := clock_timestamp();

  -- Step 1: Truncate Table
    RAISE NOTICE 'Truncating product info table...';
    TRUNCATE TABLE bronze.crm_prod_info;

  -- Step 2: Check if CSV File Exists
    IF NOT pg_stat_file('<YOUR DATASET FOLDER LOCATION>/source_crm/prod_info.csv') IS NULL THEN
        RAISE NOTICE 'Bulk inserting data to product info table...';

        -- Step 3: Bulk Insert Data
        COPY bronze.crm_prod_info 
        FROM '<YOUR DATASET FOLDER LOCATION>/source_crm/prod_info.csv'
        DELIMITER ',' CSV HEADER;

        -- Step 4: Completion Message
        RAISE NOTICE 'Data insertion completed successfully.';
    ELSE
        RAISE EXCEPTION 'CSV file not found at specified path.';
    END IF;

    -- Step 5: Duration Calculation
    end_time := clock_timestamp();
    p_duration := end_time - start_time;
    RAISE NOTICE 'Product info load duration: % seconds', EXTRACT(EPOCH FROM p_duration);



------------------------------------------------------------- SALES DETAILS TABLE ------------------------------------------------------------

	-- Start Time Logging for sales details table
    start_time := clock_timestamp();

  -- Step 1: Truncate Table
    RAISE NOTICE 'Truncating sales details table...';
    TRUNCATE TABLE bronze.crm_sales_details;

  -- Step 2: Check if CSV File Exists
    IF NOT pg_stat_file('<YOUR DATASET FOLDER LOCATION>/source_crm/sales_details.csv') IS NULL THEN
        RAISE NOTICE 'Bulk inserting data to product info table...';

        -- Step 3: Bulk Insert Data
        COPY bronze.crm_prod_info 
        FROM '<YOUR DATASET FOLDER LOCATION>/source_crm/prod_info.csv'
        DELIMITER ',' CSV HEADER;

        -- Step 4: Completion Message
        RAISE NOTICE 'Data insertion completed successfully.';
    ELSE
        RAISE EXCEPTION 'CSV file not found at specified path.';
    END IF;

  -- Step 5: Duration Calculation
    end_time := clock_timestamp();
    p_duration := end_time - start_time;
    RAISE NOTICE 'Sales Details load duration: % seconds', EXTRACT(EPOCH FROM p_duration);

------------------------------------------------------------- CUSTOMER DETAILS TABLE ------------------------------------------------------------

	-- Start Time Logging for customer details table
    start_time := clock_timestamp();

  -- Step 1: Truncate Table
    RAISE NOTICE 'Truncating customer details table...';
    TRUNCATE TABLE bronze.erp_cust;

  -- Step 2: Check if CSV File Exists
    IF NOT pg_stat_file('<YOUR DATASET FOLDER LOCATION>/source_erp/CUST_AZ12.csv') IS NULL THEN
        RAISE NOTICE 'Bulk inserting data to customer details table...';

        -- Step 3: Bulk Insert Data
        COPY bronze.erp_cust 
        FROM '<YOUR DATASET FOLDER LOCATION>/source_erp/CUST_AZ12.csv'
        DELIMITER ',' CSV HEADER;

        -- Step 4: Completion Message
        RAISE NOTICE 'Data insertion completed successfully.';
    ELSE
        RAISE EXCEPTION 'CSV file not found at specified path.';
    END IF;

  -- Step 5: Duration Calculation
    end_time := clock_timestamp();
    p_duration := end_time - start_time;
    RAISE NOTICE 'Customer Details load duration: % seconds', EXTRACT(EPOCH FROM p_duration);



------------------------------------------------------------- LOCATION DETAILS TABLE ------------------------------------------------------------

-- Start Time Logging for location details table
    start_time := clock_timestamp();

  -- Step 1: Truncate Table
    RAISE NOTICE 'Truncating location details table...';
    TRUNCATE TABLE bronze.erp_loc;

  -- Step 2: Check if CSV File Exists
    IF NOT pg_stat_file('<YOUR DATASET FOLDER LOCATION>/source_erp/LOC_A101.csv') IS NULL THEN
        RAISE NOTICE 'Bulk inserting data to customer details table...';

        -- Step 3: Bulk Insert Data
        COPY bronze.erp_loc 
        FROM '<YOUR DATASET FOLDER LOCATION>/source_erp/LOC_A101.csv'
        DELIMITER ',' CSV HEADER;

        -- Step 4: Completion Message
        RAISE NOTICE 'Data insertion completed successfully.';
    ELSE
        RAISE EXCEPTION 'CSV file not found at specified path.';
    END IF;

  -- Step 5: Duration Calculation
    end_time := clock_timestamp();
    p_duration := end_time - start_time;
    RAISE NOTICE 'Location Details load duration: % seconds', EXTRACT(EPOCH FROM p_duration);

------------------------------------------------------------- PRODUCT CATALOG TABLE ------------------------------------------------------------

    -- Step 1: Truncate Table
    RAISE NOTICE 'Truncating product catalog table...';
    TRUNCATE TABLE bronze.erp_prod_cat;

    -- Step 2: Check if CSV File Exists
    IF NOT pg_stat_file('<YOUR DATASET FOLDER LOCATION/source_erp/PX_CAT_G1V2.csv') IS NULL THEN
        RAISE NOTICE 'Bulk inserting data to product catalog table...';

        -- Step 3: Bulk Insert Data
        COPY bronze.erp_prod_cat 
        FROM '<YOUR DATASET FOLDER LOCATION/source_erp/PX_CAT_G1V2.csv'
        DELIMITER ',' CSV HEADER;

        -- Step 4: Completion Message
        RAISE NOTICE 'Data insertion completed successfully.';
    ELSE
        RAISE EXCEPTION 'CSV file not found at specified path.';
    END IF;

    -- Step 5: Duration Calculation
    end_time := clock_timestamp();
    p_duration := end_time - start_time;
    RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM p_duration);



    -- Final Commit
    --COMMIT;
	-- Batch completion time
	batch_end_time := clock_timestamp();
	b_duration = batch_end_time - batch_start_time;
	RAISE NOTICE 'Batch duration: % seconds', EXTRACT(EPOCH FROM b_duration);

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE NOTICE '❌ Data insertion failed. All changes are rolled back.';
        RAISE NOTICE 'Error occurred: %', SQLERRM;
        RAISE NOTICE 'Error code: %', SQLSTATE;
END;
$$;
