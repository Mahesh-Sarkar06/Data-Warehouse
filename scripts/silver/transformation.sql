-- Prefer this file only when you want the changes to reflect in bronze layer tables also.
-- Checking bronze.crm_cust_info have any wrong values.
-- Starting from cust_id as this is the PRIMARY KEY column and cannot have duplicate or NULL values.
SELECT cust_id, COUNT(*) AS total_count
FROM bronze.crm_cust_info
GROUP BY cust_id
HAVING COUNT(*) > 1 OR cust_id IS NULL;

-- Now we have to delete the duplicate old and NULL values from the table.
-- So, for this we will use CTE and ROW_NUMBER concept for ranking the rows as per the record created.
-- Before that we have to check the data how many are there which we need to delete
SELECT * FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY cust_id ORDER BY cust_create_date DESC) AS flag_rank
FROM bronze.crm_cust_info
) lat
WHERE flag_rank <> 1;

-- Now we will delete old duplicate data
WITH flag_table AS (
SELECT *, ROW_NUMBER() OVER(PARTITION BY cust_id ORDER BY cust_create_date DESC) AS flag_rank
FROM bronze.crm_cust_info
)
DELETE FROM bronze.crm_cust_info
WHERE cust_id IN (
SELECT cust_id FROM flag_table
WHERE flag_rank <> 1
);

-- Now deleting NULL cust_id values
DELETE FROM bronze.crm_cust_info
WHERE cust_id IS NULL;


-- NOTE: We are using DELETE command instead of TRUNCATE because it executes over ROWS and obeys the TRIGGER conditions if applied.
