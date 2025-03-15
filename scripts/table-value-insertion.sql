/*
========================================================================================================================================================================
                                                                  CREATING TABLES & INSERTING VALUES
========================================================================================================================================================================

NOTE: INSERT command is for bash command terminal
*/

-- Create table under BRONZE LAYER for CRM/cust_info file
CREATE TABLE bronze.crm_cust_info (
  cust_id INT,
  cust_key VARCHAR(50),
  cust_firstname VARCHAR(50),
  cust_lastname VARCHAR(50),
  cust_marital_status VARCHAR(5),
  cust_gndr VARCHAR(5),
  cust_create_date DATE
);

-- Create table under BRONZE LAYER for CRM/prod_info file
CREATE TABLE bronze.crm_prod_info (
  prod_id INT,
  prod_key VARCHAR(50),
  prod_name VARCHAR(50),
  prod_cost FLOAT,
  prod_line VARCHAR(5),
  prod_start_date DATE,
  prod_end_date DATE
);

-- Create table under BRONZE LAYER for CRM/sales_details file
CREATE TABLE bronze.crm_sales_details (
  sale_ord_num VARCHAR(50),
  sale_prod_key VARCHAR(50),
  sale_cust_id INT,
  sale_ord_date INT,
  sale_ship_date INT,
  sale_due_date INT,
  sales INT,
  quantity INT,
  sale_price FLOAT
);



-- BULK INSERTING values to crm.cust_info
\COPY bronze.crm_cust_info FROM '<ENTER THE LOCATION OF cust_info FILE>' DELIMITER ',' CSV HEADER;

SELECT * FROM bronze.crm_cust_info;

-- BULK INSERTING values to crm_prod_info
\COPY bronze.crm_prod_info FROM '<ENTER THE LOCATION OF prod_info FILE>' DELIMITER ',' CSV HEADER;

SELECT * FROM bronze.crm_prod_info;

-- BULK INSERTING values to crm_sales_details
\COPY bronze.crm_sales_details FROM '<ENTER THE LOCATION OF sales_details FILE>' DELIMITER ',' CSV HEADER;

SELECT * FROM bronze.crm_sales_details



-- Create table under BRONZE LAYER for ERP/CUST_AZ12 file
CREATE TABLE bronze.erp_cust (
  cid VARCHAR(25),
  dob DATE,
  sex VARCHAR(10)
);

-- Create table under BRONZE LAYER for ERP/LOC_A101 file
CREATE TABLE bronze.erp_loc (
  cid VARCHAR(25),
  country VARCHAR(50)
);

-- Create table under BRONZE LAYER for ERP/PX_CAT_G1V2 file
CREATE TABLE bronze.erp_prod_cat (
  id VARCHAR(25),
  category VARCHAR(50),
  sub_category VARCHAR(50),
  maintenance VARCHAR(5)
);



-- BULK INSERTING values to erp_cust
\COPY bronze.erp_cust FROM '<ENTER THE LOCATION OF cust_az12 FILE>' DELIMITER ',' CSV HEADER;

SELECT * FROM bronze.erp_cust;

-- BULK INSERTING values to erp_loc
\COPY bronze.erp_loc FROM '<ENTER THE LOCATION OF loc_a101 FILE>' DELIMITER ',' CSV HEADER;

SELECT * FROM bronze.erp_loc;

-- BULK INSERTING values to erp_prod_cat
\COPY bronze.erp_prod_cat FROM '<ENTER THE LOCATION OF px_cat_g1v2 FILE>' DELIMITER ',' CSV HEADER;

SELECT * FROM bronze.erp_prod_cat;
