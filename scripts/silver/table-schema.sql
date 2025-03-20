-- Create table under SILVER LAYER for CRM/cust_info file
CREATE TABLE silver.crm_cust_info (
  cust_id INT,
  cust_key VARCHAR(50),
  cust_firstname VARCHAR(50),
  cust_lastname VARCHAR(50),
  cust_marital_status VARCHAR(10),
  cust_gndr VARCHAR(10),
  cust_create_date DATE,
  dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create table under SILVER LAYER for CRM/prod_info file
CREATE TABLE silver.crm_prod_info (
  prod_id INT,
  prod_key VARCHAR(25),
  prod_cat_id VARCHAR(10),
  prod_cat_key VARCHAR(15),
  prod_name VARCHAR(50),
  prod_cost FLOAT,
  prod_line VARCHAR(25),
  prod_start_date DATE,
  prod_end_date DATE,
  dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create table under SILVER LAYER for CRM/sales_details file
CREATE TABLE silver.crm_sales_details (
  sale_ord_num VARCHAR(25),
  sale_prod_key VARCHAR(25),
  sale_cust_id INT,
  sale_ord_date DATE,
  sale_ship_date DATE,
  sale_due_date DATE,
  sales FLOAT,
  quantity INT,
  sale_price FLOAT,
  dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Create table under SILVER LAYER for ERP/CUST_AZ12 file
CREATE TABLE silver.erp_cust (
  cid VARCHAR(25),
  dob DATE,
  sex VARCHAR(10),
  dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create table under SILVER LAYER for ERP/LOC_A101 file
CREATE TABLE silver.erp_loc (
  cid VARCHAR(25),
  country VARCHAR(50),
  dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create table under SILVER LAYER for ERP/PX_CAT_G1V2 file
CREATE TABLE silver.erp_prod_cat (
  id VARCHAR(25),
  category VARCHAR(50),
  sub_category VARCHAR(50),
  maintenance VARCHAR(5),
  dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
