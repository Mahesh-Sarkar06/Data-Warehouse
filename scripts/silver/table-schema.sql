-- Create table under SILVER LAYER for CRM/cust_info file
CREATE TABLE silver.crm_cust_info (
  cust_id INT,
  cust_key VARCHAR(50),
  cust_firstname VARCHAR(50),
  cust_lastname VARCHAR(50),
  cust_marital_status VARCHAR(5),
  cust_gndr VARCHAR(5),
  cust_create_date DATE,
  dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create table under SILVER LAYER for CRM/prod_info file
CREATE TABLE silver.crm_prod_info (
  prod_id INT,
  prod_key VARCHAR(50),
  prod_name VARCHAR(50),
  prod_cost FLOAT,
  prod_line VARCHAR(5),
  prod_start_date DATE,
  prod_end_date DATE,
  dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create table under SILVER LAYER for CRM/sales_details file
CREATE TABLE silver.crm_sales_details (
  sale_ord_num VARCHAR(50),
  sale_prod_key VARCHAR(50),
  sale_cust_id INT,
  sale_ord_date INT,
  sale_ship_date INT,
  sale_due_date INT,
  sales INT,
  quantity INT,
  sale_price FLOAT,
  dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
