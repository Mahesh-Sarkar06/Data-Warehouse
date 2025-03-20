/*
*/

CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER(ORDER BY cust_id) AS customer_key,
	ci.cust_id AS customer_id,
	ci.cust_key AS customer_number,
	ci.cust_firstname AS first_name,
	ci.cust_lastname AS last_name,
	CASE WHEN ci.cust_gndr <> 'N/A' THEN ci.cust_gndr
	ELSE COALESCE(cs.sex, 'N/A')
	END gender,
	loc.country,
	cs.dob,
	ci.cust_marital_status AS marital_status,
	ci.cust_create_date AS creation_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust cs
ON ci.cust_key = cs.cid
LEFT JOIN silver.erp_loc loc
ON cs.cid = loc.cid



CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER(ORDER BY pi.prod_start_date, pi.prod_cat_key) AS product_key,
	pi.prod_id AS product_id,
	pi.prod_cat_key AS product_number,
	pi.prod_name AS product_name,
	pi.prod_cat_id AS category_id,
	pc.category,
	pc.sub_category,
	pi.prod_cost AS cost,
	pi.prod_line AS product_line,
	pc.maintenance,
	pi.prod_start_date AS start_date
FROM silver.crm_prod_info pi
LEFT JOIN silver.erp_prod_cat pc
ON pi.prod_cat_id = pc.id
WHERE pi.prod_end_date IS NULL;		-- Filtered out historical data



CREATE VIEW gold.fact_sales AS
SELECT
	sd.sales_ord_num AS order_number,
	dp.product_key,
	dc.customer_key,
	sd.sales_ord_date AS order_date,
	sd.sales_ship_date AS shipping_date,
	sd.sales_due_date AS due_date,
	sd.sales,
	sd.quantity,
	sd.sales_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products dp
ON sd.sales_prod_key = dp.product_number
LEFT JOIN gold.dim_customers dc
ON sd.sales_cust_id = dc.customer_id
