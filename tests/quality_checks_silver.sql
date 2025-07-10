-- Check for Invalid Dates
-- Expectation: No Invalid Dates
SELECT sls_ship_dt
FROM silver.crm_sales_details
WHERE sls_ship_dt IS NULL
   OR sls_ship_dt < DATE '1900-01-01'
   OR sls_ship_dt > DATE '2050-01-01';


-- Check for Invalid Date Orders
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Check Data Consistency: Between Sales, Quantity, and Price
-->>  Sales = Quantity * Price
-->> Values must not be NULL, zero, or negatitve

SELECT DISTINCT
sls_sales,
sls_quantity, 
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL 
OR sls_sales <= 0 OR sls_quantity <= 0 OR SLS_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

SELECT * FROM silver.crm_sales_details;