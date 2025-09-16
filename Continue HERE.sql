 -- Foreign Key Integrity (Dimensions)
-- SELECT * FROM gold.fact_sales AS f
-- LEFT JOIN gold.dim_customers AS c
-- ON c.customer_key = f.customer_key
-- LEFT JOIN gold.dim_products AS p
-- ON p.product_key = f.product_key
-- WHERE p.product_key IS NULL;

SELECT * FROM gold.fact_sales LIMIT 1;
