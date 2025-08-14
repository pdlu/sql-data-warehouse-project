RAISE NOTICE 'Truncating table: silver.crm_cust_info';
TRUNCATE TABLE silver.crm_cust_info
RAISE NOTICE 'Inserting data into: silver.crm_cust_info';

INSERT INTO  silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)

SELECT 
cst_id, 
cst_key,
TRIM(cst_firstname),
TRIM(cst_lastname),
CASE 
	WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	ELSE 'N/A'
END,
CASE 
	WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'N/A'
END,
cst_create_date

FROM (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info) 

WHERE flag_last = 1;

----- INSERTING silver.crm_sales_details

RAISE NOTICE 'Truncating table: silver.crm_sales_details';
TRUNCATE TABLE silver.crm_sales_details
RAISE NOTICE 'Inserting data into: silver.crm_sales_details';

INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_price,
	sls_quantity
)

SELECT sls_ord_num, sls_prd_key, sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
    ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END,

CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
    ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END,

CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
    ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END,

CASE 
	WHEN sls_sales IS NULL 
		OR sls_sales <= 0 
		OR sls_sales != sls_quantity * ABS(sls_price) 
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END,

sls_quantity,

CASE 
	WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity, 0)
	ELSE sls_price
END
FROM bronze.crm_sales_details

--- INSERTING silver.crm_prd_info
RAISE NOTICE 'Truncating table: silver.crm_prd_info';
TRUNCATE TABLE silver.crm_prd_info
RAISE NOTICE 'Inserting data into: silver.crm_prd_info';

INSERT INTO silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
    )
SELECT 
    prd_id, 
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key,7, LENGTH(prd_key)) AS prd_key,
    prd_nm, 
    COALESCE(prd_cost, 0) AS prd_cost, 
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'M' THEN 'Touring'
        ELSE 'N/A'
        END AS prd_line,
    prd_start_dt, 
    LEAD(prd_start_dt) OVER (
        PARTITION BY prd_key 
        ORDER BY prd_start_dt)-1
        AS prd_end_dt
FROM bronze.crm_prd_info;

--- INSERTING silver.erp_cust_az12
RAISE NOTICE 'Truncating table: silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_cust_az12
RAISE NOTICE 'Inserting data into: silver.erp_cust_az12';

INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
	ELSE cid
	END,
CASE WHEN bdate > NOW() THEN NULL
	ELSE bdate
	END,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
	 ELSE 'NA'
	 END
FROM bronze.erp_cust_az12
;

--- INSERTING silver.erp_loc_a101
RAISE NOTICE 'Truncating table: silver.erp_loc_a101';
TRUNCATE TABLE silver.erp_loc_a101
RAISE NOTICE 'Inserting data into: silver.erp_loc_a101';

INSERT INTO silver.erp_loc_a101 (cid, cntry)
SELECT REPLACE (cid, '-', '') AS cid, 
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US', 'USA', 'United States') THEN 'United States' 
     WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'NA'
     ELSE TRIM(cntry)
     END AS cntry
FROM bronze.erp_loc_a101;

--- INSERTING silver.erp_px_cat_g1v2
RAISE NOTICE 'Truncating table: silver.erp_px_cat_g1v2';
TRUNCATE TABLE silver.erp_px_cat_g1v2
RAISE NOTICE 'Inserting data into: silver.erp_px_cat_g1v2';

INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
SELECT id, cat, subcat, maintenance 
FROM bronze.erp_px_cat_g1v2;
