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

-- SELECT * FROM bronze.erp_cust_az12 
-- WHERE cid LIKE '%AW00011000%';