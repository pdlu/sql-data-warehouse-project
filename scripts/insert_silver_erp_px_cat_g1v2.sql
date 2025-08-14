INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
SELECT id, cat, subcat, maintenance 
FROM bronze.erp_px_cat_g1v2;


-- Check for unwanted spaces
-- SELECT * FROM bronze.erp_px_cat_g1v2
-- WHERE cat != TRIM(cat) OR subcat != TRIM(cat) OR maintenance != TRIM(maintenance);

-- Check standardization and constistency
-- SELECT DISTINCT
-- maintenance
-- FROM bronze.erp_px_cat_g1v2;