/*
===================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===================================================================================

Script Purpose:
    This stored procedure loads data into 'Bronze' schema from eternal scv files.
    It Performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the 'COPY' command to load data from the csv files to bronze tables.
    - Calculate the time taken by each tables.
    - Calculate whole time of the Batch time to load.

Parameters:
    None,
    This stored procedure does not accept any parameters or return any values.

How to use:
    CALL bronze.load_bronze();

===================================================================================

*/

CALL bronze.load_bronze();

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $BODY$
DECLARE -- Defining the time variables here
	rows_count INTEGER;
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	interval_diff INTERVAL;
	hours INTEGER;
	minutes INTEGER;
	seconds INTEGER;
	milliseconds INTEGER;
	batch_start_time TIMESTAMP;
	batch_end_time TIMESTAMP;
BEGIN

	RAISE NOTICE '==================================================';
	RAISE NOTICE '=========== LOADING BRONZE LAYER =================';
	RAISE NOTICE '==================================================';
	RAISE NOTICE '';
	
	RAISE NOTICE 'Starting bronze.load_bronze procedure';
	RAISE NOTICE '';
	
	RAISE NOTICE '---------------------------------------------------';
	RAISE NOTICE '------------- Loading CRM Tables ------------------';
	RAISE NOTICE '---------------------------------------------------';
	RAISE NOTICE '';
	
	--------------------------------------------------------------------------------------------------------
	
	batch_start_time := NOW();
	
	--Truncate the bronze.crm_cust_info table and import the csv file.
	
	start_time := NOW();
	TRUNCATE TABLE bronze.crm_cust_info;
	COPY bronze.crm_cust_info	
	FROM '/Users/phuclu/Documents/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
	DELIMITER ','
	CSV HEADER;
	
	GET DIAGNOSTICS rows_count = ROW_COUNT;
	RAISE NOTICE 'crm_cust_info: % rows affected', row_count;
	
	-- TIME
	end_time := NOW();
	interval_diff := end_time - start_time;
	hours := EXTRACT(HOUR FROM interval_diff);
	minutes := EXTRACT(MINUTE FROM interval_diff);
	seconds := EXTRACT(SECOND FROM interval_diff)::INTEGER;
	milliseconds := EXTRACT(MILLISECOND FROM interval_diff)::INTEGER % 1000;
	
	RAISE NOTICE 'Load Duration: % hours, %minutes, %seconds, %milliseconds', hours, minutes, seconds, milliseconds;
	RAISE NOTICE '';
	
	--------------------------------------------------------------------------------------------------------
	
	
	--------------------------------------------------------------------------------------------------------
		
	-- Truncate the bronze.crm_prd_info table and then Import the csv file.
	
	start_time := NOW();
	TRUNCATE TABLE bronze.crm_prd_info;
	COPY bronze.crm_prd_info
	FROM '/Users/phuclu/Documents/DataWarehouse Project/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
	DELIMITER ','
	CSV HEADER;
	
	GET DIAGNOSTICS rows_count = ROW_COUNT;
	RAISE NOTICE 'crm_prd_info: % rows affected', row_count;
	
	-- TIME
	end_time := NOW();
	interval_diff := end_time - start_time;
	hours := EXTRACT(HOUR FROM interval_diff);
	minutes := EXTRACT(MINUTE FROM interval_diff);
	seconds := EXTRACT(SECOND FROM interval_diff)::INTEGER;
	milliseconds := EXTRACT(MILLISECONDS FROM interval_diff)::INTEGER % 1000;
	RAISE NOTICE 'Load Duration: % hours, %minutes, %seconds, %milliseconds', hours, minutes, seconds, milliseconds;
	RAISE NOTICE '';
	
	--------------------------------------------------------------------------------------------------------
	
	
	--------------------------------------------------------------------------------------------------------
	
	-- Truncate bronze.crm_sales_details table and then import the csv file.
	
	start_time := NOW();
	TRUNCATE TABLE bronze.crm_sales_details;
	COPY bronze.crm_sales_details
	FROM '/Users/phuclu/Documents/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
	DELIMITER ','
	CSV HEADER;
	
	GET DIAGNOSTICS rows_count = ROW_COUNT;
	RAISE NOTICE 'crm_sales_details: % rows affected', rows_count;
	
	-- TIME
	end_time := NOW();
	interval_diff := end_time - start_time;
	hours := EXTRACT(HOUR FROM interval_diff);
	minutes := EXTRACT(MINUTE FROM interval_diff);
	seconds := EXTRACT(SECOND FROM interval_diff)::INTEGER;
	milliseconds := EXTRACT(MILLISECONDS FROM interval_diff)::INTEGER % 1000;
	RAISE NOTICE 'Load Duration: %hours, %minutes, %seconds, %milliseconds', hours, minutes, seconds, milliseconds;
	RAISE NOTICE '';
	
	--------------------------------------------------------------------------------------------------------
	
	RAISE NOTICE '---------------------------------------------------';
	RAISE NOTICE '------------- Loading ERP Tables ------------------';
	RAISE NOTICE '---------------------------------------------------';
	RAISE NOTICE '';
	
	--------------------------------------------------------------------------------------------------------
	
	start_time := NOW(); -- START THE TIMER
	
	TRUNCATE TABLE bronze.erp_cust_az12;
	COPY bronze.erp_cust_az12
	FROM '/Users/phuclu/Documents/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
	DELIMITER ','
	CSV HEADER;
	
	GET DIAGNOSTICS rows_count = ROW_COUNT;
	RAISE NOTICE 'erp_cust_az12: % rows affected', rows_count;
	
	-- TIME
	end_time := NOW();
	interval_diff := end_time - start_time;
	hours := EXTRACT(HOUR FROM interval_diff);
	minutes := EXTRACT(MINUTE FROM interval_diff);
	seconds := EXTRACT(SECOND FROM interval_diff)::INTEGER;
	milliseconds := EXTRACT(MILLISECONDS FROM interval_diff)::INTEGER % 1000;
	RAISE NOTICE 'Load Duration: %hours, %minutes, %seconds, %milliseconds', hours, minutes, seconds, milliseconds;
	RAISE NOTICE '';
	
	--------------------------------------------------------------------------------------------------------
	
	start_time := NOW(); -- START THE TIMER
	
	TRUNCATE TABLE bronze.erp_loc_a101;
	COPY bronze.erp_loc_a101
	FROM '/Users/phuclu/Documents/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
	DELIMITER ','
	CSV HEADER;
	
	-- TIME
	end_time := NOW();
	interval_diff := end_time - start_time;
	hours := EXTRACT(HOUR FROM interval_diff);
	minutes := EXTRACT(MINUTE FROM interval_diff);
	seconds := EXTRACT(SECOND FROM interval_diff)::INTEGER;
	milliseconds := EXTRACT(MILLISECONDS FROM interval_diff)::INTEGER % 1000;
	RAISE NOTICE 'Load Duration: %hours, %minutes, %seconds, %milliseconds', hours, minutes, seconds, milliseconds;
	RAISE NOTICE '';
	
	--------------------------------------------------------------------------------------------------------
	
	start_time := NOW(); -- START THE TIMER
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	COPY bronze.erp_px_Cat_g1v2
	FROM '/Users/phuclu/Documents/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
	DELIMITER ','
	CSV HEADER;
	
	-- TIME
	end_time := NOW();
	interval_diff := end_time_start_time;
	hours := EXTRACT(HOUR FROM interval_diff);
	minutes := EXTRACT(MINUTE FROM interval_diff);
	seconds := EXTRACT(SECOND FROM interval_diff)::INTEGER;
	milliseconds := EXTRACT(MILLISECONDS FROM interval_diff)::INTEGER % 1000;
	RAISE NOTICE 'Load DUration: %hour, %minutes, %seconds %milliseconds', hours, minutes, seconds, milliseconds;
	RAISE NOTICE'';
	
	
	-- Error Handling Logic
		EXCEPTION
		    WHEN OTHERS THEN
		        DECLARE
		            v_message TEXT;
		            v_detail  TEXT;
		            v_hint    TEXT;
		        BEGIN
		            GET STACKED DIAGNOSTICS 
		                v_message = MESSAGE_TEXT,
		                v_detail  = PG_EXCEPTION_DETAIL,
		                v_hint    = PG_EXCEPTION_HINT;
		
		            RAISE NOTICE '----------------------------------------------------';
		            RAISE NOTICE '---- ERROR OCCURED DURING LOADING BRONZE LAYER -----';
		            RAISE NOTICE 'Error Message: %', v_message;
		            RAISE NOTICE 'Error Detail: %', COALESCE(v_detail, 'N/A');
		            RAISE NOTICE 'Error Hint: %', COALESCE(v_hint, 'N/A');
		            RAISE NOTICE '----------------------------------------------------';
	
		            ROLLBACK;	-- Rollback transaction
	        	END;

END;
$BODY$;



