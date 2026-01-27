/* 
Store Procedure: Load Bronze Layer ( Source -> Bronze)

Script Purpose:
  - This store procedure loads data into the Bronze schema from external CSV files.
  - It performs the bronze tables before loading data.
    - Truncates the bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from CSV Files to bronze tables.

Parameters:
  None:
      This Store Procedure does not accept any parameters or return any values.

Usage Example:
      EXEC bronze.load_bronze;
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @BATCH_START_TIME DATETIME, @BATCH_END_TIME DATETIME;
BEGIN TRY
	SET @BATCH_START_TIME = GETDATE();
	PRINT '============================'
	PRINT 'Loading Bronze Layer'
	PRINT '============================'

	PRINT '----------------------------'
	PRINT 'Loading CRM Tables'
	PRINT '----------------------------'


	SET @start_time = GETDATE(); 
	PRINT '>> Truncating Table: bronze.crm_cst_info'; 
	Truncate table bronze.crm_cst_info;

	PRINT '>> Inserting Data into Table: bronze.crm_cst_info';
	Bulk insert bronze.crm_cst_info 
	from 'C:\Users\pnave\OneDrive\Desktop\SQL_PROJECT\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	with (
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
	);
	SET @end_time = GETDATE()
	PRINT '>> Load Duration: '+ cast(datediff(Second, @start_time,@end_time) as NVARCHAR) + 'Seconds'
	PRINT '>> ----------------'

	SET @start_time = GETDATE(); 
	PRINT '>> Truncating Table: bronze.crm_prd_info';
	Truncate table bronze.crm_prd_info;

	PRINT '>> Inserting Data into Table: bronze.crm_prd_info';
	Bulk insert bronze.crm_prd_info 
	from 'C:\Users\pnave\OneDrive\Desktop\SQL_PROJECT\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	with (
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
	);
	SET @end_time = GETDATE()
	PRINT '>> Load Duration: '+ cast(datediff(Second, @start_time,@end_time) as NVARCHAR) + 'Seconds'
	PRINT '>> ----------------'


	SET @start_time = GETDATE(); 
	PRINT '>> Truncating Table: bronze.crm_sales_details';
	Truncate table bronze.crm_sales_details;

	PRINT '>> Inserting Data into Table: bronze.crm_sales_details';
	Bulk insert bronze.crm_sales_details 
	from 'C:\Users\pnave\OneDrive\Desktop\SQL_PROJECT\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	with (
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
	);
	SET @end_time = GETDATE()
	PRINT '>> Load Duration: '+ cast(datediff(Second, @start_time,@end_time) as NVARCHAR) + 'Seconds'
	PRINT '>> ----------------'


	PRINT '----------------------------'
	PRINT 'Loading ERP Tables'
	PRINT '----------------------------'

	SET @start_time = GETDATE(); 
	PRINT '>> Truncating Table: bronze.erp_cust_az12';
	Truncate table bronze.erp_cust_az12;

	PRINT '>> Inserting Data into Table: bronze.erp_cust_az12';
	Bulk insert bronze.erp_cust_az12 
	from 'C:\Users\pnave\OneDrive\Desktop\SQL_PROJECT\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	with (
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
	);
	SET @end_time = GETDATE()
	PRINT '>> Load Duration: '+ cast(datediff(Second, @start_time,@end_time) as NVARCHAR) + 'Seconds'
	PRINT '>> ----------------'

	SET @start_time = GETDATE(); 
	PRINT '>> Truncating Table: bronze.erp_loc_a101';
	Truncate table bronze.erp_loc_a101;

	PRINT '>> Inserting Data into Table: bronze.erp_loc_a101';
	Bulk insert bronze.erp_loc_a101 
	from 'C:\Users\pnave\OneDrive\Desktop\SQL_PROJECT\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	with (
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
	);
	SET @end_time = GETDATE()
	PRINT '>> Load Duration: '+ cast(datediff(Second, @start_time,@end_time) as NVARCHAR) + 'Seconds'
	PRINT '>> ----------------'
	
	SET @start_time = GETDATE(); 
	PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
	Truncate table bronze.erp_px_cat_g1v2;

	PRINT '>> Inserting Data into Table: bronze.erp_px_cat_g1v2';
	Bulk insert bronze.erp_px_cat_g1v2 
	from 'C:\Users\pnave\OneDrive\Desktop\SQL_PROJECT\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	with (
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
	);
	SET @end_time = GETDATE()
	PRINT '>> Load Duration: '+ cast(datediff(Second, @start_time,@end_time) as NVARCHAR) + 'Seconds';
	PRINT '>> ----------------'

	SET @BATCH_END_TIME=GETDATE();
	PRINT '=========================================='
	PRINT 'Loading Bronze Layer is Completed';
	PRINT '  - Total Load Duration: ' + cast(datediff(second,@BATCH_START_TIME,@BATCH_END_TIME) as NVARCHAR) + 'Seconds';
	PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '======================================'
		PRINT 'Error occured during loading bronze layer'
		PRINT 'Error Message' + Error_message()
		PRINT 'Error Message' + cast (ERROR_NUMBER() as NVARCHAR);
		PRINT 'Error Message' + cast (ERROR_STATE() AS NVARCHAR);
		PRINT '======================================'
	END CATCH
END 


