/*
========================================================
Stored_Procedure: Load Silver Layer (Bronze -> silver) 
========================================================
Script purpose: 
  This stored procedure performs the ETL (Extract, Transform, Load) Process to populate the silver schema tables from the beonze schema

Actions Performed:
  - Truncates Silver Tables.
  - Inserts transformed and cleanned data from Bronze into silver tbales

Parameters:
  None
  This store procedure does not accept any parameters or return any values.

Usage Example.
  Exec silver.load_silver;
*/
create or alter procedure silver.load_silver as
begin
	Declare @start_time datetime , @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time = GETDATE();
		print '======================================';
		print 'Loading silver layer' 
		print '======================================';

		print '======================================';
		print 'Loading CRM tables'
		print '======================================'

		-- loading silver.crm_cust_info

		set @start_time = getdate();
		print '>> Truncating table: silver.crm_cst_info'
		truncate table silver.crm_cst_info
		print '>> Inserting data into silver layer : silver.crm_cst_info'
		INSERT INTO silver.crm_cst_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		select
			cst_id,
			cst_key,
			TRIM(cst_firstname) as cst_firstname,
			TRIM(cst_lastname) as cst_lastname,
			case 
				when upper(trim(cst_marital_status))='M' then 'Married'
				when upper(trim(cst_marital_status))='S' then 'Single'
				else 'n/a'
			end as cst_marital_status,
			case 
				when upper(trim(cst_gndr))='M' then 'Male'
				when upper(trim(cst_gndr))='F' then 'Female'
				else 'n/a'
			end as cst_gndr,
			cst_create_date
		from(
		select *,
		row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
		from bronze.crm_cst_info
		where cst_id is not null)t
		where flag_last = 1
		set @end_time = GETDATE();
		print '>> load duration: '+ cast(Datediff(second,@start_time,@end_time) as NVARCHAR) + ' seconds';
		print '>> ---------------'

		-----------------------------------------------------------------------------------

		-- loading silver.crm_prd_info

		set @start_time = getdate();
		print '>> Truncating table: silver.crm_prd_info'
		truncate table silver.crm_prd_info
		print '>> Inserting data into silver layer : silver.crm_prd_info'
		insert into silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		select 
			prd_id,
			replace(substring(prd_key,1,5),'-','_') as cat_id,
			substring(prd_key,7,len(prd_key)) as prd_key,
			prd_nm,
			case 
				when prd_cost is null then 0
				when prd_cost is not null then prd_cost
			end prd_cost,
			case 
				when upper(trim(prd_line)) = 'M' then 'Mountain'
				when upper(trim(prd_line)) = 'R' then 'Road'
				when upper(trim(prd_line)) = 'S' then 'other Sales'
				when upper(trim(prd_line)) = 'T' then 'Touring'
				else 'n/a'
			end as prd_line,
			prd_start_dt,
			DATEADD(day, -1,
				lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)
			) as prd_end_dt
		from bronze.crm_prd_info
		set @end_time = GETDATE();
		print '>> load duration: '+ cast(Datediff(second,@start_time,@end_time) as NVARCHAR) + ' seconds';
		print '>> ---------------'
		----------------------------------------------------------------------------


		-- loading silver.crm_sales_details

		set @start_time = getdate();
		print '>> Truncating table: silver.crm_sales_details'
		truncate table silver.crm_sales_details
		print '>> Inserting data into silver layer : silver.crm_sales_details'
		insert into silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case 
				when sls_order_dt = 0 or len(sls_order_dt) != 8 then NULL
				else cast(cast(sls_order_dt as varchar) AS date) -- casting datatype from integer to date
			end as sls_order_dt,
			case 
				when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then NULL
				else cast(cast(sls_ship_dt as varchar) AS date) -- casting datatype from integer to date
			end as sls_ship_dt,
			case 
				when sls_due_dt = 0 or len(sls_due_dt) != 8 then NULL
				else cast(cast(sls_due_dt as varchar) AS date) 
			end as sls_due_dt,
			case 
				when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price) 
				then sls_quantity * ABS(sls_price) 
				else sls_sales 
			end as sls_sales, -- Recalulating sales if original value is mising or incorrect
			sls_quantity,
			case
				when sls_price is null or sls_price <=0
				then sls_sales / NULLIF(sls_quantity,0)
				else sls_price -- Derive price if original value is invalid
			end as sls_price
		from bronze.crm_sales_details;
		set @end_time = GETDATE();
		print '>> load duration: '+ cast(Datediff(second,@start_time,@end_time) as NVARCHAR) + ' seconds';
		print '>> ---------------'

		---------------------------------------------------------------------------------------------------


		print '================================='
		print 'Loading ERP tables'
		print '================================='
		-- loading silver.erp_cust_az12

		set @start_time = getdate();
		print '>> Truncating table: silver.erp_cust_az12'
		truncate table silver.erp_cust_az12
		print '>> Inserting data into silver layer : silver.erp_cust_az12'
		insert into silver.erp_cust_az12(
			cid,
			bdate,
			gen)
		select 
			case when cid like 'NAS%' THEN substring(cid,4,LEN(cid)) -- remove 'NAS' prefix if present
				else cid 
			end cid,
			case when bdate > GETDATE() then null 
				 else bdate 
			end as bdate, -- set future birthdates to NULL
			case 
				when upper(trim(gen)) = 'M' then 'Male' 
				when upper(trim(gen)) = 'F' then 'Female'
				when trim(gen) = 'Male' then gen 
				when trim(gen) = 'Female' then gen
				else 'n/a'
			end as gen -- standardize gender values and handle unknown cases
		from bronze.erp_cust_az12
		set @end_time = GETDATE();
		print '>> load duration: '+ cast(Datediff(second,@start_time,@end_time) as NVARCHAR) + ' seconds';
		print '>> ---------------'


		-----------------------------------------------------------------------

		-- load silver.erp_loc_a101

		set @start_time = GETDATE();
		print '>> Truncating table: silver.erp_loc_a101'
		truncate table silver.erp_loc_a101
		print '>> Inserting data into silver layer : silver.erp_loc_a101'
		insert into silver.erp_loc_a101(
			cid,
			cntry)
		select 
			REPLACE(cid,'-','') as cid,
			case when trim(cntry) = 'USA' then 'United States'
				 when trim(cntry) = 'US' then 'United States'
				 when trim(cntry) = 'DE' then 'Germany' 
				 when trim(cntry) is null or trim(cntry) = '' then 'n/a'
			else cntry 
			end as cntry
		from bronze.erp_loc_a101
		set @end_time = GETDATE();
		print '>> load duration: '+ cast(Datediff(second,@start_time,@end_time) as NVARCHAR) + ' seconds';
		print '>> ---------------'

		----------------------------------------------------------------------


		-- load silver.erp_px_cat_g1v2

		set @start_time = GETDATE();
		print '>> Truncating table: silver.erp_px_cat_g1v2'
		truncate table silver.erp_px_cat_g1v2
		print '>> Inserting data into silver layer : silver.erp_px_cat_g1v2'
		insert into silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance)
		 select 
			id,
			cat,
			subcat,
			maintenance
		from bronze.erp_px_cat_g1v2
		set @end_time = GETDATE();
		print '>> load duration: '+ cast(Datediff(second,@start_time,@end_time) as NVARCHAR) + ' seconds';
		print '>> ---------------'
	end try 
	begin catch
		print '=========================='
		print 'Error occured during loading bronze layer'
		print 'Error Message' +Error_message();
		print 'Error Message' +cast(Error_number() as NVARCHAR);
		print 'Error Message' + cast(Error_number() as NVARCHAR);
		print '=========================='
	END catch
end
