/*
===============================================
Script purpose: 
  This script contain to analyzing each and every column from the table to find out the 
  nulls, inconsistent data, invalid date etc
For each output was mention below the each of the query 
===============================================
*/

/*
========================================
Analyzing brinze.crm_cst_info table
========================================
*/
SELECT * FROM bronze.crm_cst_info;

-- 1) Check For nulls in primary key

SELECT cst_id , count(*) as total_count
from bronze.crm_cst_info
group by cst_id
having count(*) >1 or cst_id is null;

-- output: Having some id repeated and null present 

-- 2) checking space in cst_first_name

select cst_firstname from 
bronze.crm_cst_info
where cst_firstname!=TRIM(cst_firstname)

-- output: Having extra space in cst_firstname column

-- 3) checking space in cst_lastname

select cst_lastname from 
bronze.crm_cst_info
where cst_lastname!=TRIM(cst_lastname)

-- output: Having extra space in cst_lastname column

-- 4) checking cst_marital_status

select cst_marital_status , count(*) as total_count 
from bronze.crm_cst_info
group by cst_marital_status;

-- output: HAVE NULL in cst_marital_status

-- 5) checking cst_cst_gndr

select cst_gndr , count(*) as total_count
from bronze.crm_cst_info 
group by cst_gndr

-- output : Have more 4577 null values in cst_cst_gndr column 

-- 6) checking the frequence of the date is present and is there any null values are there or not

select cst_create_date , count(*) as total_count
from bronze.crm_cst_info
group by cst_create_date 
having count(*) > 1 or cst_create_date is null;

-- Output: some dates are repeated 

-------------------------------------------------------------------------------------------------------

/*
========================================
Analyzing brinze.crm_prd_info table
========================================
*/

 select * from bronze.crm_prd_info;

-- 1) checking weather the prd_id is repeated or having null values in prd_id

select prd_id , count(*) as total_count
from bronze.crm_prd_info
group by prd_id
having   count(*) > 1 or prd_id is null;

-- output: There is no repeating prd_id and there is no null values 

-- 2) analysing prd_key column 

select prd_key, count(*) as total_count
from bronze.crm_prd_info
group by prd_key

-- output : no null values present 

-- 3) analysing prd_nm

select prd_nm , count(*) as total_count
from bronze.crm_prd_info 
group by prd_nm 

-- output: no null values present

-- 4) analysing prd_cost

select prd_cost 
from bronze.crm_prd_info 
where prd_cost< 0 or prd_cost is null;

-- output: no negative values present and 2 nulls present in prd_cost column 


-- 5) analysing prd_line

select distinct prd_line 
from bronze.crm_prd_info;

-- output: Null values are present in the prd_line

-- 6) check for invalid date orders

select *  from bronze.crm_prd_info
where prd_end_dt < prd_start_dt;

-- output: total 200 rows are invalid date orders

----------------------------------------------------------------

/*
========================================
Analyzing brinze.crm_sales_details table
========================================
*/

select * from bronze.crm_sales_details;

-- 1) analyzing sls_order_num

	select sls_ord_num 
	from bronze.crm_sales_details 
	where sls_ord_num is null;

-- output: No nulls are present in the sls_order_num

-- 2) analyzing sls_prd_key

	select sls_prd_key 
	from bronze.crm_sales_details 
	where sls_prd_key is null;

-- output: No nulls are present in the sls_prd_key 

-- 3) analyzing sls_cust_id
	 
	select sls_cust_id 
	from bronze.crm_sales_details 
	where sls_cust_id is null;

-- output: No nulls are present in the sls_prd_key 

-- 4) checking order date column 
	
	select sls_order_dt
	from  bronze.crm_sales_details

-- output:  the datatype of the column is in string we have to convert into date 

-- 5) analyzing sls_ship_dt

	select sls_ship_dt
	from  bronze.crm_sales_details

-- output:  the datatype of the column is in integer we have to convert into date 

-- 6) analyzing sls_due_dt

	select sls_ship_dt
	from  bronze.crm_sales_details

-- output:  the datatype of the column is in integer we have to convert into date 

-- 7) analyzing sls_sales

	select sls_sales
	from bronze.crm_sales_details 
	where sls_sales is null or sls_sales < 0;

-- output: There are some null values present and some negative values

-- 8) analyzing sls_quantity

	select sls_quantity 
	from bronze.crm_sales_details 
	where sls_quantity is null or sls_quantity < 0; 

-- output: No null values and no negative values

-- 9) analyzing sls_price

	select sls_price
	from bronze.crm_sales_details 
	where sls_price is null or sls_price < 0;

-- output: some null values are present and some negative values present 

------------------------------------------------------------------------------

/*
========================================
Analyzing brinze.erp_cust_az12 table
========================================
*/

select * from bronze.erp_cust_az12

-- 1) Analyzing cid column 
	
	select cid 
	from bronze.erp_cust_az12
	where cid is null;

-- output: There is no null values but we need to extract last 5 string to join crm tables

-- 2) Analyzing bdate column

	select bdate 
	from bronze.erp_cust_az12
	where bdate is null or bdate < '1924-01-01' or bdate > GETDATE()

-- output: There is no null values but there are some invalid date in this column.

-- 3) Analyzing gen column 

	select gen, count(*) as total_count
	from bronze.erp_cust_az12 
	group by gen;

-- output: There are some values M AND F which is Male and female we have to change it and have some null values have to handle it

------------------------------------------------------------------------------------------

/*
========================================
Analyzing brinze.erp_loc_s101 table
========================================
*/

select * from bronze.erp_loc_s101

-- 1) Analyzing cid column 

	select cid 
	from bronze.erp_loc_a101
	where cid is null;

-- output : no null values and need to use this column to join other tables 

-- 2) Analyzing cntry

	select cntry , count(*) as total_count
	from bronze.erp_loc_a101
	group by cntry;

-- output: DE and germany are same and USA and united states are same and have some null values need to handle that as well



----------------------------------------------------------------------------------------------------

/*
======================================
Analyzing bronze.erp_px_cat_g1v2 table
======================================
*/ 

-- 1) Analyzing id column 

	select id
	from bronze.erp_px_cat_g1v2
	where id is null; 

-- output : No null values are present

-- 2) Analyzing cat column 

	select cat , count(*) as total_count
	from bronze.erp_px_cat_g1v2
	group by cat; 

-- output: No null values are present 

-- 3) Analyzing subcat column 

	select subcat , count(*) as total_count
	from bronze.erp_px_cat_g1v2
	group by subcat;

-- output: No null values are present 

-- 4) Analyzing maintenance column 

	select maintenance , count(*) as total_count
	from bronze.erp_px_cat_g1v2
	group by maintenance;

-- output: No null values are present 

-------------------------------------------------------------------------------------- 
