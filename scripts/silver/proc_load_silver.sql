/*
usage Example:
exec silver.load_silver;
*/

create or alter procedure silver.load_silver as 
begin
	declare @start_time datetime ,	@end_time datetime ,@batch_start_time datetime , @batch_end_time datetime;
	begin try
		set	@batch_start_time = getdate();



print '================================================'
print 'Loading Silver Layer'
print '================================================'


print '================================================'
print 'Loading CRM Tables'
print '================================================'




set @start_time = getdate ()
print '>> Truncating Table: silver.crm_cust_info'
truncate table silver.crm_cust_info;
print '>> inserting Data into : silver.crm_cust_info'


INSERT INTO silver.crm_cust_info (
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
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
CASE
	WHEN UPPER(TRIM(cst_marital_status)) = 'm' THEN 'Married'
	WHEN UPPER(TRIM(cst_marital_status)) = 's' THEN 'Single'
	ELSE 'N/A'
END AS cst_marital_status,
CASE
	WHEN UPPER(TRIM(cst_gndr)) = 'f' THEN 'Female'
	WHEN UPPER(TRIM(cst_gndr)) = 'm' THEN 'Male'
	ELSE 'N/A'
END AS cst_gndr,
cst_create_date
FROM (
SELECT *,
ROW_NUMBER() OVER ( PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id is NOT NULL
)
t WHERE flag_last = 1

set @end_time = getdate ();
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time ) as nvarchar) + ' Seconds';	
print '>> ---------------------';




		
set @start_time = getdate ();
print '>> Truncating Table: silver.crm_prd_info'
truncate table silver.crm_prd_info;
print '>> inserting Data into : silver.crm_prd_info'

insert into silver.crm_prd_info (
prd_id,
cat_id ,
prd_key,
prd_nm ,
prd_cost ,
prd_line ,
prd_start_dt ,
prd_end_dt 
)
select 
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5) , '-' , '_' ) AS cat_id,
SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
prd_nm,
isnull(prd_cost,0) AS prd_cost,
case upper(trim(prd_line))
	when 'r' then 'Road'
	when 's' then 'Other Sales'
	when 'm' then 'Mountain'
	when 't' then 'Touring'
	else 'n/a'
end as prd_line,
cast(prd_start_dt as date) as prd_start_date,
cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) -1 as date) as prd_end_dt
from bronze.crm_prd_info

set @end_time = getdate ();
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time ) as nvarchar) + 'Seconds'
print '>> ---------------------';






set @start_time = getdate ();
print '>> Truncating Table: silver.crm_sales_details'
truncate table silver.crm_sales_details;
print '>> inserting Data into : silver.crm_sales_details'

insert into silver.crm_sales_details(
sls_ord_num ,
sls_prd_key ,
sls_cust_id ,
sls_order_dt ,
sls_ship_dt ,
sls_due_dt ,
sls_sales ,
sls_quantity,
sls_price
)
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
	else cast(cast(sls_order_dt as Varchar) as date)
	end as sls_order_dt,  
case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
	else cast(cast(sls_ship_dt as Varchar) as date)
	end as sls_ship_dt,
case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
	else cast(cast(sls_due_dt as Varchar) as date)
	end as sls_due_dt,
case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
		then sls_quantity * abs(sls_price)
	else sls_sales
end as sls_sales,
sls_quantity,
case when sls_price <= 0 or sls_price is null
	THEN sls_sales / nullif (sls_quantity,0)
	else sls_price
end as sls_price
FROM bronze.crm_sales_details

set @end_time = getdate ();
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time ) as nvarchar) + 'Seconds'
print '>> ---------------------';




print '================================================'
print 'Loading ERP Tables'
print '================================================'



set @start_time = getdate ();
print '>> Truncating Table: silver.erp_cust_az12'
truncate table silver.erp_cust_az12;
print '>> inserting Data into : silver.erp_cust_az12'


insert into silver.erp_cust_az12(
cid,
bdate,
gen
)
select
case when cid like 'NAS%' then substring (cid , 4 , len(cid))
	ELSE cid
end as cid,
case when bdate > getdate() then null
else bdate
end as bdate,
case 
	when upper(trim(gen)) in ( 'F' , 'Female') then 'Female'
	when upper(trim(gen)) in ( 'm' , 'male') then 'Male'
	else 'n/a'
end as gen
from bronze.erp_cust_az12

set @end_time = getdate ();
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time ) as nvarchar) + 'Seconds'
print '>> ---------------------';






set @start_time = getdate ();
print '>> Truncating Table: silver.erp_loc_a101'
truncate table silver.erp_loc_a101;
print '>> inserting Data into : silver.erp_loc_a101'

insert into silver.erp_loc_a101(
cid,
cntry
)
  select 
  replace (cid , '-' , '' ) as cid,
  case when upper(trim(cntry)) = 'de' then 'Germany'
       when upper(trim(cntry)) in ('us','usa') then 'United States'
       when upper(trim(cntry)) = ''  or cntry is null then 'n/a'
       else cntry   
end as cntry
  from bronze.erp_loc_a101

set @end_time = getdate ();
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time ) as nvarchar) + 'Seconds'
print '>> ---------------------';




set @start_time = getdate ();
print '>> Truncating Table: silver.erp_px_cat_g1v2'
truncate table silver.erp_px_cat_g1v2;
print '>> inserting Data into : silver.erp_px_cat_g1v2'

insert into silver.erp_px_cat_g1v2
(id,cat,subcat,maintenance)
select
id,
cat,
subcat,
maintenance
from
bronze.erp_px_cat_g1v2

set @end_time = getdate ();
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time ) as nvarchar) + 'Seconds'
print '>> ---------------------';

set @batch_end_time = getdate();
print '================================================'
print 'Loading Silver Layer is Completed'
print 'Total Load Duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time ) as nvarchar ) + 'Seconds'
print '================================================'


end try
	begin catch
		print '================================================'
		print 'Error Occured during Loading Silver Layer'
		print 'Error Message' + ERROR_MESSAGE(); 
		print 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		print 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		print '================================================'
	end catch
end
