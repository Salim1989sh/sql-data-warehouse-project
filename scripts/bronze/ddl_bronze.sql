IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
    
create Table bronze.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date
)

IF OBJECT_ID ('bronze.crm_prd_info','U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
create Table bronze.crm_prd_info(
prd_id int,
prd_key nvarchar (50),
prd_nm nvarchar (50),
prd_cost int,
prd_line nvarchar (50),
prd_start_dt datetime,
prd_end_dt datetime
)

IF OBJECT_ID ('bronze.crm_sales_details','U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
create Table bronze.crm_sales_details(
sls_ord_num nvarchar (50) ,
sls_prd_key nvarchar (50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int
)

IF OBJECT_ID ('bronze.erp_cust_az12','U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
create Table bronze.erp_cust_az12(
CID nvarchar (50),
BDATE date,
GEN nvarchar (50)
)

IF OBJECT_ID ('bronze.erp_loc_a101','U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
create Table bronze.erp_loc_a101(
CID nvarchar (50),
CNTRY nvarchar (50)
)

IF OBJECT_ID ('bronze.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
create Table bronze.erp_px_cat_g1v2(
ID nvarchar (50),
CAT nvarchar (50),
SUBCAT nvarchar (50),
MAINTENANCE nvarchar (50)
)
