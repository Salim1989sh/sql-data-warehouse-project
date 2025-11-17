/*
USAGE EXAMPLE 
USE DataWarehouse;
GO

EXEC bronze.load_bronze;
GO

*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME , @batch_end_time DATETIME; 	
	BEGIN TRY
		
		SET @batch_start_time = GETDATE();
		print('======================');
		print ('Loading bronze Layer');
		print('======================');

		print('----------------------');
		print ('Loading crm Table');
		print('----------------------');



		SET	 @start_time = GETDATE();
		print ('Truncating table : bronze.crm_cust_info')
		TRUNCATE TABLE bronze.crm_cust_info;
		print ('Inserting data into bronze.crm_cust_info ')
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Salim\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET	 @end_time = GETDATE();
		PRINT'>> Load Durationl: ' + cast(datediff(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT'>> ----------------';




		SET	 @start_time = GETDATE();
		print ('Truncating table : bronze.crm_prd_info')
		TRUNCATE TABLE bronze.crm_prd_info;
		print ('Inserting data into bronze.crm_prd_info ')
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Salim\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET	 @end_time = GETDATE();
		PRINT'>> Load Durationl: ' + cast(datediff(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT'>> ----------------';



		SET	 @start_time = GETDATE();
		print ('Truncating table : bronze.crm_sales_details')
		TRUNCATE TABLE bronze.crm_sales_details;
		print ('Inserting data into bronze.crm_sales_details ')
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Salim\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET	 @end_time = GETDATE();
		PRINT'>> Load Durationl: ' + cast(datediff(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT'>> ----------------';



		print('----------------------');
		print ('Loading erp Table');
		print('----------------------');


		SET	 @start_time = GETDATE();
		print ('Truncating table : bronze.erp_cust_az12')
		TRUNCATE TABLE bronze.erp_cust_az12;
		print ('Inserting data into bronze.erp_cust_az12 ')
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Salim\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET	 @end_time = GETDATE();
		PRINT'>> Load Durationl: ' + cast(datediff(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT'>> ----------------';




		SET	 @start_time = GETDATE();
		print ('Truncating table : bronze.erp_loc_a101')
		TRUNCATE TABLE bronze.erp_loc_a101;
		print ('Inserting data into bronze.erp_loc_a101 ')
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Salim\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET	 @end_time = GETDATE();
		PRINT'>> Load Durationl: ' + cast(datediff(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT'>> ----------------';




		SET	 @start_time = GETDATE();
		print ('Truncating table : bronze.erp_px_cat_g1v2')
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		print ('Inserting data into bronze.erp_px_cat_g1v2 ')
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Salim\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET	 @end_time = GETDATE();
		PRINT'>> Load Durationl: ' + cast(datediff(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT'>> ----------------';


		SET @batch_end_time = GETDATE();
		print '======================';
		PRINT 'Loading Bronze layer is completed';
		print ' - total load duration: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds';
		print '======================';

	END TRY
	BEGIN CATCH
		PRINT '========================================'
		PRINT 'ERROR OCCURE DURING LOADING BRONZE LAYER'
		PRINT 'Error Message'+ ERROR_MESSAGE();
		PRINT 'Error Message'+ CAST (ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'Error Message'+ CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '========================================'
	END CATCH
END
