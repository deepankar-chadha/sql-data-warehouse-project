/*
=================================================================================
Stored Procedure: Load Bronze Layer (Source--> Bronze)
=================================================================================
Script  Purpose:
This Stored Procedure loads data into 'bronze' schema from external csv files.
It performs the following actions:
- Truncates the bronze table before loading data
- Uses the BULK INSERT Command to load data from CSV Files to Bronze Tables
Parameters
None
This stored procedure does not accept any parameters or return any value
usage Example;
Exec bronze.load_bronze
=================================================================================
*/

create or alter procedure bronze.load_bronze as
begin
 declare @starttime datetime, @endtime datetime, @batchstarttime datetime , @batchendtime datetime;
  begin try
  set @batchstarttime = GETDATE();
	print '==============================';
	print 'Loading Bronze Layer';
	print '===============================';

	print '--------------------------------';
	print 'loading CRM Tables';
	print '--------------------------------'

	set @starttime = getdate();
	print'>> Truncating Table:bronze.crm_cust_info';
	truncate table bronze.crm_cust_info
	print 'Inserting Data Into: bronze.crm_cust_info';
	bulk insert bronze.crm_cust_info
	from 'C:\Users\laptop\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	with (firstrow = 2,
	fieldterminator = ',',
	tablock
	);
	set @endtime = getdate();
	print 'load duration:' + cast(datediff(second,@starttime,@endtime) as nvarchar) +'seconds';
	print '>>----------------------------------';



	set @starttime = getdate();
	print '>> Truncating Table:bronze.crm_prd_info';
	truncate table bronze.crm_prd_info
	print 'Inserting Data Into: bronze.crm_prd_info';
	bulk insert bronze.crm_prd_info
	from 'C:\Users\laptop\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	with (firstrow = 2,
	fieldterminator = ',',
	tablock
	);
	set @endtime = getdate();
	print 'load duration:' + cast(datediff(second,@starttime,@endtime) as nvarchar) +'seconds';
	print '>>----------------------------------';

	set @starttime = getdate();
	print '>> Truncating Table: bronze.crm_sales_details';
	truncate table bronze.crm_sales_details
	print 'Inserting Data Into: bronze.crm_sales_details';
	bulk insert bronze.crm_sales_details
	from 'C:\Users\laptop\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	with (firstrow = 2,
	fieldterminator = ',',
	tablock
	);
	set @endtime = getdate();
	print 'load duration:' + cast(datediff(second,@starttime,@endtime) as nvarchar) +'seconds';
	print '>>----------------------------------';

	print '--------------------------------';
	print 'loading ERP Tables';
	print '--------------------------------';

	set @starttime = getdate();
	print '>> Truncating Table: bronze.erp_cust_az12';
	truncate table bronze.erp_cust_az12
	print 'Inserting Data Into: bronze.erp_cust_az12';
	bulk insert bronze.erp_cust_az12
	from 'C:\Users\laptop\Desktop\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
	with (firstrow = 2,
	fieldterminator = ',',
	tablock
	);
	set @endtime = getdate();
	print 'load duration:' + cast(datediff(second,@starttime,@endtime) as nvarchar) +'seconds';
	print '>>----------------------------------';

	set @starttime = getdate();
	print '>> Truncating Table: bronze.erp_px_cat_g1v2';
	truncate table bronze.erp_px_cat_g1v2
	print 'Inserting Data Into: bronze.erp_px_cat_g1v2';
	bulk insert bronze.erp_px_cat_g1v2
	from 'C:\Users\laptop\Desktop\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
	with (firstrow = 2,
	fieldterminator = ',',
	tablock
	);
	set @endtime = getdate();
	print 'load duration:' + cast(datediff(second,@starttime,@endtime) as nvarchar) +'seconds';
	print '>>----------------------------------';

	set @starttime = getdate();
	print '>> Truncating Table: bronze.erp_loca101';
	truncate table bronze.erp_loca101
	print 'Inserting Data Into: bronze.erp_loca101';
	bulk insert bronze.erp_loca101
	from 'C:\Users\laptop\Desktop\sql-data-warehouse-project\datasets\source_erp\local_a100.csv'
	with (firstrow = 2,
	fieldterminator = ',',
	tablock
	);
	set @endtime = getdate();
	print 'load duration:' + cast(datediff(second,@starttime,@endtime) as nvarchar) +'seconds';
	print '>>----------------------------------';

	set @batchendtime = GETDATE();
	print 'load duration:' + cast(datediff(second,@batchstarttime,@batchendtime) as nvarchar) +'seconds';
	print '>>----------------------------------';
	end try
	begin catch
	print '============================';
	print 'error occured during loading bronze layer';
	print 'error message' + error_message();
	print 'error message' + cast(error_number() as nvarchar);
	print 'error message' + cast(error_state() as nvarchar);
	print '============================'
	end catch
end;



exec bronze.load_bronze;
