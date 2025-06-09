/*
====================================================================
DDL Scripts: Create Bronze Table
====================================================================
Script Purpose:
This Script creates tables in th 'bronze' schema, dropping existing tables
if already exists
Run this script to re-deine the structure of 'bronze' tables
====================================================================
*/

if OBJECT_ID('BRONZE.CRM_CUST_INFO', 'U') is not null
drop table BRONZE.CRM_CUST_INFO;
CREATE TABLE BRONZE.CRM_CUST_INFO(
CST_ID INT,
CST_KEY NVARCHAR(50),
CST_FIRSTNAME NVARCHAR(50),
CST_LASTNAME NVARCHAR(50),
CST_MATERIALSTATUS NVARCHAR(50),
CST_GNDR NVARCHAR(50),
CST_CREATE_DATE DATE
);
if OBJECT_ID('BRONZE.CRM_PRD_INFO', 'U') is not null
drop table BRONZE.CRM_PRD_INFO;
CREATE TABLE BRONZE.CRM_PRD_INFO(
PRD_ID INT,
PRD_KEY NVARCHAR(50),
PRD_NM NVARCHAR(50),
PRD_COST INT,
PRD_LINE NVARCHAR(50),
PRD_STRT_DT DATETIME,
PRD_END_DT DATETIME
);
if OBJECT_ID('BRONZE.CRM_SALES_DETAILS', 'U') is not null
drop table BRONZE.CRM_SALES_DETAILS;
CREATE TABLE BRONZE.CRM_SALES_DETAILS(
SLS_ODR_NUM NVARCHAR(50),
SLS_PRD_KEY NVARCHAR(50),
SLS_CST_ID INT,
SLS_ODR_DT INT,
SLS_SHIP_DT INT,
SLS_DUE_DT INT,
SLS_SALES INT,
SLS_QUANTITY INT,
SLS_PRICE INT
);
if OBJECT_ID('BRONZE.ERP_LOCA101', 'U') is not null
drop table BRONZE.ERP_LOCA101;
CREATE TABLE BRONZE.ERP_LOCA101(
Cid NVARCHAR(50),
CNTRY NVARCHAR(50)
);

if OBJECT_ID('BRONZE.ERP_CUST_AZ12', 'U') is not null
drop table BRONZE.ERP_CUST_AZ12;
CREATE TABLE BRONZE.ERP_CUST_AZ12(
Cid nvarchar(50),
BDATE date,
GEN nvarchar(50)
);
if OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') is not null
drop table bronze.erp_px_cat_g1v2;

create table bronze.erp_px_cat_g1v2(
id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintenace nvarchar(50)
);




