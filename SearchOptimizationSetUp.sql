'''
First we would like to have two identical tables for us to compare results of an Optimized 
table and a Non-Optimized table. We did this by creating a table within our own database 
and schema that is a copy of a large dataset from the Snowflake Sample Database.

For our tests we chose the table:

Database: SNOWFLAKE_SAMPLE_DATA
Schema: TPCDS_SF10TCL
Table: STORE_SALES
Created on: 3/8/2017, 1:41:55 PM
Rows: 28,800,239,865
Size: 1.3TB
Cluster by: LINEAR(SS_SOLD_DATE_SK, SS_ITEM_SK) 
'''

-- select correct role, database, and warehouse
use role sysadmin;
use database <my_database>;
use schema <my_schema>
use warehouse <my_warehouse>;

-- create duplicate table
create table "<my_database>"."<my_schema>"."<my_table>" as (
  select
    *
  from
    "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."STORE_SALES"
);

-- get price estimate of search optimization
select
  system $ estimate_search_optimization_costs('<my_database>.<my_schema>.<my_table>');

'''
To add search optimization you must have Enterprise edition or above, OWNERSHIP privilege on the 
table and have ADD SEARCH OPTIMIZATION privilege on the schema that contains the table. Accountadmin
can grant these priveleges to other roles. 
'''

-- switch to role accountadmin to grant previleges
use role accountadmin;

-- add search optimization previleges to role on schema
grant add SEARCH OPTIMIZATION on schema <my_schema> to role sysadmin;

-- switch back to role sysadmin
use role sysadmin;

-- add search optimization to table
alter table
  if exists "<my_database>"."<my_schema>"."<table_alias>"
add
  search optimization;

-- check search optimization status of table
show tables;