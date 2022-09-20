'''
last we tested the cost of continuously loading data into a search optimized table
'''

-- create sequence to randomize ss_ticket_number
create
or replace sequence SO_seq start with 2400000001 increment by 1;


-- insert 1 million rows with random ss_ticket_number (single batch load)
insert into
  "<my_database>"."<my_schema>"."<table_alias>"
select
  uniform(2450816, 2452642, random()),
  uniform(28800, 75599, random()),
  uniform(1, 402000, random()),
  uniform(1, 65000000, random()),
  uniform(1, 1920800, random()),
  uniform(1, 7200, random()),
  uniform(1, 32500000, random()),
  uniform(1, 1498, random()),
  uniform(1, 2000, random()),
  SO_seq.nextval,
  uniform(1, 100, random()),
  round(uniform(1 :: float, 100 :: float, random()), 2),
  round(uniform(1 :: float, 200 :: float, random()), 2),
  round(uniform(0 :: float, 200 :: float, random()), 2),
  round(uniform(0 :: float, 19778 :: float, random()), 2),
  round(uniform(0 :: float, 19972 :: float, random()), 2),
  round(uniform(1 :: float, 10000 :: float, random()), 2),
  round(uniform(1 :: float, 20000 :: float, random()), 2),
  round(uniform(0 :: float, 1797 :: float, random()), 2),
  round(uniform(0 :: float, 19778 :: float, random()), 2),
  round(uniform(0 :: float, 19972 :: float, random()), 2),
  round(uniform(0 :: float, 21769 :: float, random()), 2),
  round(
    uniform(-10000 :: float, 9986 :: float, random()),
    2
  )
from
  "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."STORE_SALES"
limit
  1000000;


-- 24 inserts of 41,667 rows each every hour
create task hourly_inserts warehouse = <my_warehouse> schedule = '60 minutes' allow_overlapping_execution = FALSE comment = 'insert 24 times in a day' as
insert into
  "<my_database>"."<my_schema>"."<table_alias>"
select
  uniform(2450816, 2452642, random()),
  uniform(28800, 75599, random()),
  uniform(1, 402000, random()),
  uniform(1, 65000000, random()),
  uniform(1, 1920800, random()),
  uniform(1, 7200, random()),
  uniform(1, 32500000, random()),
  uniform(1, 1498, random()),
  uniform(1, 2000, random()),
  SO_seq.nextval,
  uniform(1, 100, random()),
  round(uniform(1 :: float, 100 :: float, random()), 2),
  round(uniform(1 :: float, 200 :: float, random()), 2),
  round(uniform(0 :: float, 200 :: float, random()), 2),
  round(uniform(0 :: float, 19778 :: float, random()), 2),
  round(uniform(0 :: float, 19972 :: float, random()), 2),
  round(uniform(1 :: float, 10000 :: float, random()), 2),
  round(uniform(1 :: float, 20000 :: float, random()), 2),
  round(uniform(0 :: float, 1797 :: float, random()), 2),
  round(uniform(0 :: float, 19778 :: float, random()), 2),
  round(uniform(0 :: float, 19972 :: float, random()), 2),
  round(uniform(0 :: float, 21769 :: float, random()), 2),
  round(
    uniform(-10000 :: float, 9986 :: float, random()),
    2
  )
from
  "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."STORE_SALES"
limit
  41667;

-- resume hourly inserts task
alter task hourly_inserts resume;

-- suspend hourly inserts task
alter task hourly_inserts suspend;


-- 240 inserts of 4167 rows each every six minutes
create task minute_inserts warehouse = <my_warehouse> schedule = '6 minutes' allow_overlapping_execution = FALSE comment = 'insert 240 times in a day' as
insert into
  "<my_database>"."<my_schema>"."<table_alias>"
select
  uniform(2450816, 2452642, random()),
  uniform(28800, 75599, random()),
  uniform(1, 402000, random()),
  uniform(1, 65000000, random()),
  uniform(1, 1920800, random()),
  uniform(1, 7200, random()),
  uniform(1, 32500000, random()),
  uniform(1, 1498, random()),
  uniform(1, 2000, random()),
  SO_seq.nextval,
  uniform(1, 100, random()),
  round(uniform(1 :: float, 100 :: float, random()), 2),
  round(uniform(1 :: float, 200 :: float, random()), 2),
  round(uniform(0 :: float, 200 :: float, random()), 2),
  round(uniform(0 :: float, 19778 :: float, random()), 2),
  round(uniform(0 :: float, 19972 :: float, random()), 2),
  round(uniform(1 :: float, 10000 :: float, random()), 2),
  round(uniform(1 :: float, 20000 :: float, random()), 2),
  round(uniform(0 :: float, 1797 :: float, random()), 2),
  round(uniform(0 :: float, 19778 :: float, random()), 2),
  round(uniform(0 :: float, 19972 :: float, random()), 2),
  round(uniform(0 :: float, 21769 :: float, random()), 2),
  round(
    uniform(-10000 :: float, 9986 :: float, random()),
    2
  )
from
  "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."STORE_SALES"
limit
  4167;

-- resume minute inserts task
alter task minute_inserts resume;

-- suspend minute inserts task
alter task minute_inserts suspend;


-- turn off search optimization
alter table
  if exists "<my_database>"."<my_schema>"."<table_alias>" drop search optimization;


-- record results
select * from "SNOWFLAKE"."ACCOUNT_USAGE"."SEARCH_OPTIMIZATION_HISTORY" where database_name = "<my_database>";
select * from "SNOWFLAKE"."ACCOUNT_USAGE"."QUERY_HISTORY" where database_name = "<my_database>";