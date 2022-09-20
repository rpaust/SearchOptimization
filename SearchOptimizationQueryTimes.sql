'''
Once we have a table that is Search Optimized and the original table we can test query times
on secondary point lookups 
'''

-- testing non search optimized table
select
  ss_sold_Date_sk,
  sum(ss_quantity),
  sum(ss_net_profit)
from
  "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."STORE_SALES"
where
  ss_ticket_number = 2080469305
group by
  1;

-- testing search optimized table
select
  ss_sold_Date_sk,
  sum(ss_quantity),
  sum(ss_net_profit)
from
  "<my_database>"."<my_schema>"."<table_alias>"
where
  ss_ticket_number = 2080469305
group by
  1;

-- record query time
select * from "SNOWFLAKE"."ACCOUNT_USAGE"."QUERY_HISTORY" where database_name = "<my_database>";