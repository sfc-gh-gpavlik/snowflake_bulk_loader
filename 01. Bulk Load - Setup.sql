-- Worksheet 01.Bulk Load - Setup
-- Last modified 2021-09-04

/********************************************************************************************************
*                                                                                                       *
*                                     Snowflake Bulk Load Project                                       *
*                                                                                                       *
*  Copyright (c) 2020, 2021 Snowflake Computing Inc. All rights reserved.                               *
*                                                                                                       *
*  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in  *
*. compliance with the License. You may obtain a copy of the License at                                 *
*                                                                                                       *
*                               http://www.apache.org/licenses/LICENSE-2.0                              *
*                                                                                                       *
*  Unless required by applicable law or agreed to in writing, software distributed under the License    *
*  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or  *
*  implied. See the License for the specific language governing permissions and limitations under the   *
*  License.                                                                                             *
*                                                                                                       *
*  Copyright (c) 2020, 2021 Snowflake Computing Inc. All rights reserved.                               *
*                                                                                                       *
********************************************************************************************************/

/****************************************************************************************************
*                                                                                                   *
*  This worksheet creates an internal stage and fills it with data from SNOWFLAKE_SAMPLE_DATA.      *
*  For the purposes of the control table and stored procedure, an internal stage will               *
*  work the same way as an external one.                                                            *
*                                                                                                   *
*  ==> Run this worksheet only once. It doesn't need re-running when resetting the environment.     *
*                                                                                                   *
*  ==> This script uses a warehouse named TEST, but no code requires this name,                     *
*      If you want to use a different warehouse, you will need to change the name                   *
*      in the SQL script.                                                                           *
*                                                                                                   *
*                                                                                                   *
*  Run this  only to create a some sample data for testing. You do not need to run this             *
*  if you have another stage data to use. You do not need to run this more than once. To reset the  *
*  test environment, run all statements in the "03. Bulk Load - Set/Reset" worksheet.               *
*                                                                                                   *
*  NOTE: If you do not see a SAMPLE_DATA database, it may be named something else. If you don't     *
*        have one at all, you can import the shared database from SFC_SAMPLES. Click on the Shares  *
*        button on the ribbon bar to import the database.                                           *
*                                                                                                   *
*  Delete the first to characters /* on line 109 and execute SQL to load sample data.               *
*                                                                                                   *
****************************************************************************************************/


-- Copy 1.5 billion rows from Snowflake's sample data to a stage:
use role ACCOUNTADMIN;
drop table if exists ORDERS;
drop table if exists JORDERS;
drop table if exists LINEITEM;

create or replace stage TEST_STAGE;
grant all privileges on stage TEST_STAGE to SYSADMIN;
use role SYSADMIN;

-- NOTE: Not all Snowflake accounts have the same names for sample data. Adjust as required.
select to_varchar(count(*), '999,999,999,999,999,999') as ROW_COUNT from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."ORDERS";
select to_varchar(count(*), '999,999,999,999,999,999') as ROW_COUNT from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."JORDERS";
select to_varchar(count(*), '999,999,999,999,999,999') as ROW_COUNT from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."LINEITEM";

-- Copy data into CSV files in an internal stage:
alter warehouse "TEST" set warehouse_size = 'XXLARGE';
copy into @TEST_STAGE/TPCH/ORDERS/    from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."ORDERS";     -- Takes ~0:01:01 on 2XL
copy into @TEST_STAGE/TPCH/JORDERS/   from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."JORDERS";    -- Takes ~0:01:21 on 2XL
copy into @TEST_STAGE/TPCH/LINEITEM/  from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."LINEITEM";   -- Takes ~0:06:12 on 2XL
alter WAREHOUSE "TEST" set warehouse_size = 'XSMALL';

-- Send status message:
select 'Test data staged for bulk load utility.' as STATUS;
