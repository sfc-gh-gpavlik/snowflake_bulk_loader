-- Worksheet 03.Bulk Load - Set/Reset
-- Last modified 2020-09-04

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
*  This script sets and resets the control table and target tables.                                 *
*  Click on the "All Queries" checkbox and run all statements to reset unit tests.                  *
*                                                                                                   *
*  This script *is* possibly useful for production environments. If the LIST command does not       *
*  time out, you can use the LIST command here to populate the control table.                       *
*                                                                                                   *
****************************************************************************************************/

/****************************************************************************************************
*                                                                                                   *
*  Create a sample table for testing. Skip if this is a production use of the project.              *
*                                                                                                   *
****************************************************************************************************/
create or replace transient table ORDERS   like "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF10000"."ORDERS";
create or replace transient table JORDERS  like "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF10000"."JORDERS";
create or replace transient table LINEITEM like "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF10000"."LINEITEM";


-- For now, just insert these into the statements table +++++++++ TO DO MORE HERE!!!
truncate COPY_INTO_STATEMENTS;

insert into COPY_INTO_STATEMENTS
    (
     STATEMENT_NAME
    ,STATEMENT_TEXT
    )
values
    ('ORDERS',   'copy into ORDERS   from @TEST_STAGE file_format=(type=CSV) on_error=continue'),
    ('JORDERS',  'copy into JORDERS  from @TEST_STAGE file_format=(type=CSV) on_error=continue'),
    ('LINEITEM', 'copy into LINEITEM from @TEST_STAGE file_format=(type=CSV) on_error=continue');

/****************************************************************************************************
*  Reset the control table if it's already been created.                                            *
****************************************************************************************************/
truncate table if exists FILE_INGEST_CONTROL;
   
   
/****************************************************************************************************
*                                                                                                   *
*  This section populates the control table. It uses the stage LIST to fill the control table.      *
*  This section *is* useful in a production environment if the LIST command returns (does not       *
*  time out) and returns the file list you need.                                                    *
*                                                                                                   *
****************************************************************************************************/
list @TEST_STAGE/TPCH/;

insert into FILE_INGEST_CONTROL
    (
     FILE_PATH, 
     COPY_INTO_NAME,
     INGESTION_ORDER, 
     FILE_SIZE
    )
    select 
        "name",
        case
            when "name" like '%/TPCH/ORDERS/%'   then 'ORDERS'      -- Specify which COPY_INTO statement to use for each set of files.
            when "name" like '%/TPCH/JORDERS/%'  then 'JORDERS'
            when "name" like '%/TPCH/LINEITEM/%' then 'LINEITEM'
        end,
        LAST_MODIFIED_TO_TIMESTAMP("last_modified"),
        "size"
from table(result_scan(last_query_id()));

-- Review the control table to see how it looks:
select * from FILE_INGEST_CONTROL order by INGESTION_ORDER asc limit 10;

-- Recommended values for FILE_TO_PROCESS and FILES_AT_ONCE
with
PARAMS(WAREHOUSE_SIZE, NUMBER_OF_WAREHOUSES, CORES, FILES_TO_PROCESS, AVG_MB, FILES_AT_ONCE_PER8_WH, FILES_AT_ONCE)
as
(
select  'X-Small'                                           as WAREHOUSE_SIZE,        --Change to the size you plan to use
        1                                                   as NUMBER_OF_WAREHOUSES,  --Change to the number of warehouses you will run the SP in parallel
        nodes_per_warehouse(WAREHOUSE_SIZE) * 8             as CORES,
        count(*)                                            as FILES_TO_PROCESS,
        avg(FILE_SIZE)/2000000                              as AVG_MB,  
        ceil((512 / AVG_MB) / CORES) * CORES                as FILES_AT_ONCE_PER8_WH,
        FILES_AT_ONCE_PER8_WH * 
        (ceil(8 / NUMBER_OF_WAREHOUSES) * 
        NUMBER_OF_WAREHOUSES / 8)::integer                 as FILES_AT_ONCE
from    FILE_INGEST_CONTROL
)
select  FILES_TO_PROCESS, 
        iff(FILES_AT_ONCE > 1000, 1000, 
            iff(FILES_AT_ONCE < CORES, CORES, FILES_AT_ONCE)) as FILES_AT_ONCE
from    PARAMS;
  
/****************************************************************************************************
*                                                                                                   *
* Calculate file size consistency and maximum recommended warhouse size for each running            *
* FileIngest stored procedure. NOTE: X-Small warehouses are always the most efficient option.       *
*                                                                                                   *
* This section *is* useful in production to determine the maximum recommended warehouse size.       *
* You can run the stored procedure on as many warehouses as you want up to the max recommended      *
* size for each warehouse. The max recommended size depends on the consistency of the file size.    *
*                                                                                                   *
****************************************************************************************************/
with 
    FILE_STATS (AVG_FILE_SIZE) as
    (
        select
        avg(FILE_SIZE)  as AVG_SIZE
        from            FILE_INGEST_CONTROL
    )
select  sum(case when FILE_SIZE / S.AVG_FILE_SIZE < 0.50 then 1 else 0 end) / count(*) * 100    as PERCENT_ABNORMALLY_SMALL_FILES,
        sum(case when FILE_SIZE / S.AVG_FILE_SIZE > 2.00 then 1 else 0 end) / count(*) * 100    as PERCENT_ABNORMALLY_LARGE_FILES,
        (100 - PERCENT_ABNORMALLY_SMALL_FILES) - PERCENT_ABNORMALLY_LARGE_FILES                 as PERCENT_AVERAGE_SIZE_FILES,
        case
            when PERCENT_AVERAGE_SIZE_FILES >= 95 and PERCENT_ABNORMALLY_LARGE_FILES < 0.5 then 'Medium'
            when PERCENT_AVERAGE_SIZE_FILES >= 90 and PERCENT_ABNORMALLY_LARGE_FILES < 1.0 then 'Small'
            else                                                                                'X-Small'
        end                                                                                     as MAX_RECOMMENDED_WAREHOUSE_SIZE
from    FILE_INGEST_CONTROL C, FILE_STATS S;
