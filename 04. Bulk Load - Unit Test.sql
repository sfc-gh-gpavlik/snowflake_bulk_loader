-- Worksheet 04.Bulk Load - Unit Test
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
* Small scale unit test for the Bulk Load project.                                                  *
*                                                                                                   *
* Start with a small test, and then increase the number of files and the number of concurrently     *
* running FileIngest stored procedures. There's no practical limit to the number running in         *
* parallel. For maximum stability, after conducting a short (5-10 minute) test with parallel jobs,  *
* run multiple FileIngest stored procedures from different sessions (each worksheet will be in      *
* its own session) and schedule them to run from a task. That way closing the client will not       *
* stop the running stored procedures. Even though a single run of the FileIngest stored procedure   *
* should take longer than a minute, you can schedule the tasks to run once a minute. That will      *
* ensure that they run again quickly after finishing a section of files.                            *
*                                                                                                   *
****************************************************************************************************/

-- Check the Ingestion Control Table:
select * from FILE_INGEST_CONTROL;

-- The unit test only requires an X-Small warehouse.
alter warehouse TEST set warehouse_size = 'X-Small';

/****************************************************************************************************
*                                                                                                   *
* Run a small unit test. When you run a full scale jobs in parallel, do NOT use these test values.  *
* Go to worksheet 03. Bulk Load - Set/Reset and run the last two SQL statements to get recommended  *
* values for the warehouse size and FILE_INGEST parameters.                                         *
*                                                                                                   *
****************************************************************************************************/
call file_ingest('TEST_STAGE', 'FILE_INGEST_CONTROL', 'COPY_INTO_STATEMENTS', 'ASC', 16, 8, 20, 3);

-- Examine the control table.
select * from FILE_INGEST_CONTROL where INGESTION_STATUS <> 'WAITING' order by INGESTION_ORDER desc;

-- Check on loaded rows
select (select count(*) from JORDERS) as JORDERS_COUNT,
       (select count(*) from ORDERS) as ORDERS_COUNT,
       (select count(*) from LINEITEM) as LINEITEM_COUNT;

-- Check to see if there are any errors noted in the control table:
select * from FILE_INGEST_CONTROL where error_msg <> '';
