-- Worksheet 04.Bulk Load - Unit Test
-- Last modified 2020-04-17

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

alter warehouse TEST set warehouse_size = 'XSmall';

-- Set some variables to make it more obvious what each parameter means:
set SORT_ORDER      = 'ASC';    -- The column that controls the order of file loading
set FILES_TO_LOAD   = 16;       -- The total number of files to process in one procedure run
set FILES_AT_ONCE   = 8;        -- The number of files to load in a single transaction 
set MAX_RUN_MINUTES = 1;        -- The maximum run time allowed for a new pass to start 
set TRIES           = 2;        -- The times to retry failed loads 

-- Copy 16 files, 8 files at a time with a maxumum run time of 1 minute. Retry files up to 3 times.
call FILE_INGEST('TEST_STAGE', 'GetCopyTemplate', 'FILE_INGEST_CONTROL', $SORT_ORDER, $FILES_TO_LOAD, $FILES_AT_ONCE, $MAX_RUN_MINUTES, $TRIES);   

-- Alternatively, you can run without setting variables (only using them to make the call more readable).
call file_ingest('TEST_STAGE', 'GetCopyTemplate', 'FILE_INGEST_CONTROL', 'ASC', 16, 8, 1, 3);

-- Examine the control table.
select * from FILE_INGEST_CONTROL where INGESTION_STATUS <> 'WAITING' order by INGESTION_ORDER desc;

-- Check on loaded rows
select count(*) from TARGET_TABLE;
