-- Worksheet 05.Bulk Load - Runtime
-- Last modified 2020-04-17

/****************************************************************************************************
*                                                                                                   *
* This worksheet has several queries to monitor performance of bulk load executions. If you get a   *
* message about a missing PROGRESS_BAR function, run the CREATE statement for the PROGRESS_BAR UDF. *
*                                                                                                   *
* The final two queries allow you to suspend and resume running FileIngest stored procedures.       *
* After finihing the COPY INTO for a group of files, the procedure looks for new files marked       *
* WAITING. The "Kill Swith" query simply updates all files in the control table marked WAITING to   *
* SUSPENDED. This has the effect of killing all running FileIngest stored procedures gracefully.    *
*                                                                                                   *
****************************************************************************************************/

-- See rows going into the target table:
select to_varchar(count(*), '999,999,999,999,999,999') as ROW_COUNT from TARGET_TABLE;

-- Big progress bar to refresh periodically:
with
PCT_COMPLETE (PERCENT_COMPLETE) as
(
    select 
        (select sum(FILE_SIZE) from FILE_INGEST_CONTROL where INGESTION_STATUS = 'LOADED') /  /* Numerator   */
        (select sum(FILE_SIZE) from FILE_INGEST_CONTROL) * 100 as PERCENT_COMPLETE            /* Denominator */
)
select progress_bar(PERCENT_COMPLETE, 2, 67) as PROGRESS from PCT_COMPLETE;

-- Get the progress and ETA.
-- NOTE: These results will only be accurate for a continuous run of the bulk load utility.
--       The results are only accurate while the bulk load utility is running, not after it's done.
--       If you stop and restart the utility, these numbers will not be meaningful unless
--       you modify the query to choose the most relevant period of time in the control table.
with
PCT_COMPLETE (PERCENT_COMPLETE) as
(
    select 
        (select sum(FILE_SIZE) from FILE_INGEST_CONTROL where INGESTION_STATUS = 'LOADED') /  /* Numerator   */
        (select sum(FILE_SIZE) from FILE_INGEST_CONTROL) * 100 as PERCENT_COMPLETE            /* Denominator */
),
RUNTIME (RUNNING_SECONDS) as
(
select datediff('seconds', min(START_TIME), to_timestamp_ntz(current_timestamp)) as RUNNING_SECONDS from FILE_INGEST_CONTROL
),
GB_LOADED (GIGABYTES_LOADED) as
(
select sum(FILE_SIZE) / 1000000000 from FILE_INGEST_CONTROL where INGESTION_STATUS = 'LOADED'
)
select  progress_bar(P.PERCENT_COMPLETE, 2, 12)                     as PROGRESS,
        L.GIGABYTES_LOADED                                          as GIGABYTES_LOADED,
        round(R.RUNNING_SECONDS / 60)                               as RUNNING_MINUTES,
        round((R.RUNNING_SECONDS * (100 / PERCENT_COMPLETE)) / 60)  as ESTIMATED_TOTAL_RUNTIME_MINUTES,
        ESTIMATED_TOTAL_RUNTIME_MINUTES - RUNNING_MINUTES           as ESTIMATED_MINUTES_REMAINING
from RUNTIME R, PCT_COMPLETE P, GB_LOADED L
;

-- Get a list of currently-loading files:
select * from FILE_INGEST_CONTROL where INGESTION_STATUS = 'LOADING';

-- Statistics for actively loading
select   OWNER_SESSION,
         count(FILE_PATH) as FILES_BEING_LOADED,
         sum(FILE_SIZE) / 1000000000 as GB_LOADING
from     FILE_INGEST_CONTROL
where    INGESTION_STATUS = 'LOADING'
group by OWNER_SESSION
;

-- Get a list of possibly stuck files (from a killed or failed SP run):
select  FILE_PATH,
        datediff(minute, START_TIME, to_timestamp_ntz(current_timestamp)) as LOADING_MINUTES 
from    FILE_INGEST_CONTROL
where   INGESTION_STATUS = 'LOADING' and
        LOADING_MINUTES > 60;

-- Reset stuck files. Adjust time to determine stuck files as applicable. If no running Bulk Load SP, set to 0:
update  FILE_INGEST_CONTROL
set     INGESTION_STATUS = 'WAITING'
where   INGESTION_STATUS = 'LOADING' and
        datediff(minute, START_TIME, to_timestamp_ntz(current_timestamp)) >= 60 -- <== Adjust time in minutes here
;

-- Statistics for already-attempted loads:
select  count(FILE_PATH)               as FILES, 
        INGESTION_STATUS               as INGESTION_STATUS,
        sum(C.FILE_SIZE) / 1000000000  as GB_LOADED,
        avg(C.FILE_SIZE) / 1000000     as AVERAGE_FILE_SIZE_MB,
        max(C.FILE_SIZE) / 1000000     as MAX_FILE_SIZE_MB,
        min(C.FILE_SIZE) / 1000000     as MIN_FILE_SIZE_MB,
        sum(L.ROW_COUNT)               as ROW_COUNT,
        avg(L.ROW_COUNT)               as AVERAGE_ROW_COUNT,
        sum(L.ROW_PARSED)              as ROW_PARSED,
        sum(L.ERROR_COUNT)             as ERROR_COUNT
from    FILE_INGEST_CONTROL C
        inner join  information_schema.load_history L on 
                    stage_path_shorten(C.FILE_PATH) = stage_path_shorten(L.FILE_NAME)
group by INGESTION_STATUS
having   INGESTION_STATUS <> 'LOADING';

-- Load log
select  C.FILE_PATH,
        L.FILE_NAME,
        L.ROW_COUNT,
        L.ROW_PARSED,
        L.FIRST_ERROR_MESSAGE,
        L.FIRST_ERROR_LINE_NUMBER,
        L.FIRST_ERROR_CHARACTER_POSITION,
        L.FIRST_ERROR_COL_NAME,
        L.ERROR_COUNT,
        C.INGESTION_ORDER,
        C.INGESTION_STATUS,
        C.OWNER_SESSION,
        C.EXEC_UUID,
        C.TRY_COUNT,
        C.START_TIME,
        C.END_TIME,
        C.FILE_SIZE,
        C.ERROR_MSG
from FILE_INGEST_CONTROL C
    inner join   information_schema.load_history L on 
                 stage_path_shorten(C.FILE_PATH) = stage_path_shorten(L.FILE_NAME);

-- Kill Switch. Run this on the control table, and the SP will think it's done with all the files.
-- The SP will terminate after completing the currently-running batches, and not attempt new ones.
update FILE_INGEST_CONTROL set INGESTION_STATUS = 'SUSPENDED' where INGESTION_STATUS = 'WAITING';

-- Resume after Kill Switch. Undo the kill switch to run again.
update FILE_INGEST_CONTROL set INGESTION_STATUS = 'WAITING' where INGESTION_STATUS = 'SUSPENDED';
