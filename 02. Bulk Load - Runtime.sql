-- Worksheet 02.Bulk Load - Runtime
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
*                              ***  Snowflake Bulk Load Project  ***                                *          
*                                                                                                   *
*  https://drive.google.com/drive/folders/13LF-H-sAKGBkiR7HnYDyKiFlULYgaFDd                         *
*                                                                                                   *
*  ==> Purpose: This project loads a large number of files from stage to a table in a specified     *
*               order. You can create these objects in any database you choose. You must create     *
*               all the objects in this worksheet in order for the project to run properly.         *
*                                                                                                   *
*  ==> Setup:   1) Create all objects in this worksheet in a database of your choice.               *
*               2) Populate the FILE_INGEST_CONTROL table with all the files to load. Set the order *
*                  to load them using the INGESTION_ORDER column. You can define that column as any *
*                  data type that works with ORDER BY. You can name the table anything you want,    *
*                  so if you have multiple tables to load you can run the loads in parallel.        *
*               3) Use the Bulk Load - Set/Reset worksheet to assist with setup. For stages that    *
*                  the LIST command works (does not time out), you can use this to populate the     *
*                  FILE_INGEST_CONTROL table including setting the INGESTION_ORDER using the last   *
*                  modified date for each file. For very large (LIST times out) or complex stages,  *
*                  you may need to get creative to populate the control table. The documentation    *
*                  has some ideas for how to do this.                                               *
*                                                                                                   *
*  ==> Running: 1) Make sure your COPY INTO statement works for the stage and table before running. *
*               2) REPLACE the target table after testing the COPY INTO statement in step 1.        *
*               3) Copy your working COPY INTO statement and paste it into the STATEMENT_TEXT       *
*                  column of the COPY_INTO_STATEMENTS table.                                        *
*                  NOTE: Place "files=( @~FILE_LIST~@ )" at the end of your COPY INTO statement.    *
*               4) ==> IMPORTANT <== Run the create or replace function GetCopyTemplate()           *
*                  If you don't run it after modifying it, calling the function will return the     *
*                  sample value. This will be confusing to troubleshoot since the COPY INTO         *
*                  statement will look right. Be sure to run create or replace GetCopyTemplate()    *
*               5) Run a single instance of the FILE_INGEST stored procedure with a small number of *
*                  files specified in the parameters.                                               *
*               6) Examine results, and if everything looks good run multiple copies of the stored  *
*                  procedure in parallel EACH USING ITS OWN WAREHOUSE. It defeats the purpose of    *
*                  the project if they run in the same warehouse. Increase the number of files in   *
*                  testing until the SP runs at least five minutes per run.                         *
*               7) Automate running a bulk load. Create multiple Snowflake TASKs to run the         *
*                  FILE_INGEST stored procedure. Ensure that each TASK runs in a different          *
*                  warehouse. Set the tasks to run once per minute. (They will not re-run if they   *
*                  are still running from the last execution.) Set the stored procedure to run      *
*                  a long time, perhaps 60 minutes.                                                 *
*               8) ===> IMPORTANT <=== For one-time loads, when done, suspend the TASKs.            *
*                                                                                                   *
****************************************************************************************************/

-- Required User Defined Function.
create or replace function STAGE_PATH_SHORTEN(FILE_PATH string)
returns string
language javascript
strict immutable
as
$$
    /*
        Removes the cloud provider prefix and stage name from the file path
    */
    var s3 = FILE_PATH.search(/s3:\/\//i);

    if ( s3 != -1){
        return FILE_PATH.substring(FILE_PATH.indexOf("/", s3 + 5) + 1);
    }

    var azure = FILE_PATH.search(/azure:\/\//i);

    if ( azure != -1){
        return FILE_PATH.substring(FILE_PATH.indexOf("/", azure + 8) + 1);
    }

    var newStyleInternal = FILE_PATH.search(/stages\//i);

    if (newStyleInternal != -1){
        return FILE_PATH.substring(FILE_PATH.indexOf("/", newStyleInternal + 7) + 1);
    }

    var newStyleInternal = FILE_PATH.search(/stages[a-zA-Z0-9]{4,10}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}\//i);

    if (newStyleInternal != -1){
        return FILE_PATH.substring(FILE_PATH.indexOf("/", newStyleInternal) + 1);
    }

    var stageRegExp = "/";
    var re = new RegExp(stageRegExp, "i");

    var stageInStr = FILE_PATH.search(re);

    if (stageInStr != -1){
        return FILE_PATH.substring(FILE_PATH.indexOf("/", stageInStr) + 1);
    }

    throw "Unknown file path type."
$$;

-- Write a stored procedure to control this table:
create or replace table COPY_INTO_STATEMENTS
    (
     STATEMENT_NAME      string                                 -- The name to specify which COPY INTO statement to use for a file
    ,STATEMENT_TEXT      string                                 -- The text of the COPY INTO statement including files=( @~FILE_LIST~@ ) at the appropriate location
    ,EFFECTIVE           timestamp_tz default current_timestamp -- The effective date of the COPY INTO statement for this table
    ,SUPERSEDED          timestamp_tz                           -- The date when the COPY INTO statement will expire and a new one will supersede it
    );

-- Create a control table to drive the stored procedure. 
create or replace table FILE_INGEST_CONTROL 
    (
     FILE_PATH           string                         -- The file path in the stage                                   
    ,COPY_INTO_NAME      string                         -- The name of the copy into statement to use for this file.    
    ,INGESTION_ORDER     timestamp_tz                   -- Can be any sortable data type. Used only for ORDER BY.       
    ,INGESTION_STATUS    string      default 'WAITING'  -- Status set by COPY INTO results.                             
    ,OWNER_SESSION       integer                        -- The session_id running this stored procedure.                
    ,EXEC_UUID           string                         -- A unique separator in case two SPs run in the same session   
    ,TRY_COUNT           integer     default 0          -- The number of times a file has been tried in a COPY INTO     
    ,START_TIME          timestamp                      -- The last time a file was sent as part of a COPY INTO         
    ,END_TIME            timestamp                      -- The last a COPY INTO returned with this file sent to it      
    ,FILE_SIZE           bigint                         -- Used to collect statistics. Not needed or used by this SP.   
    ,ERROR_MSG           string                         -- The error message (if any) on the most recent COPY INTO      
    );

-- Create the stored procedure to load the target table
create or replace procedure FILE_INGEST(
                                         STAGE_NAME              string
                                        ,CONTROL_TABLE           string
                                        ,COPY_STATEMENTS_TABLE   string
                                        ,SORT_ORDER              string
                                        ,FILES_TO_PROCESS        float
                                        ,FILES_AT_ONCE           float
                                        ,MAX_RUN_MINUTES         float
                                        ,TRIES                   float
                                       )
returns  variant
language javascript
as
$$
/****************************************************************************************************
*                                                                                                   *
* Stored procedure to load a large table from a stage with a large number of files.                 *
* Note that this procedure requires a control table with a specific format [DDL to follow].         *
*                                                                                                   *
* @param  {string}  STAGE_NAME:           The name of the stage from which to load files            *
* @param  {string}  CONTROL_TABLE:        The name of the table that controls this procedure        *
* @param  {string}  COPY_STATEMENTS_TABLE The name of the table with the copy into statements.      *
* @param  {string}  SORT_ORDER:           The column that controls the order of file loading        *
* @param  {string}  FILES_TO_PROCESS:     The total number of files to process in one procedure run *
* @param  {float}   FILES_AT_ONCE:        The number of files to load in a single transaction       *
* @param  {float}   MAX_RUN_MINUTES:      The maximum run time allowed for a new pass to start      *
* @param  {float}   TRIES float           The times to try loading a file, If > 1 it will retry     *
* @return {variant}:                      A JSON with statistics from the execution.                *
*                                                                                                   *
****************************************************************************************************/

class File{}

let out = {};
let parameterError = checkParameters(FILES_TO_PROCESS, FILES_AT_ONCE, SORT_ORDER, MAX_RUN_MINUTES, TRIES);
if(parameterError != "No_Errors"){
    out["Parameter_Error"] = parameterError;
    return out;
}

if(countDuplicateFiles(CONTROL_TABLE)){
    out["Duplicate_Files_Error"] = "There are duplicate files in the control table. De-duplicate before running.";
    return out;
}

let i = 0;
let filesClaimed = -1;
let filesProcessed = 0;
let filesRS;
let passes = Math.ceil(FILES_TO_PROCESS / FILES_AT_ONCE);

let uuid = getUUIDv4();
let endTime = new Date().getTime() + MAX_RUN_MINUTES * 60000;
let isEndTime = 0;

out["Session_ID"] = executeSingleValueQuery("SESSION", "select current_session() as SESSION;");
out["Start_Time"] = date2Timestamp(new Date());

for (i = 0; i < passes && isEndTime == 0; i++){
    try{
        filesClaimed = claimFiles(CONTROL_TABLE, TRIES, FILES_AT_ONCE, SORT_ORDER, uuid);
        if (filesClaimed == 0){
            out["Termination_Reason"] = "Processed_All_Waiting_Files";
            break;
        }
        filesRS = loadFiles(CONTROL_TABLE, COPY_STATEMENTS_TABLE, uuid);
        filesProcessed += filesClaimed;
        markCompleteFiles(CONTROL_TABLE, filesRS);
        if (new Date().getTime() >= endTime){
            out["Termination_Reason"] = "Time_Limit";
            isEndTime = 1;
        }
    }
    catch(err){
        out["Termination_Reason"] = "ERROR: " + err.message.replace(/"/g, '"');
        break;
    }
}

if (filesProcessed >= FILES_TO_PROCESS){
    out["Termination_Reason"] = "File_Limit";
}

out["Files_Processed"] = filesProcessed;
out["End_Time"] = date2Timestamp(new Date());
out["UUID"] = uuid;

return out;

/***************************************************************************************************
*  End of main function                                                                            *
***************************************************************************************************/

function countDuplicateFiles(controlTable){
    let sql = getCheckDuplicateSQL(controlTable);
    return executeSingleValueQuery("DUPLICATES", sql);
}

function claimFiles(controlTable, tries, filesAtOnce, sortOrder, uuid){
    let sql = getClaimFilesSQL(controlTable, tries, filesAtOnce, sortOrder, uuid);
    return executeFirstValueQuery(sql);
}

function loadFiles(controlTable, copyStatementsTable ,uuid){
    let fileSQL = getFileListSQL(controlTable, copyStatementsTable, uuid);
    let rs = getResultSet(fileSQL);
    rs.next();
    let sql = getCopyIntoSQL(rs.getColumnValue("FILE_LIST"), rs.getColumnValue("STATEMENT_TEXT"));
    return getResultSet(sql);
}

function markCompleteFiles(controlTable, fileRS){
    let loadResults = getLoadResults(fileRS)
    let sql = getMarkCompletedFilesSQL(controlTable, loadResults)
    executeNonQuery(sql);
}

function getLoadResults(filesRS){

    if (filesRS instanceof Error){
        throw filesRS;
    }

    let s = '';
    let f = [];
    let i = 0;
    
    let file = '';
    let status = '';
    let first_error = '';

    while(filesRS.next()){
        file        = filesRS.getColumnValue("file").replace(/'/g, "''");
        status      = filesRS.getColumnValue("status").replace(/'/g, "''");
        first_error = filesRS.getColumnValue("first_error");
        
        if (first_error != null){
            first_error = EscapeLiteralString(first_error);
        } 
        else {
            first_error = "";
        }
        f.push( "('" + file + "','" + status + "','" + first_error + "')");
    }

    for (i = 0; i < f.length - 1; i++){
        s += f[i] + ",\n";
    }
    s += f[i];

    return s;
}

function getCopyFileList(rs, filesAtOnce){

    let s = '';

    for (var i = 0; i < filesAtOnce; i++){
        if (rs.next()){
            if (i > 0) {
                s += ',';
            }
            s += "'" + shortenFilePath(rs.getColumnValue("FILE_PATH")) + "'";
        } else {
            break;
        }
    }
    return s;
}

function shortenFilePath(filePath){

    let doubleSlash = filePath.indexOf("//");

    if (doubleSlash == -1){
        return filePath.substring(filePath.indexOf("/") + 1);
    }
    else{
        return filePath.substring(filePath.indexOf("/", doubleSlash + 2) + 1);
    } 
}

function getControlQuery(CONTROL_TABLE, TRIES, ORDER_BY, FILES_TO_PROCESS) {

    return "select FILE_PATH from " + CONTROL_TABLE + " where INGESTION_STATUS = 'WAITING' and TRY_COUNT < " + TRIES +
           " order by INGESTION_ORDER " + ORDER_BY + " limit " + FILES_TO_PROCESS + ";";

}

/***************************************************************************************************
*  Error and Exception Handling                                                                    *
***************************************************************************************************/
function checkParameters(filesToProcess, filesAtOnce, sortOrder, maxRunMinutes, tries){

    if(filesToProcess <= 0){
        return "FILES_TO_PROCESS parameter must be greater than 0,";
    }
    if(filesAtOnce < 1 || filesAtOnce > 1000){
        return "FILES_AT_ONCE parameter must be between 1 and 1000.";
    }
    if(sortOrder.trim().toUpperCase() != "ASC" && sortOrder.trim().toUpperCase() != "DESC"){
        return "SORT_ORDER must be 'ASC' or 'DESC' only.";
    }
    if(maxRunMinutes <= 0){
        return "MAX_RUN_MINUTES must be greater than 0.";
    }
    if(tries < 1){
        return "TRIES must be one or greater.";
    }

    return "No_Errors";
}

/***************************************************************************************************
*  SQL Template Functions                                                                          *
***************************************************************************************************/

function getCheckDuplicateSQL(controlTable){

return `
select  count(FILE_PATH)                as FILES_COUNT,
        count(distinct FILE_PATH)       as FILES_DISTINCT,
        FILES_COUNT - FILES_DISTINCT    as DUPLICATES
from    ${controlTable};
`;

}

function getClaimFilesSQL(controlTable, tries, filesAtOnce, sortOrder, uuid){

return `
-- CLAIM NEXT FILES
update FILE_INGEST_CONTROL
set    INGESTION_STATUS = 'LOADING',
       OWNER_SESSION    = current_session(),
       EXEC_UUID        = '${uuid}',
       TRY_COUNT        = TRY_COUNT + 1,
       START_TIME       = to_timestamp_ntz(current_timestamp())
where  FILE_PATH in
(
    with 
    FIRST_FILE(COPY_INTO_NAME) as
    (
        select   COPY_INTO_NAME
        from     FILE_INGEST_CONTROL
        where    INGESTION_STATUS = 'WAITING' and 
                 TRY_COUNT < ${tries}
        order by INGESTION_ORDER ${sortOrder}
        limit    1
    ),
    FILE_LIST(FILE_PATH, COPY_INTO_NAME) as
    (
        select   FILE_PATH,
                 COPY_INTO_NAME
        from     FILE_INGEST_CONTROL
        where    INGESTION_STATUS = 'WAITING' and
                 TRY_COUNT < ${tries}
        order by INGESTION_ORDER ${sortOrder}
    )
    select  FILE_PATH
    from    FILE_LIST L,
            FIRST_FILE F
    where   L.COPY_INTO_NAME = F.COPY_INTO_NAME
    limit   ${filesAtOnce}
)
`;
}

function getFileListSQL(controlTable, copyStatementsTable, uuid){

return `
select   listagg('\\n\\'' || stage_path_shorten(F.FILE_PATH) || '\\'', ',') as FILE_LIST,
         any_value(F.COPY_INTO_NAME) as COPY_INTO_NAME,
         any_value(C.STATEMENT_TEXT) as STATEMENT_TEXT
from     ${controlTable}  F
join     ${copyStatementsTable} C
    on   F.COPY_INTO_NAME = C.STATEMENT_NAME
where    INGESTION_STATUS = 'LOADING' and EXEC_UUID = '${uuid}' and
         C.EFFECTIVE <= current_timestamp and (C.SUPERSEDED is null or C.SUPERSEDED > current_timestamp)
order by INGESTION_ORDER;
`;
}

function getMarkCompletedFilesSQL(controlTable, loadResults){

return `
--MARK FINISHED FILES
merge into ${controlTable} C 
      using 
      (
        select FILE_PATH, LOAD_STATUS, FIRST_ERROR from (values 
${loadResults}
          )
        as L(FILE_PATH, LOAD_STATUS, FIRST_ERROR)
      ) L
      on C.FILE_PATH = L.FILE_PATH
when matched then
      update set 
      C.INGESTION_STATUS = L.LOAD_STATUS,
      C.ERROR_MSG        = L.FIRST_ERROR,
      C.END_TIME         = to_timestamp_ntz(current_timestamp()),
      C.EXEC_UUID        = null;
`;
}
  
function getCopyIntoSQL(fileList, copyTemplate){

return `${copyTemplate}\nfiles=(${fileList})`;

}

/***************************************************************************************************
*  SQL functions                                                                                   *
***************************************************************************************************/

function getResultSet(sql){
    let cmd  = {sqlText: sql};
    let stmt = snowflake.createStatement(cmd);
    let rs   = stmt.execute();
    return rs;
}

function executeNonQuery(queryString) {
    let out = '';
    let cmd = {sqlText: queryString};
    let stmt = snowflake.createStatement(cmd);
    let rs = stmt.execute();
}

function executeSingleValueQuery(columnName, queryString) {
    let cmd  = {sqlText: queryString};
    let stmt = snowflake.createStatement(cmd);
    let rs   = stmt.execute();
    rs.next();
    return rs.getColumnValue(columnName);
}

function executeFirstValueQuery(queryString) {
    let cmd  = {sqlText: queryString};
    let stmt = snowflake.createStatement(cmd);
    let rs   = stmt.execute();
    rs.next();
    return rs.getColumnValue(1);
}

/***************************************************************************************************
*  Library functions                                                                               *
***************************************************************************************************/

function escapeLiteralString(str){
    str = str.replace(/'/g, "''");
    str = str.replace(/\\/g, "\\\\");
    str = str.replace(/(\r\n|\n|\r)/gm," ");
    return str;
}

function getUUIDv4() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, 
        function(c){
            var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
            return v.toString(16);
        }
    );
}

function date2Timestamp(date){

    let yyyy = date.getFullYear();
    let dd   = date.getDate();
    let mm   = (date.getMonth() + 1);

    if (dd < 10) dd = "0" + dd;
    if (mm < 10) mm = "0" + mm;

    let cur_day = yyyy + "-" + mm + "-" + dd;
    let hours   = date.getHours()
    let minutes = date.getMinutes()
    let seconds = date.getSeconds();

    if (hours < 10) hours = "0" + hours;
    if (minutes < 10) minutes = "0" + minutes;
    if (seconds < 10) seconds = "0" + seconds;

    return cur_day + " " + hours + ":" + minutes + ":" + seconds;
}
$$;

/****************************************************************************************************
*  Function to display a progress bar in a column.                                                  *
****************************************************************************************************/
create or replace function PROGRESS_BAR(PERCENTAGE float, DECIMALS float, SEGMENTS float)
returns string
language javascript
strict immutable
as
$$
    let percent = PERCENTAGE;

    if (isNaN(percent)) percent =   0;
    if (percent < 0)    percent =   0;
    if (percent > 100)  percent = 100;

    percent = percent.toFixed(DECIMALS);

    let filledSegments = Math.round(SEGMENTS * (percent / 100));
    let emptySegments  = SEGMENTS - filledSegments;

    let bar = '⬛'.repeat(filledSegments) + '⬜'.repeat(emptySegments);

    return bar + " " + percent + "%";
$$;
 
-- This is an overload with only the percentage, using defaults for 
-- number of segments and decimal points to display on percentage.
create or replace function PROGRESS_BAR(PERCENTAGE float)
returns string
language sql
as
$$
    select progress_bar(PERCENTAGE, 2, 10)
$$;
 
-- This is an overload with the percentage and the option set for the
-- number of decimals to display. It uses a default for number of segments.
create or replace function PROGRESS_BAR(PERCENTAGE float, DECIMALS float)
returns string
language sql
as
$$
    select progress_bar(PERCENTAGE, DECIMALS, 10)
$$;

/****************************************************************************************************
*  Returns the number of nodes for a given named cluster size                                       *
****************************************************************************************************/
create or replace function NODES_PER_WAREHOUSE(WAREHOUSE_SIZE string)
returns integer
language SQL
as
$$
    case upper(WAREHOUSE_SIZE)
        when 'X-SMALL'  then 1
        when 'XSMALL'   then 1
        when 'XS'       then 1
        when 'SMALL'    then 2
        when 'S'        then 2
        when 'MEDIUM'   then 4
        when 'M'        then 4
        when 'LARGE'    then 8
        when 'L'        then 8
        when 'X-LARGE'  then 16
        when 'XLARGE'   then 16
        when 'XL'       then 16
        when '2X-LARGE' then 32
        when '2XLARGE'  then 32
        when '2XL'      then 32
        when '3X-LARGE' then 64
        when '3XLARGE'  then 64
        when '3XL'      then 64
        when '4X-LARGE' then 128
        when '4XLARGE'  then 128
        when '4XL'      then 128
        when '5X-LARGE' then 256
        when '5XLARGE'  then 256
        when '5XL'      then 256
        when '6X-LARGE' then 512
        when '6XLARGE'  then 512
        when '6XL'      then 512
        else            null
    end
$$;
         
/****************************************************************************************************
*  Convert the last modified value from the Snowflake LIST command into a timestamp.                *
****************************************************************************************************/
create or replace function LAST_MODIFIED_TO_TIMESTAMP(LAST_MODIFIED string) 
returns timestamp_tz
language sql
as
$$
    to_timestamp_tz(left(LAST_MODIFIED, len(LAST_MODIFIED) - 4) || ' ' || '00:00', 'DY, DD MON YYYY HH:MI:SS TZH:TZM')
$$;
