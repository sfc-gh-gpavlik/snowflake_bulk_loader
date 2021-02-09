-- Worksheet 02.Bulk Load - Runtime
-- Last modified 2020-04-17

/****************************************************************************************************
*                                                                                                   *
*                              ***  Snowflake Bulk Load Project  ***                                *
*                                                                                                   *                                                                           *
*  Provide feedback to greg.pavlik at snowflake.com                                                 *
*                                                                                                   *
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
*               3) Copy your working COPY INTO statement and paste it into the "GetCopyTemplate"    *
*                  User Defined Function.NOTE: Leave "files=( @~FILE_LIST~@ )" at the end of your   *
*                  COPY INTO statement.                                                             *
*               4) ==> IMPORTANT <== Run the create or replace function GetCopyTemplate()           *
*                  If you don't run it after modifying it, calling the function will return the     *
*                  sample value. This will be confusing to troubleshoot since the COPY INTO         *
*                  statement will look right. Be sure to run create or replace GetCopyTemplate()    *
*               5) Run a single instance of the FileIngest stored procedure with a small number of  *
*                  files specified in the parameters.                                               *
*               6) Examine results, and if everything looks good run multiple copies of the stored  *
*                  procedure in parallel EACH USING ITS OWN WAREHOUSE. It defeats the purpose of    *
*                  the project if they run in the same warehouse. Increase the number of files in   *
*                  testing until the SP runs at least five minutes per run.                         *
*               7) Automate running a bulk load. Create multiple Snowflake TASKs to run the         *
*                  FileIngest stored procedure. Ensure that each TASK runs in a different           *
*                  warehouse. Set the tasks to run once per minute. (They will not re-run if they   *
*                  are still running from the last execution.) Set the stored procedure to run      *
*                  a long time, perhaps 60 minutes.                                                 *
*               8) ===> IMPORTANT <=== When the table is fully loaded, suspend the TASKs.           *
*                                                                                                   *
****************************************************************************************************/

create or replace function GetCopyTemplate()
returns string
language javascript
as
$$
/****************************************************************************************************
*                                                                                                   *
*   This is a simple COPY INTO statement. Use yours, but you must keep the                          *
*   files=( @~FILE_LIST~@ ) part in the statement. Never use that token in the comments.            *
*   The stored procedure will replace the token with a list of files.                               *
*                                                                                                   *
*   =====> IMPORTANT! <===== Run this create or replace after modifying.                            *
*                                                                                                   *
****************************************************************************************************/
return `
-- Do not modify this UDF above this line.
----------------------------------------------------------------------------------------------------


copy into TARGET_TABLE from @TEST_STAGE file_format=(type=CSV) files=( @~FILE_LIST~@ ) ;            --    <=== Replace with your COPY INTO statement


----------------------------------------------------------------------------------------------------
-- Do not modify this UDF below this line.
`;
$$;

-- Required User Defined Function.
create or replace function STAGE_PATH_SHORTEN(FILE_PATH string)
returns string
language javascript
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

-- Create a control table to drive the stored procedure. 
-- (This may be a good place to put the killswitch,)
-- (Actually, just change "WAITING" to "SUSPENDED")
create or replace table FILE_INGEST_CONTROL 
    (
    FILE_PATH           string,                         -- The file path in the stage                                   
    INGESTION_ORDER     timestamp_tz,                   -- Can be any sortable data type. Used only for ORDER BY.       
    INGESTION_STATUS    string      default 'WAITING',  -- Status set by COPY INTO results.                             
    OWNER_SESSION       integer,                        -- The session_id running this stored procedure.                
    EXEC_UUID           string,                         -- A unique separator in case two SPs run in the same session   
    TRY_COUNT           integer     default 0,          -- The number of times a file has been tried in a COPY INTO     
    START_TIME          timestamp,                      -- The last time a file was sent as part of a COPY INTO         
    END_TIME            timestamp,                      -- The last a COPY INTO returned with this file sent to it      
    FILE_SIZE           bigint,                         -- Used to collect statistics. Not needed or used by this SP.   
    ERROR_MSG           string                          -- The error message (if any) on the most recent COPY INTO      
    );

-- Create the stored procedure to load the target table
create or replace procedure FILE_INGEST(
                                        STAGE_NAME          string,
                                        COPY_TEMPLATE       string,
                                        CONTROL_TABLE       string,
                                        SORT_ORDER          string,
                                        FILES_TO_PROCESS    float,
                                        FILES_AT_ONCE       float,
                                        MAX_RUN_MINUTES     float,
                                        TRIES               float
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
* @param  {string}  STAGE_NAME:         The name of the stage from which to load files              *
* @param  {string}  TARGET_TABLE:       The table to load from the stage files                      *
* @param  {string}  COPY_TEMPLATE:      The name of the UDF to call to get the COPY INTO template   *
* @param  {string}  CONTROL_TABLE:      The name of the table that controls this procedure          *
* @param  {string}  SORT_ORDER:         The column that controls the order of file loading          *
* @param  {string}  FILES_TO_PROCESS:   The total number of files to process in one procedure run   *
* @param  {float}   FILES_AT_ONCE:      The number of files to load in a single transaction         *
* @param  {float}   MAX_RUN_MINUTES:    The maximum run time allowed for a new pass to start        *
* @param  {float}   TRIES float         The times to try loading a file, If > 1 it will retry       *
* @return {string}:                     A JSON with statistics from the execution.                  *
*                                                                                                   *
****************************************************************************************************/

    var out = {};
    
    var parameterError = CheckParameters(FILES_TO_PROCESS, FILES_AT_ONCE, SORT_ORDER, MAX_RUN_MINUTES, TRIES);
    
    if(parameterError != "No_Errors"){
        out["Parameter_Error"] = parameterError;
        return out;
    }

    var i = 0;
    var filesClaimed = -1;
    var filesProcessed = 0;
    var filesRS;
    var passes = Math.ceil(FILES_TO_PROCESS / FILES_AT_ONCE);

    var uuid = GetUUIDv4();
    var copyTemplate = ExecuteSingleValueQuery("TEMPLATE", "select " + COPY_TEMPLATE + "() as TEMPLATE;");

    var endTime = new Date().getTime() + MAX_RUN_MINUTES * 60000;
    var isEndTime = 0;

    out["Session_ID"] = ExecuteSingleValueQuery("SESSION", "select current_session() as SESSION;");
    out["Start_Time"] = Date2Timestamp(new Date());

    for (i = 0; i < passes && isEndTime == 0; i++){

        filesClaimed = ClaimFiles(CONTROL_TABLE, TRIES, FILES_AT_ONCE, SORT_ORDER, uuid);
        if (filesClaimed == 0){
            out["Termination_Reason"] = "Processed_All_Waiting_Files";            
            break;  // No more files to claim. All files processed.
        }
        filesProcessed += filesClaimed;

        filesRS = LoadFiles(CONTROL_TABLE, copyTemplate, uuid);

        MarkCompleteFiles(CONTROL_TABLE, filesRS);

        if (new Date().getTime() >= endTime){
            out["Termination_Reason"] = "Time_Limit";
            isEndTime = 1;
        }
    }

    if (filesProcessed >= FILES_TO_PROCESS){
        out["Termination_Reason"] = "File_Limit";
    }

    out["Files_Processed"] = filesProcessed;
    out["End_Time"] = Date2Timestamp(new Date());
    out["UUID"] = uuid;

    return out;

/***************************************************************************************************
*                                                                                                  *
*  End of main function                                                                            *
*                                                                                                  *
***************************************************************************************************/

function ClaimFiles(controlTable, tries, filesAtOnce, sortOrder, uuid){

    sql = GetClaimFilesSQL(controlTable, tries, filesAtOnce, sortOrder, uuid);

    return ExecuteFirstValueQuery(sql);
}

function LoadFiles(controlTable, copyTemplate, uuid){

    var fileSQL = GetFileListSQL(controlTable, uuid);

    var fileList = ExecuteSingleValueQuery("FILE_LIST", fileSQL);

    sql = GetCopyIntoSQL(copyTemplate, fileList);

    return GetResultSet(sql);
}

function MarkCompleteFiles(controlTable, fileRS){

    var loadResults = GetLoadResults(fileRS)

    var sql = GetMarkCompletedFilesSQL(controlTable, loadResults)

    // CHANGE THIS TO GET THE UPDATED ROWS 
    ExecuteNonQuery(sql);

}

function GetLoadResults(filesRS){

    var s = '';
    var f = [];
    var i = 0;

    while(filesRS.next()){
        f.push( "('" + filesRS.getColumnValue("file") + "','" +
                       filesRS.getColumnValue("status") + "','" +
                       filesRS.getColumnValue("first_error") + "')");
    }
    
    for (i = 0; i < f.length - 1; i++){
        s += f[i] + ",\n";
    }
    s += f[i];

    return s;
}

function GetCopyFileList(rs, filesAtOnce){

    var s = '';

    for (var i = 0; i < filesAtOnce; i++){
        if (rs.next()){
            if (i > 0) {
                s += ',';
            }
            s += "'" + ShortenFilePath(rs.getColumnValue("FILE_PATH")) + "'";
        } else {
            break;
        }
    }
    return s;
}

function ShortenFilePath(filePath){

    var doubleSlash = filePath.indexOf("//");

    if (doubleSlash == -1){
        return filePath.substring(filePath.indexOf("/") + 1);
    }
    else{
        return filePath.substring(filePath.indexOf("/", doubleSlash + 2) + 1);
    } 
}

function GetControlQuery(CONTROL_TABLE, TRIES, ORDER_BY, FILES_TO_PROCESS) {

    return "select FILE_PATH from " + CONTROL_TABLE + " where INGESTION_STATUS = 'WAITING' and TRY_COUNT < " + TRIES +
           " order by INGESTION_ORDER " + ORDER_BY + " limit " + FILES_TO_PROCESS + ";";

}

/***************************************************************************************************
*                                                                                                  *
*  Error and Exception Handling                                                                    *
*                                                                                                  *
***************************************************************************************************/
function CheckParameters(filesToProcess, filesAtOnce, sortOrder, maxRunMinutes, tries){

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
*                                                                                                  *
*  SQL Template Functions                                                                          *
*                                                                                                  *
***************************************************************************************************/

function GetClaimFilesSQL(controlTable, tries, filesToProcess, sortOrder, uuid){

var sql = 
`
-- CLAIM NEXT FILES
update @~CONTROL_TABLE~@ set
       INGESTION_STATUS = 'LOADING',
       OWNER_SESSION    = current_session(),
       EXEC_UUID        = '@~EXEC_UUID~@',
       TRY_COUNT        = TRY_COUNT + 1,
       START_TIME       = to_timestamp_ntz(current_timestamp())
where  
       (EXEC_UUID is null or EXEC_UUID = '@~EXEC_UUID~@')
            and
       (TRY_COUNT < @~TRIES~@)
            and
       FILE_PATH in 
       (
         select   FILE_PATH
         from     @~CONTROL_TABLE~@
         where    INGESTION_STATUS = 'WAITING'    -- Need to "or" other options here such as error notices
         order by INGESTION_ORDER @~SORT_ORDER~@
         limit    @~FILES_AT_ONCE~@
       );
`;

sql = sql.replace(/@~CONTROL_TABLE~@/g,     controlTable);
sql = sql.replace(/@~TRIES~@/g,             tries);
sql = sql.replace(/@~FILES_AT_ONCE~@/g,     filesToProcess);
sql = sql.replace(/@~SORT_ORDER~@/g,        sortOrder);
sql = sql.replace(/@~EXEC_UUID~@/g,         uuid);

return sql;
}

function GetFileListSQL(controlTable, uuid){

var sql =
`
select   listagg('\\n\\'' || stage_path_shorten(FILE_PATH) || '\\'', ',')
         as FILE_LIST
from     @~CONTROL_TABLE~@ 
where    INGESTION_STATUS = 'LOADING' and EXEC_UUID = '@~EXEC_UUID~@'
order by INGESTION_ORDER;
`;

sql = sql.replace(/@~CONTROL_TABLE~@/g, controlTable);
sql = sql.replace(/@~EXEC_UUID~@/g, uuid);

return sql;
}

function GetMarkCompletedFilesSQL(fileIngestControlTable, loadResults){

var sql = 
`
--MARK FINISHED FILES
merge into @~CONTROL_TABLE~@ C 
      using 
      (
        select FILE_PATH, LOAD_STATUS, FIRST_ERROR from (values 
@~LOAD_RESULTS~@
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

sql = sql.replace(/@~CONTROL_TABLE~@/g, fileIngestControlTable);
sql = sql.replace(/@~LOAD_RESULTS~@/g, loadResults);

return sql;
}

function GetCopyIntoSQL(copyTemplate, fileList){

var sql = copyTemplate.replace("@~FILE_LIST~@", fileList);

return sql;
}

/***************************************************************************************************
*                                                                                                  *
*  SQL functions                                                                                   *
*                                                                                                  *
***************************************************************************************************/

function GetResultSet(sql){
    cmd1 = {sqlText: sql};
    stmt = snowflake.createStatement(cmd1);
    var rs;
    rs = stmt.execute();
    return rs;
}

function ExecuteNonQuery(queryString) {
    var out = '';
    cmd1 = {sqlText: queryString};
    stmt = snowflake.createStatement(cmd1);
    var rs;

    rs = stmt.execute();
}

function ExecuteSingleValueQuery(columnName, queryString) {
    var out;
    cmd1 = {sqlText: queryString};
    stmt = snowflake.createStatement(cmd1);
    var rs;
    try{
        rs = stmt.execute();
        rs.next();
        return rs.getColumnValue(columnName);
    }
    catch(err) {
        if (err.message.substring(0, 18) == "ResultSet is empty"){
            throw "ERROR: No rows returned in query.";
        } else {
            throw "ERROR: " + err.message.replace(/\n/g, " ");
        } 
    }
    return out;
}

function ExecuteFirstValueQuery(queryString) {
    var out;
    cmd1 = {sqlText: queryString};
    stmt = snowflake.createStatement(cmd1);
    var rs;
    try{
        rs = stmt.execute();
        rs.next();
        return rs.getColumnValue(1);
    }
    catch(err) {
        if (err.message.substring(0, 18) == "ResultSet is empty"){
            throw "ERROR: No rows returned in query.";
        } else {
            throw "ERROR: " + err.message.replace(/\n/g, " ");
        } 
    }
    return out;
}

/***************************************************************************************************
*                                                                                                  *
*  Library functions                                                                               *
*                                                                                                  *
***************************************************************************************************/

function GetUUIDv4() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, 
        function(c){
            var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
            return v.toString(16);
        }
    );
}

function Date2Timestamp(date){

    var yyyy = date.getFullYear();
    var dd   = date.getDate();
    var mm   = (date.getMonth() + 1);

    if (dd < 10)
        dd = "0" + dd;

    if (mm < 10)
        mm = "0" + mm;

    var cur_day = yyyy + "-" + mm + "-" + dd;

    var hours   = date.getHours()
    var minutes = date.getMinutes()
    var seconds = date.getSeconds();

    if (hours < 10)
        hours = "0" + hours;

    if (minutes < 10)
        minutes = "0" + minutes;

    if (seconds < 10)
        seconds = "0" + seconds;

    return cur_day + " " + hours + ":" + minutes + ":" + seconds;
}
$$;
