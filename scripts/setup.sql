--contents to run in the consumer account
CREATE APPLICATION ROLE IF NOT EXISTS ckan_app_role;
CREATE SCHEMA IF NOT EXISTS core;
GRANT USAGE ON SCHEMA core TO APPLICATION ROLE ckan_app_role;
CREATE OR ALTER VERSIONED SCHEMA code_schema;
GRANT USAGE ON SCHEMA code_schema TO APPLICATION ROLE ckan_app_role;
CREATE STAGE IF NOT EXISTS core.published_extracts encryption = (type = 'SNOWFLAKE_SSE');
GRANT ALL ON STAGE core.published_extracts TO APPLICATION ROLE ckan_app_role;

CREATE OR REPLACE STREAMLIT code_schema.CKAN_OPEN_DATA_CONNECTOR
  FROM '/streamlit'
  MAIN_FILE = '/main.py'
;
GRANT USAGE ON STREAMLIT code_schema.CKAN_OPEN_DATA_CONNECTOR TO APPLICATION ROLE ckan_app_role;

CREATE TABLE IF NOT EXISTS core.resources (
owner_org string NOT NULL
,database_name string not null
,schema_name string not null
,table_name string NOT NULL
,package_id string not NULL
,resource_id string not null
,presigned_url string NULL
,date_updated timestamp default CURRENT_TIMESTAMP()
,file_name string null
,extension string NOT NULL
,compressed string not null
);
CREATE TABLE IF NOT EXISTS core.ckan_log (dt timestamp_ltz, packageid string, resourceid string, table_name string, message string);
--Settings captured at runtime, e.g. the EAI name passed to FINALIZE so the UI can rebuild the CKAN UDFs after a URL or key change.
CREATE TABLE IF NOT EXISTS core.app_config (key string, value string, updated timestamp_ltz);
CREATE STREAM IF NOT EXISTS core.resources_stream on table core.resources;

CREATE SCHEMA IF NOT EXISTS config;
GRANT USAGE ON SCHEMA config TO APPLICATION ROLE ckan_app_role;

CREATE OR REPLACE PROCEDURE CONFIG.register_reference(ref_name STRING, operation STRING, ref_or_alias STRING)
  RETURNS STRING
  LANGUAGE SQL
  AS $$
    BEGIN
      CASE (operation)
        WHEN 'ADD' THEN
          SELECT SYSTEM$ADD_REFERENCE(:ref_name, :ref_or_alias);
        WHEN 'REMOVE' THEN
          SELECT SYSTEM$REMOVE_REFERENCE(:ref_name);
        WHEN 'CLEAR' THEN
          SELECT SYSTEM$REMOVE_REFERENCE(ref_name);
      ELSE
        RETURN 'unknown operation: ' || operation;
      END CASE;
      RETURN NULL;
    END;
  $$;


GRANT USAGE ON PROCEDURE CONFIG.register_reference(STRING, STRING, STRING)
  TO APPLICATION ROLE ckan_app_role;

CREATE OR REPLACE PROCEDURE CONFIG.FINALIZE(EXTERNAL_ACCESS_OBJECT string, CKAN_URL string)
returns string
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'create_functions'
AS
$$
import os
import logging
logger = logging.getLogger("python_logger")
def create_functions(session, external_access_object, ckan_url):
  try:
    files = ['get_orgs.sql','package_search.sql','resource_update.sql','resource_renew_url.sql']
    for f in files:
      create_function(session,external_access_object,'/scripts/function_ddls/' + f, ckan_url)
    #Remember what the UDFs were built with so they can be rebuilt from the app UI.
    for k, v in (('eai_name', external_access_object), ('ckan_url', ckan_url)):
      session.sql("DELETE FROM core.app_config WHERE key = ?", params=[k]).collect()
      session.sql("INSERT INTO core.app_config SELECT ?, ?, current_timestamp()", params=[k, v]).collect()
    #Renewal now depends on resource_renew_url, which must exist before creating its task.
    session.sql("CALL config.ensure_url_renewal_task()").collect()
    return "Finalization complete"
  except Exception as ex:
        logger.error(ex)
        raise ex

def create_function(session, external_access_object, filename, ckan_url):
    file = session.file.get_stream(filename)
    create_function_ddl = file.read(-1).decode("utf-8")
    create_function_ddl = create_function_ddl.format(external_access_object, ckan_url)
    session.sql("begin " + create_function_ddl + " end;").collect()
    return f'{filename} created'    
$$;
GRANT USAGE ON PROCEDURE CONFIG.FINALIZE(string, string) to application role ckan_app_role;

CREATE OR REPLACE PROCEDURE CONFIG.create_vwh_objects(vwh string)
returns string
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'create_functions'
AS
$$
import os

def create_functions(session, vwh,):
    #Serverless; the vwh argument is kept for compatibility with earlier versions and is not used.
    files = ['renew_urls_task.sql']
    for f in files:
      create_function(session,vwh,'/scripts/function_ddls/' + f)
    return "VWH Dependent objects complete"

def create_function(session, vwh, filename):
    file = session.file.get_stream(filename)
    create_function_ddl = file.read(-1).decode("utf-8")

    create_function_ddl = create_function_ddl.format(vwh)
    session.sql("begin " + create_function_ddl + " end;").collect()
    return f'{filename} created'       
$$;

GRANT USAGE ON PROCEDURE CONFIG.create_vwh_objects(string) to application role ckan_app_role;

CREATE OR REPLACE PROCEDURE CONFIG.create_vwh_objects_tname(tname string, cron string)
returns string
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'create_functions'
AS
$$
import os

def create_functions(session,tname,cron):
    files = ['refresh_urls_onchange.sql']
    for f in files:
      create_function(session,tname, cron,'/scripts/function_ddls/' + f)
    return "VWH Dependent objects complete"

def create_function(session, tname, cron, filename):
    file = session.file.get_stream(filename)
    create_function_ddl = file.read(-1).decode("utf-8")

    create_function_ddl = create_function_ddl.format(tname,cron)
    session.sql("begin " + create_function_ddl + " end;").collect()
    #Make sure the presigned url renewal task exists once the app can create tasks.
    session.sql("call config.ensure_url_renewal_task()").collect()
    return f'{filename} created'       
$$;

GRANT USAGE ON PROCEDURE CONFIG.create_vwh_objects_tname(string,string) to application role ckan_app_role;

CREATE OR REPLACE FUNCTION config.add_quotes(object_name string)
RETURNS STRING
LANGUAGE SQL
AS
$$ 
  '"'|| object_name ||'"'
$$;
GRANT USAGE ON FUNCTION config.add_quotes(string) to application role ckan_app_role;

CREATE OR REPLACE PROCEDURE CONFIG.unload_to_internal_stage(extension string
                                                            , compressed string
                                                            , file_alias string
                                                            , table_name string
                                                            , FQTN string)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
DECLARE
    full_ext string default '';
    formatOptions string default '';
BEGIN
     //MAP compression algorithm to an extension
      //brotli =.br
      //zstd = .zst
      //gzip = .gz
      //deflate =.zz
      //raw_deflate .rzz ??
      //.SNAPPY
      //.LZO
      full_ext := '.'||extension;
      let cmp string := compressed;
      CASE (cmp) 
        when 'brotli' then 
          full_ext := full_ext || '.br';
        when 'zstd' then 
          full_ext := full_ext || '.zst';
        when 'gzip' then 
          full_ext := full_ext || '.gz';
        when 'deflate' then 
          full_ext := full_ext || '.zz';
        when 'raw_deflate' then 
          full_ext := full_ext || '.rzz';
        when 'None' then 
            full_ext := full_ext;
        else 
            full_ext := full_ext ||'.'|| compressed;
      END CASE;
      //fileformats options are different based on type
      let ext string := extension;
      CASE (ext)
        WHEN 'csv' THEN
          formatOptions := ' NULL_IF=('''') EMPTY_FIELD_AS_NULL = FALSE FIELD_OPTIONALLY_ENCLOSED_BY=''\042''';
        ELSE
          formatOptions := ' ';
      END CASE;
      
      let file_name string := replace(replace(IFNULL(file_alias,table_name),'"',''),' ','_');
      --UNLOAD DATA TO A FILE
      execute immediate ('copy into @core.published_extracts/' ||
        file_name || :full_ext || ' from ' ||
        FQTN || ' SINGLE = TRUE MAX_FILE_SIZE=5368709120 OVERWRITE=TRUE HEADER=TRUE file_format = (TYPE = '||
        extension||' COMPRESSION = '|| compressed||' '||formatOptions||')');
      
      return file_name;
END;
GRANT USAGE ON PROCEDURE CONFIG.unload_to_internal_stage(string,string,string,string,string) to application role ckan_app_role;

CREATE OR REPLACE PROCEDURE CONFIG.SP_UPDATE_RESOURCES(tname string)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
DECLARE
    TABLES RESULTSET DEFAULT(select config.add_quotes(database_name) db_name
                                , config.add_quotes(schema_name) sch_name
                                , config.add_quotes(table_name) tbl_name
                                , db_name||'.'||sch_name||'.'||tbl_name FQTN 
                                ,file_name
                                ,extension
                                ,compressed
                            from core.resources_stream
                            where table_name = :tname);
                            
BEGIN
    FOR tbl IN tables DO
      let ext string := tbl.extension;
      let com string := tbl.compressed;
      let fname string := tbl.file_name;
      let tname string := tbl.tbl_name;
      let fqtn string := tbl.FQTN;
      //writes a file to an internal stage
      
      SYSTEM$LOG_INFO('Unloading file '||:fname||' to internal stage for table '||:fqtn);
        
      CALL config.unload_to_internal_stage(:ext,:com,:fname,:tname,:fqtn);
    END FOR;
    
    let sql string := $$
    UPDATE CORE.RESOURCES
    SET presigned_url = purl
        ,date_updated = CURRENT_TIMESTAMP()
    FROM (
            SELECT get_presigned_url(@core.published_extracts, replace(replace(IFNULL(file_name,table_name),'"',''),' ','_') ||'.'||extension || 
      CASE compressed 
        when 'brotli' then '.br'
        when 'zstd' then '.zst'
        when 'gzip' then '.gz'
        when 'deflate' then '.zz'
        when 'raw_deflate' then '.rzz'
        when 'None' then ''
        else '.'|| compressed
      END,604800) purl
            ,database_name
            ,schema_name
            ,table_name
            FROM core.resources_stream 
            WHERE METADATA$ACTION = 'INSERT'
        ) r
    WHERE r.database_name = RESOURCES.database_name
    AND r.schema_name = RESOURCES.schema_name
    AND r.table_name = RESOURCES.table_name$$;
    SYSTEM$LOG_INFO('Add file information to file and update resource table: '|| :sql);
    execute immediate(:sql);

    SYSTEM$LOG_INFO('Make API call to CKAN and regenerate presigned URL');
    --resource_update returns the CKAN resource on success, or {error, status} without an id on failure.
    INSERT INTO core.ckan_log
      SELECT current_timestamp(), package_id
      ,COALESCE(resp:id::string, resource_id)
      ,table_name
      ,IFF(resp:id IS NULL, 'resource update failed at CKAN: ' || COALESCE(resp:error::string, resp::string, 'no response'), 'presigned url updated at CKAN')
      FROM (SELECT rs.package_id, rs.resource_id, rs.table_name
                  ,parse_json(config.resource_update(rs.resource_id,rs.extension,rs.presigned_url)) resp
            FROM core.resources_stream rs
            WHERE metadata$action='INSERT'
            AND presigned_url is not null);

    insert into core.ckan_log 
    select current_timestamp(),package_id,resource_id,table_name,'SP_UPDATE_RESOURCES COMPLETE' 
    from core.resources
      where table_name = :tname;

    return 'SUCCESS';
    
EXCEPTION
  when other then
    let err := object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
    SYSTEM$LOG_ERROR(:err);
    insert into core.ckan_log select localtimestamp(), package_id, resource_id, table_name,:err::string 
    from core.resources;
    SYSTEM$LOG_ERROR(:err::string);
    return 'FAILURE';
END;

GRANT USAGE ON PROCEDURE CONFIG.SP_UPDATE_RESOURCES(string) to application role ckan_app_role;

CREATE OR REPLACE PROCEDURE CONFIG.SP_UPDATE_RESOURCES_ALL()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
DECLARE
    TABLES RESULTSET DEFAULT(select config.add_quotes(database_name) db_name
                                , config.add_quotes(schema_name) sch_name
                                , config.add_quotes(table_name) tbl_name
                                , db_name||'.'||sch_name||'.'||tbl_name FQTN 
                                ,file_name
                                ,extension
                                ,compressed
                            from core.resources_stream);
                            
BEGIN
    FOR tbl IN tables DO
      let ext string := tbl.extension;
      let com string := tbl.compressed;
      let fname string := tbl.file_name;
      let tname string := tbl.tbl_name;
      let fqtn string := tbl.FQTN;
      //writes a file to an internal stage
      
      SYSTEM$LOG_INFO('Unloading file '||:fname||' to internal stage for table '||:fqtn);
        
      CALL config.unload_to_internal_stage(:ext,:com,:fname,:tname,:fqtn);
    END FOR;
    
    let sql string := $$
    UPDATE CORE.RESOURCES
    SET presigned_url = purl
        ,date_updated = CURRENT_TIMESTAMP()
    FROM (
            SELECT get_presigned_url(@core.published_extracts, replace(replace(IFNULL(file_name,table_name),'"',''),' ','_') ||'.'||extension || 
      CASE compressed 
        when 'brotli' then '.br'
        when 'zstd' then '.zst'
        when 'gzip' then '.gz'
        when 'deflate' then '.zz'
        when 'raw_deflate' then '.rzz'
        when 'None' then ''
        else '.'|| compressed
      END,604800) purl
            ,database_name
            ,schema_name
            ,table_name
            FROM core.resources_stream 
            WHERE METADATA$ACTION = 'INSERT'
        ) r
    WHERE r.database_name = RESOURCES.database_name
    AND r.schema_name = RESOURCES.schema_name
    AND r.table_name = RESOURCES.table_name$$;
    SYSTEM$LOG_INFO('Add file information to file and update resource table: '|| :sql);
    execute immediate(:sql);

    SYSTEM$LOG_INFO('Make API call to CKAN and regenerate presigned URL');
    --resource_update returns the CKAN resource on success, or {error, status} without an id on failure.
    INSERT INTO core.ckan_log
      SELECT current_timestamp(), package_id
      ,COALESCE(resp:id::string, resource_id)
      ,table_name
      ,IFF(resp:id IS NULL, 'resource update failed at CKAN: ' || COALESCE(resp:error::string, resp::string, 'no response'), 'presigned url updated at CKAN')
      FROM (SELECT rs.package_id, rs.resource_id, rs.table_name
                  ,parse_json(config.resource_update(rs.resource_id,rs.extension,rs.presigned_url)) resp
            FROM core.resources_stream rs
            WHERE metadata$action='INSERT'
            AND presigned_url is not null);

    insert into core.ckan_log 
    select current_timestamp(),package_id,resource_id,table_name,'SP_UPDATE_RESOURCES_ALL COMPLETE' 
    from core.resources;

    return 'SUCCESS';
    
EXCEPTION
  when other then
    let err := object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
    SYSTEM$LOG_ERROR(:err);
    insert into core.ckan_log select localtimestamp(), package_id, resource_id, table_name,:err::string 
    from core.resources;
    SYSTEM$LOG_ERROR(:err::string);
    return 'FAILURE';
END;

GRANT USAGE ON PROCEDURE CONFIG.SP_UPDATE_RESOURCES_ALL() to application role ckan_app_role;

--Presigned urls expire after 7 days. core.renew_presigned_urls_task renews all of them every 6 days. Earlier versions used
--core.refresh_urls_task for this (and needed a warehouse); drop it only after the new task was created, so there is never a
--gap in renewal. Task creation fails until the app holds EXECUTE TASK / EXECUTE MANAGED TASK, so this also runs on
--publish and redeploy, not only during setup.
CREATE OR REPLACE PROCEDURE CONFIG.ensure_url_renewal_task()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
BEGIN
    SHOW USER FUNCTIONS LIKE 'RESOURCE_RENEW_URL' IN SCHEMA config;
    LET has_renewal_udf INTEGER := (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    IF (has_renewal_udf = 0) THEN
        RETURN 'SKIPPED: external access not configured yet';
    END IF;
    CALL CONFIG.create_vwh_objects('');
    SHOW TASKS LIKE 'REFRESH_URLS_TASK' IN SCHEMA core;
    LET legacy_count INTEGER := (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    IF (legacy_count > 0) THEN
        DROP TASK IF EXISTS core.refresh_urls_task;
        INSERT INTO core.ckan_log (dt, message) VALUES (current_timestamp(), 'Legacy refresh_urls_task replaced by renew_presigned_urls_task');
    END IF;
    RETURN 'RENEWAL TASK READY';
EXCEPTION
    WHEN OTHER THEN
        LET err STRING := 'Presigned url renewal task not created yet: ' || SQLERRM;
        SYSTEM$LOG_WARN(:err);
        RETURN 'ERROR: ' || SQLERRM;
END;

GRANT USAGE ON PROCEDURE CONFIG.ensure_url_renewal_task() to application role ckan_app_role;

--Task DDL is not changed by an app upgrade, so rebuild one refresh task per published table from core.resources with the
--current code, keep each table's schedule, and drop tasks that do not belong to a published table (e.g. the unprefixed
--REFRESH_UPDATED_URLS_TASK from older versions, or _REFRESH_UPDATED_URLS_TASK created with an empty table name).
CREATE OR REPLACE PROCEDURE CONFIG.redeploy_tasks()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
DECLARE
    default_cron STRING DEFAULT '0 23 * * *';
    rebuilt INTEGER DEFAULT 0;
    dropped INTEGER DEFAULT 0;
    failed INTEGER DEFAULT 0;
BEGIN
    --Temporary tables are not allowed inside an application, so keep the SHOW TASKS query id and read it with RESULT_SCAN.
    SHOW TASKS LIKE '%REFRESH_UPDATED_URLS_TASK' IN SCHEMA core;
    LET qid STRING := (SELECT LAST_QUERY_ID());

    --Keep the table's own schedule; tables that only had an orphaned task inherit that task's schedule.
    LET tbls RESULTSET := (
        WITH t AS (SELECT "name" task_name, TRIM(REGEXP_REPLACE("schedule", '(USING CRON )|( America/Los_Angeles)')) cron
                   FROM TABLE(RESULT_SCAN(:qid)))
        SELECT r.table_name, COALESCE(t.cron, (SELECT MAX(cron) FROM t), :default_cron) cron
        FROM (SELECT DISTINCT table_name FROM core.resources) r
        LEFT JOIN t ON t.task_name = UPPER(r.table_name) || '_REFRESH_UPDATED_URLS_TASK');
    --Evaluated now, before the loop re-creates tasks.
    LET orphans RESULTSET := (
        SELECT "name" task_name FROM TABLE(RESULT_SCAN(:qid)) t
        WHERE NOT EXISTS (SELECT 1 FROM core.resources r WHERE t."name" = UPPER(r.table_name) || '_REFRESH_UPDATED_URLS_TASK'));

    FOR r IN tbls DO
        LET tname STRING := r.table_name;
        LET cron STRING := r.cron;
        BEGIN
            CALL CONFIG.create_vwh_objects_tname(:tname, :cron);
            rebuilt := rebuilt + 1;
        EXCEPTION
            WHEN OTHER THEN
                failed := failed + 1;
                LET task_err STRING := 'refresh task redeploy failed: ' || SQLERRM;
                INSERT INTO core.ckan_log (dt, table_name, message) VALUES (current_timestamp(), :tname, :task_err);
        END;
    END FOR;

    FOR o IN orphans DO
        LET task_id STRING := 'core."' || o.task_name || '"';
        DROP TASK IF EXISTS IDENTIFIER(:task_id);
        dropped := dropped + 1;
    END FOR;

    CALL CONFIG.ensure_url_renewal_task();
    LET msg STRING := 'Refresh tasks redeployed: ' || rebuilt || ' rebuilt, ' || dropped || ' orphaned removed, ' || failed || ' failed';
    INSERT INTO core.ckan_log (dt, message) VALUES (current_timestamp(), :msg);
    RETURN msg;
EXCEPTION
    WHEN OTHER THEN
        LET err STRING := 'refresh task redeploy failed: ' || SQLERRM;
        INSERT INTO core.ckan_log (dt, message) VALUES (current_timestamp(), :err);
        RETURN 'ERROR: ' || SQLERRM;
END;

GRANT USAGE ON PROCEDURE CONFIG.redeploy_tasks() to application role ckan_app_role;

--The CKAN UDFs are created at runtime by FINALIZE, so an upgrade does not change them. Rebuild them with the stored
--integration name and host so every upgrade ships the current UDF code. Skipped on new installs (no UDFs yet).
CREATE OR REPLACE PROCEDURE CONFIG.rebuild_ckan_functions()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
DECLARE
    eai STRING DEFAULT '';
    host STRING DEFAULT '';
BEGIN
    SHOW USER FUNCTIONS LIKE 'GET_ORGS' IN SCHEMA config;
    LET has_udfs INTEGER := (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    SHOW USER FUNCTIONS LIKE 'CKAN_URL_FN' IN SCHEMA core;
    LET has_url INTEGER := (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    IF (has_udfs = 0 OR has_url = 0) THEN
        RETURN 'SKIPPED: external access not configured yet';
    END IF;

    --Installs finalized before app_config existed used the integration name from the setup instructions.
    SELECT COALESCE(MAX(value), 'ckan_apis_access_integration') INTO :eai FROM core.app_config WHERE key = 'eai_name';
    LET rs RESULTSET := (EXECUTE IMMEDIATE 'SELECT core.ckan_url_fn()');
    LET c CURSOR FOR rs;
    OPEN c;
    FETCH c INTO host;
    CLOSE c;

    CALL CONFIG.FINALIZE(:eai, :host);
    LET msg STRING := 'CKAN functions rebuilt for ' || host || ' using ' || eai;
    INSERT INTO core.ckan_log (dt, message) VALUES (current_timestamp(), :msg);
    RETURN msg;
EXCEPTION
    WHEN OTHER THEN
        LET err STRING := 'CKAN function rebuild failed, use Initialize > Rebuild CKAN functions: ' || SQLERRM;
        INSERT INTO core.ckan_log (dt, message) VALUES (current_timestamp(), :err);
        RETURN 'ERROR: ' || SQLERRM;
END;

GRANT USAGE ON PROCEDURE CONFIG.rebuild_ckan_functions() to application role ckan_app_role;

CALL CONFIG.rebuild_ckan_functions();
--Build the renewal UDF before rebuilding tasks that depend on it.
CALL CONFIG.redeploy_tasks();

