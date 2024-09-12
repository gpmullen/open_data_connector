//this is intended to capture any updates as they occur on the resources table. i.e. data has been update in the base table
CREATE OR REPLACE task core.{0}_refresh_updated_urls_task
 SCHEDULE = 'USING CRON {1} America/Los_Angeles'
 USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
 AS
 EXECUTE IMMEDIATE
 $$
    DECLARE 
        sql VARCHAR := '';
        is_first BOOLEAN := TRUE;
    BEGIN
        SYSTEM$LOG_INFO('Begin getting databases the app has access to');
        --Get all Databases that we can see so that we can find each tables last altered data. Exclude the app itself.
        SHOW TERSE DATABASES IN ACCOUNT; 
        LET dbs RESULTSET := (SELECT "name" db_name FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) res WHERE db_name <> current_database()); 

        --loop through each db and build the query to get the last_altered datetime.
         SYSTEM$LOG_INFO('Begin getting all tables that have been updated');
        FOR t IN dbs DO
             SYSTEM$LOG_INFO('Add database tables to update. DB Name: ' || t.db_name);
            IF (is_first) THEN
                sql := 'SELECT LAST_ALTERED,table_catalog db_name, table_Schema schema_name, table_name FROM '||t.db_name||'.INFORMATION_SCHEMA."TABLES" WHERE schema_name <> \'INFORMATION_SCHEMA\' and table_name = \'{0}\' ';
                is_first := false;
            ELSE
                sql := sql || ' UNION ALL SELECT LAST_ALTERED,table_catalog db_name, table_Schema schema_name, table_name FROM '||t.db_name||'.INFORMATION_SCHEMA."TABLES" WHERE schema_name <> \'INFORMATION_SCHEMA\' and table_name = \'{0}\' ';
            END IF;
        END FOR;
        SYSTEM$LOG_INFO('End getting all tables that have been updated. Resulting SQL: ' || :sql);

        --Invalidate the presigned_url on the resource table for any table that was updated in the last 24 hours. 
        --This will force records into the resource_stream
        
        sql := 'UPDATE core.resources
                set presigned_url = null
                where exists (select 1 from ('||:sql||') info_schema 
                    where info_schema.db_name = core.resources.database_name
                    and info_schema.schema_name = core.resources.schema_name
                    and info_schema.table_name = core.resources.table_name
                    and info_schema.table_name = \'{0}\'
                    and last_altered > dateadd(hours,-24,current_timestamp())
                    )';
        
        SYSTEM$LOG_INFO('Updating all the tables that have been changed: ' || :sql);
        execute immediate :sql;
       
        --Unload all files that are in the resouces_Stream and publish to CKAN
        CALL CONFIG.SP_UPDATE_RESOURCES('{0}');
    EXCEPTION
    WHEN OTHER THEN
        let err string := SQLERRM;
        SYSTEM$LOG_ERROR(:err);
        INSERT INTO core.ckan_log
        select current_timestamp(),rs.package_id,rs.resource_id,rs.table_name,'presigned url update failed: ' || :err
        FROM core.resources_stream rs;
    END;
$$;

GRANT ALL ON TASK core.{0}_refresh_updated_urls_task TO APPLICATION ROLE ckan_app_role;
alter task core.{0}_refresh_updated_urls_task resume;    
