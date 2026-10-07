//this is intended to capture any updates as they occur on the resources table. i.e. data has been update in the base table
CREATE OR REPLACE task core.{0}_refresh_updated_urls_task
 SCHEDULE = 'USING CRON {1} America/Los_Angeles'
 USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
 AS
 EXECUTE IMMEDIATE
 $$
    DECLARE 
        tname STRING := '{0}';
        changed INTEGER := 0;
        --Only the databases/schemas this table is published from. Scanning every visible database was left over from the
        --original single task design and scales as tasks x databases.
        res CURSOR FOR SELECT DISTINCT database_name, schema_name FROM core.resources WHERE table_name = '{0}';
    BEGIN
        --Invalidate the presigned_url for this table if it was updated in the last 24 hours.
        --This will force records into the resource_stream
        FOR r IN res DO
            LET db STRING := r.database_name;
            LET sch STRING := r.schema_name;
            BEGIN
                LET q STRING := 'SELECT COUNT(*) FROM "' || REPLACE(db, '"', '""') || '".INFORMATION_SCHEMA."TABLES" WHERE table_schema = ? AND table_name = ? AND last_altered > dateadd(hours,-24,current_timestamp())';
                LET rs RESULTSET := (EXECUTE IMMEDIATE :q USING (sch, tname));
                LET c CURSOR FOR rs;
                OPEN c;
                FETCH c INTO changed;
                CLOSE c;
                IF (changed > 0) THEN
                    SYSTEM$LOG_INFO('Table changed, invalidating presigned url: ' || db || '.' || sch || '.' || tname);
                    UPDATE core.resources SET presigned_url = NULL
                        WHERE database_name = :db AND schema_name = :sch AND table_name = :tname;
                END IF;
            EXCEPTION
                WHEN OTHER THEN
                    --Name the unreachable database in ckan_log and still renew expiring urls below.
                    LET db_err STRING := 'presigned url update skipped database ' || db || ': ' || SQLERRM;
                    SYSTEM$LOG_WARN(:db_err);
                    INSERT INTO core.ckan_log (dt, table_name, message) VALUES (current_timestamp(), :tname, :db_err);
            END;
        END FOR;
        --Expiring urls for unchanged tables are renewed by core.renew_presigned_urls_task.
       
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
