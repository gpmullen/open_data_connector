//Presigned urls expire after 7 days (604800 seconds). Regenerate every published file and url, and update CKAN, every
//6 days regardless of whether the data changed or how each table's own refresh task is scheduled.
CREATE OR REPLACE TASK core.renew_presigned_urls_task
 SCHEDULE = '8640 MINUTE'
 USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
 AS
 EXECUTE IMMEDIATE
 $$
    BEGIN
        SYSTEM$LOG_INFO('Begin renewing presigned urls for all resources');
        --Clearing the url puts every resource in resources_stream, the same path as Re-Publish on the Manage page.
        UPDATE core.resources SET presigned_url = NULL, date_updated = current_timestamp();
        LET result STRING;
        CALL CONFIG.SP_UPDATE_RESOURCES_ALL() INTO :result;
        INSERT INTO core.ckan_log (dt, message) VALUES (current_timestamp(), 'Presigned url renewal: ' || :result);
    EXCEPTION
    WHEN OTHER THEN
        LET err STRING := 'Presigned url renewal failed: ' || SQLERRM;
        SYSTEM$LOG_ERROR(:err);
        INSERT INTO core.ckan_log (dt, message) VALUES (current_timestamp(), :err);
    END;
 $$;

GRANT ALL ON TASK core.renew_presigned_urls_task TO APPLICATION ROLE ckan_app_role;
alter task core.renew_presigned_urls_task resume;
