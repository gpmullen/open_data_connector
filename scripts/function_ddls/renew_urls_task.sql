//Presigned urls expire after 7 days (604800 seconds). Renew links to existing files every 6 days.
//Do not export data or change core.resources: its stream is reserved for data publish requests.
CREATE OR REPLACE TASK core.renew_presigned_urls_task
 SCHEDULE = '8640 MINUTE'
 USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
 AS
 EXECUTE IMMEDIATE
 $$
    BEGIN
        SYSTEM$LOG_INFO('Begin renewing presigned urls for all resources');
        INSERT INTO core.ckan_log (dt, packageid, resourceid, table_name, message)
        SELECT current_timestamp(), package_id, COALESCE(resp:id::string, resource_id), table_name,
               IFF(resp:id IS NULL,
                   'resource url renewal failed at CKAN: ' || COALESCE(resp:error::string, resp::string, 'no response'),
                   'presigned url renewed at CKAN; data timestamp unchanged')
        FROM (
            SELECT package_id, resource_id, table_name,
                   parse_json(config.resource_renew_url(resource_id,
                       get_presigned_url(@core.published_extracts,
                           replace(replace(IFNULL(file_name,table_name),'"',''),' ','_') || '.' || extension ||
                           CASE compressed
                               WHEN 'brotli' THEN '.br'
                               WHEN 'zstd' THEN '.zst'
                               WHEN 'gzip' THEN '.gz'
                               WHEN 'deflate' THEN '.zz'
                               WHEN 'raw_deflate' THEN '.rzz'
                               WHEN 'None' THEN ''
                               ELSE '.' || compressed
                           END, 604800))) resp
            FROM core.resources
            WHERE presigned_url IS NOT NULL
        );
    EXCEPTION
    WHEN OTHER THEN
        LET err STRING := 'Presigned url renewal failed: ' || SQLERRM;
        SYSTEM$LOG_ERROR(:err);
        INSERT INTO core.ckan_log (dt, message) VALUES (current_timestamp(), :err);
    END;
 $$;

GRANT ALL ON TASK core.renew_presigned_urls_task TO APPLICATION ROLE ckan_app_role;
alter task core.renew_presigned_urls_task resume;
