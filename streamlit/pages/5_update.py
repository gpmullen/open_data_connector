import streamlit as st
from snowflake.snowpark.context import get_active_session
import logging
import util as util
import time

session = get_active_session()
logger = logging.getLogger("python_logger")
app_name=util.get_app_name()
ckan_url=util.get_ckan_url()

st.header('Updating the Open Data Connector')
st.info('After upgrading the version, you may need to redploy the tasks and external access funtions if they have changed. You can do so by running the following code in a worksheet. You can re-run this code multiple times.')

st.code(f"""
            begin 
                SHOW TASKS LIKE '%REFRESH_UPDATED_URLS_TASK' IN {app_name}.core; 
                LET res RESULTSET := (select DISTINCT substr("name",1,regexp_instr("name",'_REFRESH_UPDATED_URLS_TASK')-1) tname,"warehouse" wh, regexp_replace("schedule",'(USING CRON )|(America/Los_Angeles)') sch from table(result_scan(last_query_id()))); 
                FOR vwh IN res DO
                    let sql string := 'CALL {app_name}.config.create_vwh_objects_tname(\\'' || vwh.tname || '\\',\\''||vwh.sch||'\\')';        
                    execute immediate(:sql);
                    CALL {app_name}.CONFIG.FINALIZE('ckan_apis_access_integration','{ckan_url}');
                END FOR;
                return 'SUCCESS';
            end;"""
            )            
