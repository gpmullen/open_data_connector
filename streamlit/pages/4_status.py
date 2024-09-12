import streamlit as st
from snowflake.snowpark.context import get_active_session
import util as util
import logging

session = get_active_session()
logger = logging.getLogger("python_logger")
app_name=util.get_app_name()

st.header('Status and Logging')
st.info('Review Request Log to API')

try:
    logs = session.sql('select * from core.ckan_log order by 1 desc')
    lde = st.experimental_data_editor(logs,num_rows="fixed",use_container_width=True)
except Exception as ex:
            logger.error(ex)
            st.error(util.error_msg, icon='🚨')

st.info('Tasks')
try:
    tasks = session.sql(f"begin show tasks in {app_name}.core; let res RESULTSET := (select \"name\",\"warehouse\",\"schedule\",\"state\", IFF(\"condition\" IS NOT NULL,'Has Stream Condition','No Stream Condition') \"has_stream\" from table(result_scan(last_query_id()))); return table(res); end;").collect()
    tde = st.experimental_data_editor(tasks,num_rows="fixed",use_container_width=True)
except Exception as ex:
            logger.error(ex)
            st.error(util.error_msg, icon='🚨')

col1,col2 = st.columns(2)
with col1:
    btnStartTask = st.button('Start Tasks',key='StartTask',type='primary')
    if btnStartTask:
        try:
            session.sql(f"""
             BEGIN 
               show tasks in {app_name}.core;
               LET c1 cursor for (SELECT \'{app_name}.core.\' || \"name\" as tname FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
               open c1;
               FOR arow in c1 DO
                  let taskname varchar := arow.tname;
                  alter task identifier(:taskname) resume; 
               END FOR; close c1;
               END;""").collect()
            st.success('Tasks resumed')
        except Exception as ex:
                    logger.error(ex)
                    st.error(util.error_msg, icon='🚨')
with col2:                    
    btnSuspendTask = st.button('Suspend Tasks',key='suspend',type='secondary')
    if btnSuspendTask:
        try:
            session.sql(f"""
             BEGIN 
               show tasks in {app_name}.core;
               LET c1 cursor for (SELECT \'{app_name}.core.\' || \"name\" as tname FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
               open c1;
               FOR arow in c1 DO
                  let taskname varchar := arow.tname;
                  alter task identifier(:taskname) suspend; 
               END FOR; close c1;
               END;""").collect()
            st.success('Tasks suspended')
        except Exception as ex:
                    logger.error(ex)
                    st.error(util.error_msg, icon='🚨')


st.info('Streams')
try:
    streams = session.sql(f"begin show streams in {app_name}.core; let res RESULTSET := (select \"name\",\"stale\",\"stale_after\",\"invalid_reason\" from table(result_scan(last_query_id()))); return table(res); end;").collect()
    tde = st.experimental_data_editor(streams,num_rows="fixed",use_container_width=True)
except Exception as ex:
            logger.error(ex)
            st.error(util.error_msg, icon='🚨')            

recreate = st.button('Recreate Stream',key='recreate',help='If the stream is stale, recreate it and republish',type='primary')            
if recreate:
        try:
            session.sql(f"CREATE OR REPLACE STREAM core.resources_stream on table core.resources;").collect()
            st.success('Stream recreated')
        except Exception as ex:
            logger.error(ex)
            st.error(util.error_msg, icon='🚨')  
