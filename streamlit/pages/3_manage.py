import streamlit as st
from snowflake.snowpark.context import get_active_session
import util as util
import logging
import time
import pandas as pd

session = get_active_session()
logger = logging.getLogger("python_logger")
app_name=util.get_app_name()

st.header('Edit Published Resources')
st.info('To update records, click into a cell and make your changes, then hit return.')
st.info('To delete records, use the grey column on the left to select a row, or multiple rows with the shift or control/cmd key, then hit delete on your keyboard.')
tbls = session.table('core.resources')
try:
    edited = st.experimental_data_editor(tbls, key="ede",num_rows="dynamic",use_container_width=True
                                         #,disabled=("OWNER_ORG","DATABASE_NAME","SCHEMA_NAME","TABLE_NAME","PRESIGNED_URL")                                       
                                         )
except Exception as ex:
    session.sql('update core.resources set date_updated = current_timestamp();').collect()
    st.experimental_rerun()

col1, col2, col3 = st.columns(3)
with col1: 
    btnSave = st.button('Save', key='save', type='primary')
with col2:
    btnRepublish = st.button('Re-Publish', key='republish', type='secondary')
with col3:
    btnRefresh = st.button('Refresh', key='refresh', type='secondary')

if btnSave:
    with st.spinner("Saving resources..."):
        time.sleep(.1)
        try:
            session.write_pandas(edited, "core.resources_temp", auto_create_table=True, overwrite=True, quote_identifiers=False)                        
            #session.write_pandas(edited, "core.resources", auto_create_table=False, overwrite=True, quote_identifiers=False)                        
            session.sql("INSERT OVERWRITE INTO core.resources select OWNER_ORG,DATABASE_NAME,SCHEMA_NAME,TABLE_NAME,PACKAGE_ID,RESOURCE_ID,PRESIGNED_URL,CURRENT_TIMESTAMP(),FILE_NAME,EXTENSION,COMPRESSED from core.resources_temp").collect()
            st.success('Saved!', icon="✅")
            
        except Exception as ex:
            logger.error(ex)
            st.error(util.error_msg, icon='🚨')
if btnRepublish:
    with st.spinner("Updating resources..."):
        time.sleep(.1)
        try:
            session.sql("UPDATE CORE.RESOURCES SET DATE_UPDATED = CURRENT_TIMESTAMP()").collect()
            result = session.sql("call CONFIG.SP_UPDATE_RESOURCES_ALL()").collect()[0][0]
            if result == 'FAILURE':
                st.error(util.error_msg, icon='🚨')
            else: 
                st.success('Saved!', icon="✅")
        except Exception as ex:
            logger.error(ex)
            st.error(util.error_msg, icon='🚨')

if btnRefresh:
    st.experimental_rerun()       


