import streamlit as st
from snowflake.snowpark.context import get_active_session
import logging
import util as util

session = get_active_session()
logger = logging.getLogger("python_logger")

st.header('Updating the Open Data Connector')
st.info('Upgrades run this automatically. Use it again if the Status page shows refresh tasks that do not match your published tables. '
        'It rebuilds one refresh task per published table with the current code, keeps each table\'s schedule, and removes tasks that do not belong to a published table.')

if st.button('Redeploy refresh tasks', type='primary'):
    try:
        with st.spinner('Redeploying tasks...'):
            result = session.sql('CALL config.redeploy_tasks()').collect()[0][0]
        if result.startswith('ERROR'):
            st.error(result, icon='🚨')
        else:
            st.success(result, icon='✅')
    except Exception as ex:
        logger.error(ex)
        st.error(f'Redeploy failed: {ex}', icon='🚨')
