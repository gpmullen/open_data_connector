import streamlit as st
from snowflake.snowpark.context import get_active_session
import logging
import util as util

session = get_active_session()
logger = logging.getLogger("python_logger")
app_name = util.get_app_name()

def object_exists(show_sql: str) -> bool:
    return len(session.sql(show_sql).collect()) > 0

def save_key():
    apikey = st.session_state.get('apikey', '').strip()
    if not apikey:
        st.session_state.key_msg = ('error', 'Enter an API key first.')
        return
    try:
        key = util.sql_literal(apikey)
        #Update in place: replacing the secret would detach it from the external access integration.
        if object_exists("show secrets like 'ckan_api_key' in schema core"):
            session.sql(f"ALTER SECRET core.ckan_api_key SET SECRET_STRING = '{key}'").collect()
        else:
            session.sql(f"CREATE SECRET core.ckan_api_key TYPE = GENERIC_STRING SECRET_STRING = '{key}'").collect()
            session.sql("GRANT USAGE ON SECRET core.ckan_api_key TO APPLICATION ROLE ckan_app_role").collect()
        st.session_state.apikey = ''
        util.reset_status()
        st.session_state.key_msg = ('success', 'API key saved.')
    except Exception as ex:
        logger.error(ex)
        st.session_state.key_msg = ('error', f'Saving the API key failed: {ex}')

def save_url():
    try:
        host = util.normalize_host(st.session_state.get('ckan_url', ''))
    except ValueError as ex:
        st.session_state.url_msg = ('error', str(ex))
        return
    try:
        session.sql(f"CREATE OR REPLACE FUNCTION core.ckan_url_fn() RETURNS STRING AS $$'{host}'$$").collect()
        #Update in place: replacing the rule would detach it from the external access integration.
        if object_exists(f"show network rules like 'EXTERNAL_ACCESS_RULE' in schema {app_name}.config"):
            session.sql(f"ALTER NETWORK RULE {app_name}.config.external_access_rule SET VALUE_LIST = ('{host}')").collect()
        else:
            session.sql(f"""CREATE NETWORK RULE {app_name}.config.external_access_rule
                TYPE = HOST_PORT MODE = EGRESS VALUE_LIST = ('{host}')""").collect()
            session.sql("GRANT USAGE ON NETWORK RULE config.external_access_rule TO APPLICATION ROLE ckan_app_role").collect()
        util.reset_status()
        st.session_state.url_msg = ('success', f'CKAN URL saved: {host}')
    except Exception as ex:
        logger.error(ex)
        st.session_state.url_msg = ('error', f'Saving the URL failed: {ex}')

def rebuild_functions():
    eai = util.get_config('eai_name')
    host = util.get_ckan_url()
    try:
        session.sql("CALL config.finalize(?, ?)", params=[eai, host]).collect()
        util.reset_status()
        ok, msg = util.test_connection()
        st.session_state.ea_msg = ('success' if ok else 'error', msg)
    except Exception as ex:
        logger.error(ex)
        st.session_state.ea_msg = ('error', f'Rebuilding the CKAN functions failed: {ex}')

def show_msg(key):
    if key in st.session_state:
        level, msg = st.session_state.pop(key)
        getattr(st, level)(msg)

def eai_sql(host: str, eai: str) -> str:
    return f'''CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION {eai}
  ALLOWED_NETWORK_RULES = ({app_name}.CONFIG.EXTERNAL_ACCESS_RULE)
  ALLOWED_AUTHENTICATION_SECRETS = ({app_name}.CORE.CKAN_API_KEY)
  ENABLED = TRUE;

GRANT USAGE ON INTEGRATION {eai} TO APPLICATION {app_name};

CALL {app_name}.CONFIG.FINALIZE('{eai}','{host}');'''

if not util.is_task_configured():
    st.error('The app needs the EXECUTE TASK and EXECUTE MANAGED TASK privileges. Grant them from the app\'s Security tab, or run:', icon='🚨')
    st.code(f'''GRANT EXECUTE TASK ON ACCOUNT TO APPLICATION {app_name};
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO APPLICATION {app_name};''')

st.header('Step 1 of 3: Register API Key or Token')
st.info('Register the CKAN API token from your CKAN user profile. It is stored in a Snowflake SECRET owned by the app. Use the same steps to replace an expired or rotated token.')
st.text_input('CKAN API Key', key='apikey', type='password')
st.button('Save API key', on_click=save_key, type='primary')
show_msg('key_msg')
if not util.is_key_configured():
    st.stop()
st.success('CKAN API key registered', icon='✅')

st.header('Step 2 of 3: CKAN API URL')
current_host = util.get_ckan_url()
if current_host:
    st.write(f'Current CKAN host: **{current_host}**')
st.text_input('CKAN API URL', key='ckan_url', placeholder='publish.data.ca.gov',
              help='Host name only, e.g. publish.data.ca.gov. Do not include https:// or a trailing slash.')
st.button('Save URL', on_click=save_url, type='primary')
show_msg('url_msg')
if not util.is_url_configured():
    st.stop()
host = util.get_ckan_url()

st.header('Step 3 of 3: External Access')
eai_name = util.get_config('eai_name')
if not eai_name and util.is_external_access_configured():
    #Installs finalized before app_config existed; this is the name the setup instructions always used.
    eai_name = 'ckan_apis_access_integration'
if eai_name:
    st.info(f'The CKAN functions use the external access integration **{eai_name}**. After you change the API key or URL, rebuild the functions so they use the new values.')
    col1, col2 = st.columns(2)
    with col1:
        st.button('Rebuild CKAN functions', on_click=rebuild_functions, type='primary')
    with col2:
        if st.button('Test connection'):
            ok, msg = util.test_connection()
            st.session_state.ea_msg = ('success' if ok else 'error', msg)
    show_msg('ea_msg')
    with st.expander('Connection still failing? Re-create the external access integration'):
        st.write('Run this in a worksheet with ACCOUNTADMIN, or a role with the CREATE INTEGRATION privilege.')
        st.code(eai_sql(host, eai_name))
        st.write('If the app network rule cannot be updated, you can use a network rule you own instead:')
        st.code(f'''CREATE NETWORK RULE <database>.<schema>.ckan_egress_rule
  TYPE = HOST_PORT MODE = EGRESS VALUE_LIST = ('{host}');
-- then use it in ALLOWED_NETWORK_RULES above instead of {app_name}.CONFIG.EXTERNAL_ACCESS_RULE''')
else:
    st.warning('Enable external access to the CKAN API. Run this in a worksheet with ACCOUNTADMIN, or a role with the CREATE INTEGRATION privilege:')
    st.code(eai_sql(host, 'ckan_apis_access_integration'))
    if st.button('Check External Access'):
        util.reset_status()
        st.rerun()

if not util.is_external_access_configured():
    st.stop()
st.success('External Access is enabled', icon='✅')

st.header('Grant access to your tables')
st.info('The app needs access to each table you want to publish. Run these commands for each table, replacing the text in <angle brackets>.')
st.code(f'''GRANT USAGE ON DATABASE <database_name> TO APPLICATION {app_name};
GRANT USAGE ON SCHEMA <database_name>.<schema_name> TO APPLICATION {app_name};
GRANT SELECT ON TABLE <database_name>.<schema_name>.<table_name> TO APPLICATION {app_name};''')
