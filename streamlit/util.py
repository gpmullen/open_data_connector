import streamlit as st
from snowflake.snowpark.context import get_active_session
import json
import re
import time
session = get_active_session()
error_msg = 'An error has occurred. Create an [Event Table](https://other-docs.snowflake.com/en/native-apps/consumer-enable-logging) in your account and enable Event Sharing to share the error with the provider'
CKAN_FUNCTIONS = {'GET_ORGS', 'PACKAGE_SEARCH', 'RESOURCE_UPDATE'}

@st.cache_data
def get_app_name() -> str:
    with st.spinner('Getting App Name...'):
        time.sleep(.1)
        return session.sql("""
            select '"'||current_database()||'"' DB
        """).collect()[0]["DB"]

def sql_literal(value: str) -> str:
    #Escape a value for use inside a single quoted Snowflake string literal.
    return value.replace('\\', '\\\\').replace("'", "\\'")

def normalize_host(value: str) -> str:
    #CKAN host only, e.g. publish.data.ca.gov. Strips protocol, path and trailing slashes.
    host = re.sub(r'^\s*https?://', '', value.strip(), flags=re.IGNORECASE).split('/')[0].strip()
    if not re.fullmatch(r'[A-Za-z0-9.-]+(:[0-9]{1,5})?', host):
        raise ValueError(f'"{value}" is not a valid host name')
    return host

def reset_status():
    for k in ('is_key_configured', 'is_external_access_configured', 'is_task_configured'):
        st.session_state.pop(k, None)

def is_task_configured() -> bool:
    #Per table tasks are serverless, so the app only needs the task privileges, not a warehouse.
    if not st.session_state.get('is_task_configured'):
        import snowflake.permissions as permissions
        st.session_state.is_task_configured = bool(permissions.get_held_account_privileges(["EXECUTE TASK"])) \
            and bool(permissions.get_held_account_privileges(["EXECUTE MANAGED TASK"]))
    return st.session_state.is_task_configured

def is_key_configured() -> bool:
    app_name = get_app_name()
    if not st.session_state.get('is_key_configured'):
        df = session.sql(f"show secrets like 'ckan_api_key' in schema {app_name}.core;").collect()
        st.session_state.is_key_configured = len(df) > 0
    return st.session_state.is_key_configured

def is_url_configured() -> bool: 
    return len(get_ckan_url()) > 0

def get_ckan_url() -> str:    
    app_name = get_app_name()  
    df = session.sql(f"show user functions like 'ckan_url_fn' in schema {app_name}.core;").collect()
    if len(df) > 0:
        return session.sql('SELECT core.ckan_url_fn();').collect()[0][0]
    return ''

def get_config(key: str) -> str:
    rows = session.sql("SELECT value FROM core.app_config WHERE key = ? ORDER BY updated DESC LIMIT 1", params=[key]).collect()
    return rows[0][0] if rows else ''

def is_external_access_configured() -> bool:
    app_name = get_app_name()
    if not st.session_state.get('is_external_access_configured'):
        df = session.sql(f"show user functions in schema {app_name}.config;").collect()
        names = {row['name'].upper() for row in df}
        st.session_state.is_external_access_configured = CKAN_FUNCTIONS.issubset(names)
    return st.session_state.is_external_access_configured

def test_connection():
    #Returns (ok, message) from a live call to CKAN through the external access integration.
    try:
        data = json.loads(session.sql('SELECT config.get_orgs()').collect()[0][0])
    except Exception as ex:
        return False, str(ex)
    if isinstance(data, dict) and 'error' in data:
        return False, json.dumps(data)
    return True, f'Connected. {len(data)} organization(s) visible to this API key.'

def _ckan_call(sql: str, params=None):
    data = json.loads(session.sql(sql, params=params).collect()[0][0])
    if isinstance(data, dict) and 'error' in data:
        raise RuntimeError(f"CKAN returned an error: {json.dumps(data)}")
    return data

@st.cache_data(ttl=300)
def ckan_orgs() -> list:
    return sorted(org['name'] for org in _ckan_call('SELECT config.get_orgs()'))

@st.cache_data(ttl=300)
def ckan_packages(owner_org: str) -> list:
    #One row per resource: {package_id, package_name, resource_id, resource_name}
    data = _ckan_call('SELECT config.package_search(?)', params=[owner_org])
    rows = []
    for p in data.get('results', []):
        for r in p.get('resources', []):
            rows.append({'package_id': p['id'], 'package_name': p['name'], 'resource_id': r['id'], 'resource_name': r.get('name') or r['id']})
    return rows
