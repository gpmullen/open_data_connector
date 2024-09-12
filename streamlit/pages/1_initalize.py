import streamlit as st
from snowflake.snowpark.context import get_active_session
import util as util
from cron_descriptor import get_description

session = get_active_session()
app_name = util.get_app_name()

def write_key():
    if 'apikey' in st.session_state and len(st.session_state.apikey)>0:
        apikey = st.session_state.apikey
        _ =  session.sql("""CREATE OR REPLACE SECRET core.ckan_api_key
                        TYPE = GENERIC_STRING
                        SECRET_STRING = '{0}';""".format(apikey)).collect()
        _ = session.sql("""GRANT USAGE ON SECRET core.ckan_api_key TO APPLICATION ROLE ckan_app_role;""").collect()

def write_url():
    if 'ckan_url' in st.session_state and len(st.session_state.ckan_url)>0:
        ckan_url = st.session_state.ckan_url
        try:
            _ = session.sql(f"CREATE OR REPLACE FUNCTION core.ckan_url_fn() RETURNS STRING AS $$'{ckan_url}'$$;").collect()
            _ = session.sql(f"""CREATE OR REPLACE NETWORK RULE {app_name}.CONFIG.external_access_rule
            TYPE = HOST_PORT
            MODE = EGRESS
            VALUE_LIST = ('{ckan_url}');            
            """).collect()
            _ = session.sql(f"GRANT USAGE ON NETWORK RULE CONFIG.external_access_rule TO APPLICATION ROLE ckan_app_role;").collect()
        except:
            st.error('Network Rule Creation failed. Check the URL. It should not include HTTP/HTTPS or trailing slashes')
            st.session_state.vwh_submitted = False


def check_integration():
    if util.is_external_access_configured():
        st.success('External Access is enabled', icon='✅')
    else:
        st.error('External Access is not complete. Please complete all steps on this page.', icon='🚨')

def createTasks():
    if 'vwh' in st.session_state:
        vwh = st.session_state.vwh
        try:
            _ = session.sql(f'call config.create_vwh_objects(\'{vwh}\')').collect()
            st.session_state.vwh_submitted = True
        except:
            st.error('Task Creation failed. Check the name of your virtual warehouse and that permissions are granted.')
            st.session_state.vwh_submitted = False

check_integration()
st.header('Step 1 of 4: Choose Virtual Warehouse and Create Weekly Task')
st.info('Tasks require a Virtual Warehouse (VWH) to run on a regular basis. Please input the name of a VWH to use for ongoing tasks. This VWH can be a shared resource. An XS VWH is recommended. These tasks automate updates to CKAN as changes occur.')

vwh = st.text_input("Name of VWH", key='vwh')


#with st.expander("CRON Configuration (Optional)"):
#    col_cron,col_secs,col_mins,col_hour,col_dayMo,col_month,col_dayWeek = st.columns(7)
#    st.info("Data is refreshed to CKAN based on this interval. The default setting is 11PM daily.")
#    with col_mins:
#        col_mins=st.text_input("minutes", value="0", key="mins")
#    with col_hour:
#        col_hour=st.text_input("hours", value="23", key="hour")
#    with col_dayMo:
#        col_dayMo=st.text_input("day of month", value="*", key="day")
#    with col_month:
#        col_month=st.text_input("month", value="*", key="month")
#    with col_dayWeek:
#        col_dayWeek=st.text_input("day of week", value="*", key="dayweek")
#    cron=f'{col_mins} {col_hour} {col_dayMo} {col_month} {col_dayWeek}'
#    st.write('Refresh task runs at: ' + get_description(f'{col_mins} {col_hour} {col_dayMo} {col_month} {col_dayWeek}'))

if util.is_task_configured():
    st.success('Tasks created', icon='✅')
elif not vwh:
    st.stop()

st.warning('Before creating the tasks, grant access to the application by running the following command in a separate worksheet in Snowsight.')
st.code(f'''GRANT USAGE,OPERATE ON WAREHOUSE {vwh} TO APPLICATION {app_name};''')
btnCreateTask = st.button('Create Tasks',on_click=createTasks, type='primary')
 
if 'vwh_submitted' not in st.session_state or not st.session_state.vwh_submitted:
    st.stop()

st.header('Step 2 of 4: Register API Key or Token')
st.divider()
st.info('Please register your CKAN API key from your user profile to allow for system authentication. This value will be stored in a Snowflake SECRET object.')
submitted = st.text_input('CKAN API Key', on_change=write_key, key='apikey',type='password')

if util.is_key_configured():
    st.success("CKAN API Key registered", icon="✅")
    st.header('Step 3 of 4: Create External Access')
    st.divider()
    ckan_url = st.text_input('CKAN API URL', on_change=write_url, key='ckan_url', help='Use the subdomain and primary domain only. e.g. test.domain.com. Do not provide the protocol i.e. HTTPS.')
    if util.is_url_configured():
        if len(ckan_url.strip()) == 0:
            ckan_url = util.get_ckan_url()
        st.warning("You must enable external access to the CKAN API. Execute the the following code with the ACCOUNTADMIN role or a custom role that has been granted the CREATE INTEGRATION permission for the account. Open a new worksheet in Snowsight to execute the following:")
        st.code(f'''        
        CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION ckan_apis_access_integration
        ALLOWED_NETWORK_RULES = ({app_name}.CONFIG.EXTERNAL_ACCESS_RULE)
        ALLOWED_AUTHENTICATION_SECRETS = ({app_name}.core.ckan_api_key)
        ENABLED = true;
                
        GRANT USAGE ON INTEGRATION ckan_apis_access_integration TO APPLICATION {app_name};

        CALL {app_name}.CONFIG.FINALIZE('ckan_apis_access_integration','{ckan_url}');''')
        btnRefresh= st.button('Check External Access')
        if btnRefresh:
            st.experimental_rerun()
        st.header('Step 4 of 4: Grant Permissions to your tables')
        st.divider()
        st.info('The app requires access to any table that you want to publish. To accomplish this you must run the following three commands for each table in your Snowflake account. You will need to replace the text in <angled bracket> with your own values.')
        st.code(f'''GRANT USAGE ON DATABASE <database_name> TO APPLICATION {app_name};
                GRANT USAGE ON SCHEMA <database_name>.<schema_name> TO APPLICATION {app_name};
                GRANT SELECT ON TABLE <database_name>.<schema_name>.<table_name> TO APPLICATION {app_name};''')
        with st.expander("See an example"):
            st.info('Here is an example of all the fully qualified statements together')
            st.code(f"""        
    GRANT USAGE,OPERATE ON WAREHOUSE compute_wh TO APPLICATION {app_name};
    GRANT EXECUTE TASK ON ACCOUNT TO APPLICATION {app_name};
    GRANT EXECUTE MANAGED TASK ON ACCOUNT TO APPLICATION {app_name};

    CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION ckan_apis_access_integration
    ALLOWED_NETWORK_RULES = ({app_name}.CONFIG.EXTERNAL_ACCESS_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = ({app_name}.core.ckan_api_key)
    ENABLED = true;

    GRANT USAGE ON INTEGRATION ckan_apis_access_integration TO APPLICATION {app_name};

    CALL {app_name}.CONFIG.FINALIZE('ckan_apis_access_integration', '{ckan_url}');

    GRANT USAGE ON DATABASE source1 TO APPLICATION {app_name};
    GRANT USAGE ON SCHEMA source1.public TO APPLICATION {app_name};
    GRANT SELECT ON TABLE source1.public.customer TO APPLICATION {app_name};
    //if you have multiple tables in a schema, then there's no need to reapply the previous two statements
    GRANT SELECT ON TABLE source1.public.customer2 TO APPLICATION {app_name};

    GRANT USAGE ON DATABASE source2 TO APPLICATION {app_name};
    GRANT USAGE ON SCHEMA source2.public TO APPLICATION {app_name};
    GRANT SELECT ON TABLE source2.public.more_tables TO APPLICATION {app_name};
    """)
