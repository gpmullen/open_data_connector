import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
import util as util
from datetime import datetime
import logging
import time
from cron_descriptor import get_description

session = get_active_session()
logger = logging.getLogger("python_logger")
app_name=util.get_app_name()
RESOURCE_DB=util.get_app_name()
RESOURCE_SCHEMA='CORE'
RESOURCE_TABLE = 'RESOURCES'

#0 = create_on
#1 = Object Name
idx_table_name=1
#2 = kind
#3 = Database Name
idx_db_name=3
#4 = schema
idx_schema_name=4
def getDatabases():
    if 'tables' in st.session_state:
        return set([row[idx_db_name] for row in st.session_state.tables])
    else:
        return []

def getSchemas():
    if 'ddlDatabaseToPublish' in st.session_state:
        return set([row[idx_schema_name] for row in st.session_state.tables if row[idx_db_name] == st.session_state.ddlDatabaseToPublish])
    else:
        return []

def getTables():
    if 'ddlSchemaToPublish' in st.session_state:
        return [row[idx_table_name] for row in st.session_state.tables if row[idx_schema_name] == st.session_state.ddlSchemaToPublish and row[idx_db_name] == st.session_state.ddlDatabaseToPublish]
    else:
        return []
    
def package_id_format(name) -> str:
    return str(st.session_state.packages.filter(col('PACKAGE_NAME')== name).select('PACKAGE_ID').collect()[0][0])

def resource_id_format(resource_name) -> str:
    return str(st.session_state.packages.filter(col('RESOURCE_NAME')== resource_name).select('RESOURCE_ID').collect()[0][0])

@st.cache_data
def getPackages(owner_org):
    if len(owner_org)>0:
        try:
            with st.spinner("Getting your organization's packages..."):
                time.sleep(.1)
                st.session_state.packages = session.sql(f'''with cte as (select parse_json(config.package_search('{owner_org}')) data)            
                    select ' ' PACKAGE_ID,' ' PACKAGE_NAME,' ' RESOURCE_ID,' ' RESOURCE_NAME
                    union
                    select 
                    packages.value:id::string PACKAGE_ID
                    , packages.value:name::string PACKAGE_NAME
                    , resources.value:id::string RESOURCE_ID
                    , resources.value:name::string RESOURCE_NAME
                    from cte,
                    lateral flatten(input => cte.data:results) packages,
                    lateral flatten(input => packages.value:resources) resources
                    ''')
                return st.session_state.packages.select('PACKAGE_NAME').distinct().sort(col("PACKAGE_NAME"),ascending=True).collect()
        except Exception as ex:
            logger.error(ex)
            st.error(util.error_msg)
            return []
    else:
        return []

@st.cache_data
def getResources(package_name):
    if package_name is not None and len(package_name.strip()) > 0:
        with st.spinner("Getting resources..."):
            time.sleep(.1)
            return st.session_state.packages.filter((col('PACKAGE_NAME') == package_name) & (col("RESOURCE_NAME") != '')).select('RESOURCE_NAME').collect()        
    else:
        return []
    
def updateResource():
    try:
        with st.spinner(f"Writing data to {RESOURCE_DB}.{RESOURCE_SCHEMA}.{RESOURCE_TABLE}"):
            time.sleep(.1)
            if "ddlOwnerOrg" in st.session_state: ddlOwnerOrg=st.session_state["ddlOwnerOrg"] 
            if "ddlDatabaseToPublish" in st.session_state: ddlDatabaseToPublish=st.session_state["ddlDatabaseToPublish"] 
            if "ddlSchemaToPublish" in st.session_state: ddlSchemaToPublish=st.session_state["ddlSchemaToPublish"] 
            if "ddlTableToPublish" in st.session_state: ddlTableToPublish=st.session_state["ddlTableToPublish"] 
            if "txtFileAlias" in st.session_state and len(st.session_state["txtFileAlias"].strip()) > 0: 
                txtFileAlias = st.session_state["txtFileAlias"] 
            else:
                txtFileAlias = None
            package_id = package_id_format(ddlPackages)
            resource_id = resource_id_format(ddlResources)
            dfControl = session.create_dataframe([[ddlOwnerOrg
                                                ,ddlDatabaseToPublish
                                                ,ddlSchemaToPublish
                                                ,ddlTableToPublish
                                                ,package_id
                                                , resource_id
                                                ,None
                                                ,datetime.now()
                                                ,txtFileAlias
                                                ,rdoOutputType
                                                ,rdoCompress]])
            #insert
            session.sql('BEGIN TRANSACTION')
            dfControl.write.mode("append").save_as_table("{0}.{1}.{2}".format(RESOURCE_DB,RESOURCE_SCHEMA,RESOURCE_TABLE))
            #TASKS
            result = session.sql("call CONFIG.SP_UPDATE_RESOURCES(\'{ddlTableToPublish}\')").collect()[0][0]
            if result == 'FAILURE':
                st.error(util.error_msg, icon='🚨')        
                session.sql('ROLLBACK')
            else:
                session.sql('COMMIT TRANSACTION')
                st.success('Saved!', icon="✅")
                createTasks()
            
    except Exception as ex:
        logger.error(ex)
        st.error(util.error_msg, icon='🚨')        
        session.sql('ROLLBACK')

def loadTables():
    with st.spinner("Getting tables you have authorized access to..."):
        #get tables and views that we have permissions to read
        time.sleep(.1)
        st.session_state.tables = session.sql("""
        BEGIN 
            SHOW TERSE TABLES IN ACCOUNT; 
            LET ret RESULTSET := (SELECT * 
                FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) res 
                WHERE "database_name" <> CURRENT_DATABASE() 
                AND NOT EXISTS (SELECT 1    FROM core.resources r 
                                            WHERE r.database_name = res."database_name" 
                                            AND r.schema_name = res."schema_name" 
                                            and r.table_name = res."name")
                ); 
            RETURN TABLE(ret); 
        END;""").collect()

@st.cache_data
def getOrgs():
    try:  
        with st.spinner("Getting your organizations..."):
            time.sleep(.1)      
            df = session.sql("""WITH cte AS (SELECT parse_json(config.get_orgs()) data)
                                SELECT '' name
                                UNION
                                SELECT org.value:name::string name
                                FROM cte,
                                LATERAL FLATTEN(input => cte.data) org                                
                                ORDER BY 1
                                """).collect()
            return df
    except Exception as ex:
        logger.error(ex)
        st.error(util.error_msg)
        return []

def populateCompressionOptions():
    if st.session_state.rdoOutputType == 'parquet':
        return ['SNAPPY','LZO','None']
    else:
        return ['gzip','bz2','brotli','zstd','deflate','raw_deflate','None']

#
# add createtasks
#

def createTasks():

        try:
            _ = session.sql(f'call config.create_vwh_objects_tname(\'{ddlTableToPublish}\',\'{cron}\')').collect()
        except:
            st.error('Task Creation failed. Check that permissions are granted.')
            st.error(util.error_msg)


st.header('Publish Resources')
st.info('Use the drop downs to select the Package and Resources to map a table to.')
st.warning('You must already have created the package and resource in the Open Data.')
if not util.is_key_configured():
    st.error('Please configure CKAN API key to allow for authentication', icon='🚨')
elif not util.is_external_access_configured():
    st.error('Please configure the External Access Integration', icon='🚨')
else:    
    
    ddlOwnerOrg = st.selectbox("Owner Org",options=getOrgs(), help='Select an Organization to populate the packages', key='ddlOwnerOrg')
    if ddlOwnerOrg and len(ddlOwnerOrg.strip()) > 0:
        ddlPackages = st.selectbox("Packages", help='Required', key='ddlPackages', options=getPackages(ddlOwnerOrg))
        if ddlPackages and len(ddlPackages.strip()) > 0:
            with st.spinner("Loading Resources..."):
                time.sleep(.1)
                ddlResources = st.selectbox("Resources", options=getResources(ddlPackages), help='Required', key='ddlResources')
            with st.spinner("Loading dropdowns..."):
                time.sleep(.1)
                loadTables()
                st.info('If the database, schema or table to publish is not on the list, review step 3 of initalization to grant access.')
                ddlDatabaseToPublish = st.selectbox("Database",options=getDatabases() ,help='Required', key='ddlDatabaseToPublish')
                ddlSchemaToPublish = st.selectbox("Schema", options=getSchemas(), help='Required', key='ddlSchemaToPublish')            
                ddlTableToPublish = st.selectbox("Table", options=getTables(), help='Required', key='ddlTableToPublish')
                txtFileAlias = st.text_input('File Alias (without extension)', help='This will be the name of the file in CKAN without extension', key='txtFileAlias')            
                rdoOutputType = st.radio("File Type", options=['csv','json','parquet'], key='rdoOutputType',horizontal=True)
                rdoCompress = st.radio("Compression",options=populateCompressionOptions(), key='rdoCompress',horizontal=True)

            
            with st.expander("CRON Configuration"):
                col_cron,col_secs,col_mins,col_hour,col_dayMo,col_month,col_dayWeek = st.columns(7)
                st.info("Data is refreshed to CKAN based on this interval. The default setting is 11PM daily.")
            with col_mins:
                col_mins=st.text_input("minutes", value="0", key="mins")
            with col_hour:
                col_hour=st.text_input("hours", value="23", key="hour")
            with col_dayMo:
                col_dayMo=st.text_input("day of month", value="*", key="day")
            with col_month:
                col_month=st.text_input("month", value="*", key="month")
            with col_dayWeek:
                col_dayWeek=st.text_input("day of week", value="*", key="dayweek")
            cron=f'{col_mins} {col_hour} {col_dayMo} {col_month} {col_dayWeek}'
            st.write('Refresh task runs at: ' + get_description(f'{col_mins} {col_hour} {col_dayMo} {col_month} {col_dayWeek}'))

              
            col1,col2 = st.columns(2)
            with col1:
                btnPublish = st.button("Publish", on_click=updateResource, type='primary',help='Publishes the data set to the portal.')
            with col2:
                btnTableRefresh = st.button("Refresh Tables", on_click=loadTables, type='secondary', help='If you have added permissions to a new table, press this button to refresh the list of database tables in the drop down lists.')



