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
st.info('To update records, click into a cell and make your changes, then click Save. Edits are kept until you Save or Refresh.')
st.info('To delete records, use the grey column on the left to select a row, or multiple rows with the shift or control/cmd key, then hit delete on your keyboard.')

def load_resources():
    #Snapshot the table once; passing the lazy Snowpark table re-reads it on every rerun and resets the editor.
    st.session_state.resources_df = session.table('core.resources').to_pandas()
    st.session_state.editor_version = st.session_state.get('editor_version', 0) + 1

if 'resources_df' not in st.session_state:
    load_resources()

edited = st.data_editor(st.session_state.resources_df, key=f"ede_{st.session_state.editor_version}",
                        num_rows="dynamic", use_container_width=True)

col1, col2, col3 = st.columns(3)
with col1: 
    btnSave = st.button('Save', key='save', type='primary')
with col2:
    btnRepublish = st.button('Re-Publish', key='republish', type='secondary',
                             help='Regenerates the files and presigned urls for all published tables and updates CKAN now.')
with col3:
    btnRefresh = st.button('Refresh', key='refresh', type='secondary', help='Discard unsaved edits and reload from the table.')

REQUIRED_COLUMNS = ['OWNER_ORG', 'DATABASE_NAME', 'SCHEMA_NAME', 'TABLE_NAME', 'PACKAGE_ID', 'RESOURCE_ID', 'EXTENSION', 'COMPRESSED']

def clean_for_save(df):
    #Clicking into the editor's blank last row adds an empty row; drop those, and report rows missing required values.
    blank = df.apply(lambda c: c.isna() | (c.astype(str).str.strip() == '')).all(axis=1)
    df = df[~blank]
    missing = df[REQUIRED_COLUMNS].apply(lambda c: c.isna() | (c.astype(str).str.strip() == ''))
    bad = [f"row {i + 1}: {', '.join(missing.columns[missing.loc[idx]])}" for i, idx in enumerate(df.index) if missing.loc[idx].any()]
    return df, bad

if btnSave:
    with st.spinner("Saving resources..."):
        try:
            to_save, bad_rows = clean_for_save(edited)
            if bad_rows:
                raise ValueError('Fill in the required values or delete the row. Missing ' + '; '.join(bad_rows))
            session.write_pandas(to_save, "RESOURCES_TEMP", schema="CORE", auto_create_table=True, overwrite=True, quote_identifiers=False)
            session.sql("INSERT OVERWRITE INTO core.resources select OWNER_ORG,DATABASE_NAME,SCHEMA_NAME,TABLE_NAME,PACKAGE_ID,RESOURCE_ID,PRESIGNED_URL,CURRENT_TIMESTAMP(),FILE_NAME,EXTENSION,COMPRESSED from core.resources_temp").collect()
            st.session_state.manage_msg = ('success', f'Saved {len(to_save)} row(s). The next scheduled refresh publishes them, or click Re-Publish to publish now.')
        except Exception as ex:
            logger.error(ex)
            st.session_state.manage_msg = ('error', f'Save failed: {ex}')
            #Keep the user's edits on screen so they can fix them.
            st.session_state.resources_df = edited
            st.session_state.editor_version += 1
            st.rerun()
    load_resources()
    st.rerun()

if btnRepublish:
    with st.spinner("Updating resources..."):
        try:
            session.sql("UPDATE CORE.RESOURCES SET DATE_UPDATED = CURRENT_TIMESTAMP()").collect()
            result = session.sql("call CONFIG.SP_UPDATE_RESOURCES_ALL()").collect()[0][0]
            if result == 'FAILURE':
                st.session_state.manage_msg = ('error', 'Re-Publish failed. See the Status page for the CKAN error.')
            else: 
                st.session_state.manage_msg = ('success', 'Re-Published. Check the Status page for each resource result.')
        except Exception as ex:
            logger.error(ex)
            st.session_state.manage_msg = ('error', f'Re-Publish failed: {ex}')
    load_resources()
    st.rerun()

if btnRefresh:
    load_resources()
    st.rerun()

if 'manage_msg' in st.session_state:
    level, msg = st.session_state.pop('manage_msg')
    getattr(st, level)(msg)

st.header('Re-map to a new portal')
st.info('Use this after the CKAN portal moves (for example a new publisher backend) and package or resource IDs changed. '
        'Pick the matching package and resource on the current portal for each published table, then update and re-publish.')
try:
    published = session.sql("""SELECT DISTINCT owner_org, database_name, schema_name, table_name, package_id, resource_id
                               FROM core.resources ORDER BY table_name""").collect()
except Exception as ex:
    logger.error(ex)
    published = []

if published:
    labels = [f"{r['DATABASE_NAME']}.{r['SCHEMA_NAME']}.{r['TABLE_NAME']}" for r in published]
    choice = st.selectbox('Published table', options=range(len(published)), format_func=lambda i: labels[i], key='remap_table')
    row = published[choice]
    st.write(f"Current package id: `{row['PACKAGE_ID']}`  resource id: `{row['RESOURCE_ID']}`  on host **{util.get_ckan_url()}**")
    try:
        orgs = util.ckan_orgs()
        org_index = orgs.index(row['OWNER_ORG']) if row['OWNER_ORG'] in orgs else 0
        org = st.selectbox('Owner org on current portal', options=orgs, index=org_index, key='remap_org')
        resources = util.ckan_packages(org) if org else []
        packages = sorted({r['package_name'] for r in resources})
        package = st.selectbox('Package', options=packages, key='remap_package')
        pkg_resources = [r for r in resources if r['package_name'] == package]
        res_index = st.selectbox('Resource', options=range(len(pkg_resources)),
                                 format_func=lambda i: pkg_resources[i]['resource_name'], key='remap_resource')
    except Exception as ex:
        logger.error(ex)
        st.error(f'Could not read the current portal. Check the API key, URL and external access on the Initialize page. {ex}', icon='🚨')
        pkg_resources = []

    if pkg_resources and st.button('Update IDs and re-publish', type='primary', key='remap_save'):
        target = pkg_resources[res_index]
        try:
            with st.spinner('Updating and publishing...'):
                #Clearing the url puts the row in the stream, so SP_UPDATE_RESOURCES re-unloads and calls CKAN with the new ids.
                session.sql("""UPDATE core.resources
                               SET owner_org = ?, package_id = ?, resource_id = ?, presigned_url = NULL, date_updated = current_timestamp()
                               WHERE database_name = ? AND schema_name = ? AND table_name = ?""",
                            params=[org, target['package_id'], target['resource_id'],
                                    row['DATABASE_NAME'], row['SCHEMA_NAME'], row['TABLE_NAME']]).collect()
                result = session.sql("CALL config.sp_update_resources(?)", params=[row['TABLE_NAME']]).collect()[0][0]
            load_resources()
            last = session.sql("""SELECT message FROM core.ckan_log WHERE table_name = ? AND message NOT LIKE 'SP_UPDATE_RESOURCES%'
                                  ORDER BY dt DESC LIMIT 1""", params=[row['TABLE_NAME']]).collect()
            last_msg = last[0][0] if last else ''
            if result == 'FAILURE' or last_msg.startswith('resource update failed'):
                st.error(f'Publishing failed: {last_msg or "see the Status page"}', icon='🚨')
            else:
                st.success(f"Re-mapped to {package} / {target['resource_name']} and published.", icon='✅')
        except Exception as ex:
            logger.error(ex)
            st.error(f'Re-map failed: {ex}', icon='🚨')
else:
    st.write('No published tables yet.')

