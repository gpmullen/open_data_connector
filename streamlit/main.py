import streamlit as st
from snowflake.snowpark.context import get_active_session
import snowflake.permissions as permissions;

session = get_active_session()
st.set_page_config(layout="wide")

st.header('How to use the Open Data Portal Connector')
st.warning('The Open Data Connector publishes data to a public portal. You are responsible for only publishing data that is meant for public distribution.')
st.info('Step 1: Initialize the App by performing some one-time setup steps')
st.info('Step 2: Select and publish a table using a listing from the Open Data Portal')
st.info('Step 3: Remove any tables that should no longer receive updates')
st.info('Step 4: Review and track the status of jobs')

if not permissions.get_held_account_privileges(["EXECUTE TASK"]):
    st.error("The app needs EXECUTE TASK permissions to regularly update data and links to CKAN.")
    permissions.request_account_privileges(["EXECUTE TASK"])

if not permissions.get_held_account_privileges(["EXECUTE MANAGED TASK"]):
    st.error("The app needs EXECUTE MANAGED TASK permissions to regularly update data and links to CKAN.")
    permissions.request_account_privileges(["EXECUTE MANAGED TASK"])
