CREATE OR REPLACE FUNCTION config.get_orgs()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'get_orgs'
EXTERNAL_ACCESS_INTEGRATIONS = ({0})
PACKAGES = ('snowflake-snowpark-python','requests')
SECRETS = ('cred' = core.ckan_api_key )
AS
$$
import _snowflake
import requests
import json
import logging
logger = logging.getLogger("python_logger")
session = requests.Session()

def get_orgs():
  try:
    token = _snowflake.get_generic_secret_string('cred')
    url = "https://{1}/api/action/organization_list_for_user"
    #Newer CKAN API tokens use Authorization; X-CKAN-API-Key is kept for older CKAN versions.
    headers = {{"Authorization": token, "X-CKAN-API-Key": token}}
    response = session.get(url, headers = headers, timeout = 60)
    body = response.json()
    if response.status_code != 200 or not body.get('success'):
      return json.dumps({{'error': body.get('error', body), 'status': response.status_code}})
    return json.dumps(body['result'])
  except Exception as ex:
    logger.error(ex)
    return json.dumps({{'error': str(ex)}})
$$;
  
GRANT USAGE ON FUNCTION config.get_orgs() TO APPLICATION ROLE ckan_app_role;
