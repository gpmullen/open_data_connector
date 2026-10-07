CREATE OR REPLACE FUNCTION config.package_search(org_id string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'package_search'
EXTERNAL_ACCESS_INTEGRATIONS = ({0})
PACKAGES = ('snowflake-snowpark-python','requests')
SECRETS = ('cred' = core.ckan_api_key )
AS
$$
import _snowflake
import requests
import json
import logging
session = requests.Session()
logger = logging.getLogger("python_logger")

def package_search(org_id):
  try:
    token = _snowflake.get_generic_secret_string('cred')
    url = "https://{1}/api/action/package_search"
    #Newer CKAN API tokens use Authorization; X-CKAN-API-Key is kept for older CKAN versions.
    headers = {{"Authorization": token, "X-CKAN-API-Key": token}}
    params = {{"fq": "organization:" + org_id, "include_private": "true", "rows": 1000}}
    response = session.get(url, headers = headers, params = params, timeout = 60)
    body = response.json()
    if response.status_code != 200 or not body.get('success'):
      return json.dumps({{'error': body.get('error', body), 'status': response.status_code}})
    return json.dumps(body['result'])
  except Exception as ex:
    logger.error(ex)
    return json.dumps({{'error': str(ex)}})
$$;
  
GRANT USAGE ON FUNCTION config.package_search(string) TO APPLICATION ROLE ckan_app_role;
