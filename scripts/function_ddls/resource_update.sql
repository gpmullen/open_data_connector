CREATE OR REPLACE FUNCTION config.resource_update(resource_id string, extension string, presigned_url string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'resource_update'
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

def resource_update(resource_id, extension, presigned_url):
  try:
    logger.info('Begin API call to update resource')
    token = _snowflake.get_generic_secret_string('cred')
    url = "https://{1}/api/action/resource_update"
    #Newer CKAN API tokens use Authorization; X-CKAN-API-Key is kept for older CKAN versions.
    headers = {{"Authorization": token, "X-CKAN-API-Key": token}}
    json_options = {{'id':resource_id,'format':extension ,'url':presigned_url, 'clear_upload':'true'}}
    response = session.post(url, headers = headers, json = json_options, timeout = 60)
    logger.info('End API call to update resource')
    body = response.json()
    if response.status_code != 200 or not body.get('success'):
      #No 'id' key, so callers can tell the update failed; the CKAN error is kept for ckan_log.
      return json.dumps({{'error': body.get('error', body), 'status': response.status_code}})
    return json.dumps(body['result'])
  except Exception as ex:
    logger.error(ex)
    return json.dumps({{'error': str(ex)}})
$$;
  
GRANT USAGE ON FUNCTION config.resource_update(string, string, string) TO APPLICATION ROLE ckan_app_role;
