CREATE OR REPLACE FUNCTION config.resource_renew_url(resource_id string, presigned_url string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'resource_renew_url'
EXTERNAL_ACCESS_INTEGRATIONS = ({0})
PACKAGES = ('requests')
SECRETS = ('cred' = core.ckan_api_key)
AS
$$
import _snowflake
import requests
import json
import logging
session = requests.Session()
logger = logging.getLogger("python_logger")

def resource_renew_url(resource_id, presigned_url):
  try:
    token = _snowflake.get_generic_secret_string('cred')
    headers = {{"Authorization": token, "X-CKAN-API-Key": token}}
    #PATCH preserves last_modified and all other metadata omitted from this request.
    response = session.post("https://{1}/api/action/resource_patch", headers=headers,
                            json={{'id':resource_id, 'url':presigned_url}}, timeout=60)
    body = response.json()
    if response.status_code != 200 or not body.get('success'):
      return json.dumps({{'error': body.get('error', body), 'status': response.status_code}})
    return json.dumps(body['result'])
  except Exception as ex:
    logger.error(ex)
    return json.dumps({{'error': str(ex)}})
$$;

GRANT USAGE ON FUNCTION config.resource_renew_url(string, string) TO APPLICATION ROLE ckan_app_role;
