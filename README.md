# CKAN_OPEN_DATA_CONNECTOR
Open Data Connector Native App for Sharing Data with CKAN open-source DMS (data management system)
This source code is meant to be deployed as part of the Snowflake Native App Framework on the Snowflake Marketplace.
Review this article for [details](https://medium.com/@gabriel.mullen/california-open-data-connector-in-snowflake-using-native-app-framework-6e381291edde).

## Pre-Requistes
* This connector assumes that you have a hosted CKAN portal (e.g. data.ca.gov). 
* You have a user for which you can generate an authentication token for. 
* You have created a CKAN package and a CKAN resource to target. This allows Open Data Users to create the metadata within CKAN's UI. You do not need to populate the CKAN resource, but the connector will map the data to a specific resource-id that you choose through the connector ui.

Once you have those items in place, you can configure the connector through the UI to connect and map a Snowflake table to your CKAN instance.

## Data update timestamps

Data publishes, including manual Re-Publish, send CKAN `last_modified` as the current UTC time in `YYYY-MM-DDTHH:MM:SS` format, without a timezone suffix. This is the publication time, not the source table's last DML time. Scheduled publishes retain the existing `LAST_ALTERED` change detection.

The six-day URL renewal task uses CKAN `resource_patch` to renew links to existing staged files. It does not export data, consume the publish stream, or change `last_modified`. A null timestamp remains null until the next data publish. `core.resources.presigned_url` and `date_updated` describe the last data publish, not subsequent link renewals; the current renewed link is in CKAN.

On upgrade, setup rebuilds the CKAN functions before the tasks. Deploy `scripts/function_ddls/resource_renew_url.sql` with the other UDF templates. Existing resources receive a timestamp on their next scheduled publish or manual Re-Publish.

Run offline tests without Snowflake or CKAN access:

```bash
python3 -B -m unittest discover -s tests -v
```

## TODO
* Update documentation and connector based on new product changes.