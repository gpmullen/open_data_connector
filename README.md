# CKAN_OPEN_DATA_CONNECTOR
Open Data Connector Native App for Sharing Data with CKAN open-source DMS (data management system)
This source code is meant to be deployed as part of the Snowflake Native App Framework on the Snowflake Marketplace.
Review this article for [details](https://medium.com/@gabriel.mullen/california-open-data-connector-in-snowflake-using-native-app-framework-6e381291edde).

## Pre-Requistes
* This connector assumes that you have a hosted CKAN portal (e.g. data.ca.gov). 
* You have a user for which you can generate an authentication token for. 
* You have created a CKAN package and a CKAN resource to target. This allows Open Data Users to create the metadata within CKAN's UI. You do not need to populate the CKAN resource, but the connector will map the data to a specific resource-id that you choose through the connector ui.

Once you have those items in place, you can configure the connector through the UI to connect and map a Snowflake table to your CKAN instance.

## TODO
* Update documentation and connector based on new product changes.