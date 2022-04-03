# Dashboard reload BigQuery trigger table 

This function uses Google Cloud Platform Logging to detect changes in a BigQuery table (specifically, new data adding to the table), and updated another BigQuery table that contains all the update jobs informations.

This latest one is thought to trigger an event, as the reload of a dashboard in Qliksense.
