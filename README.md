# pipeline
This code imports the results of a query to a SQL database into a BigQuery table. The table is dynamically created based on the schema of the query data, and then the query results are inserted into the table. The following steps are performed: The necessary libraries are imported.
, the environment variable for the GCP project ID is set and the connection string
