# Import the required libraries
import os
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1beta1
import pyodbc

# Set the environment variable for your GCP project ID
os.environ["GOOGLE_CLOUD_PROJECT"] = "your-project-id"

# Set the connection string for your SQL database
conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=your-server-name;DATABASE=your-database-name;UID=your-username;PWD=your-password'

# Connect to the SQL database
conn = pyodbc.connect(conn_str)

# Create a cursor
cursor = conn.cursor()

# Execute a SQL query to retrieve the data you want to import
query = "SELECT * FROM your-table"
cursor.execute(query)

# Fetch the results of the query
results = cursor.fetchall()

# Create a new BigQuery client
client = bigquery.Client()

# Set the destination BigQuery dataset and table
dataset_id = "your-dataset-id"
table_id = "your-table-id"

# Construct the fully-qualified table ID
table_ref = client.dataset(dataset_id).table(table_id)

# Check if the table exists
try:
    client.get_table(table_ref)
    table_exists = True
except:
    table_exists = False

# If the table exists, delete it
if table_exists:
    client.delete_table(table_ref)

# Create a new table in BigQuery
table = bigquery.Table(table_ref)

# Set the schema for the new table
table.schema = [
    bigquery.SchemaField("column1", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("column2", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("column3", "FLOAT", mode="REQUIRED"),
]

# Create the table
table = client.create_table(table)

# Construct a list of rows to insert into the table
rows_to_insert = []
for row in results:
    rows_to_insert.append(
        {"column1": row[0], "column2": row[1], "column3": row[2]}
    )

# Insert the rows into the table
errors = client.insert_rows(table, rows_to_insert)

# Print any errors
if errors != []:
    print(errors)

# Close the cursor and connection
cursor.close()
conn.close()
