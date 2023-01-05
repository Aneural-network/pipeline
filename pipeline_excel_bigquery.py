# Import the required libraries
import os
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1beta1
from pymongo import MongoClient

# Set the environment variable for your GCP project ID
os.environ["GOOGLE_CLOUD_PROJECT"] = "your-project-id"

# Connect to the MongoDB database
client = MongoClient("mongodb://your-username:your-password@your-server-name:27017/")
db = client["your-database-name"]

# Select the collection you want to import
collection = db["your-collection-name"]

# Fetch the documents from the collection
documents = list(collection.find())

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
for document in documents:
    rows_to_insert.append(
        {"column1": document["column1"], "column2": document["column2"], "column3": document["column3"]}
    )

# Insert the rows into the table
errors = client.insert_rows(table, rows_to_insert)

# Print any errors
if errors != []:
    print(errors)
