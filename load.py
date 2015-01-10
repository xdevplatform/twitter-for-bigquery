import json

from bigquery import get_client
from bigquery import schema_from_record
from config import *

record = None
with open (RECORD_FILE, "r") as myfile:
    data = myfile.read()
    record = json.loads(data)
 
# create schema
schema = schema_from_record(record)

print schema

# get client
client = get_client(PROJECT_ID, service_account=SERVICE_ACCOUNT, private_key=KEY, readonly=True)

# create table
created = client.create_table(DATASET_ID, TABLE_ID, schema)

print created
 
# # Submit an async query.
# job_id, _results = client.query('SELECT * FROM dataset.my_table LIMIT 1000')
# 
# # Check if the query has finished running.
# complete, row_count = client.check_job(job_id)
# 
# # Retrieve the results.
# results = client.get_query_rows(job_id)