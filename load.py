import json

from bigquery import get_client
from bigquery import schema_from_record
from config import *

record = None
with open (record_file, "r") as myfile:
    data = myfile.read()
    record = json.loads(data)
 
print record
 
result = schema_from_record(record)
print result

# client = get_client(project_id, service_account=service_account, private_key=key, readonly=True)
# 
# # Submit an async query.
# job_id, _results = client.query('SELECT * FROM dataset.my_table LIMIT 1000')
# 
# # Check if the query has finished running.
# complete, row_count = client.check_job(job_id)
# 
# # Retrieve the results.
# results = client.get_query_rows(job_id)