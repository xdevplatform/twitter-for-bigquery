import json

from bigquery import get_client
from bigquery import schema_from_record

from utils import *
from config import *

# import logging
# import sys
# 
# root = logging.getLogger()
# root.setLevel(logging.DEBUG)
# 
# ch = logging.StreamHandler(sys.stdout)
# ch.setLevel(logging.DEBUG)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# ch.setFormatter(formatter)
# root.addHandler(ch)

schema = ""
with open (SCHEMA_FILE, "r") as f:
    schema = schema + f.read()
schema = json.loads(schema)
  
# print "schema: %s" % schema

# get client
client = get_client(PROJECT_ID, service_account=SERVICE_ACCOUNT, private_key=KEY, readonly=False)
client.swallow_results = False

print "client: %s" % client

# create table
created = client.create_table(DATASET_ID, TABLE_ID, schema)
 
print "created: %s" % created

# tweet = ""
# with open(RECORD_FILE, "r") as f:
#     for line in f:
#         tweet = tweet + line
# 
# record = json.loads(tweet)
# print "record: %s" % record
# inserted = client.push_rows(DATASET_ID, TABLE_ID, [record], None) 
# print "inserted: %s" % inserted

row = 0

with open(RECORDS_FILE, "r") as f:
    for tweet in f:
        
        record = json.loads(tweet)
        record = scrub(record)
        
        inserted = client.push_rows(DATASET_ID, TABLE_ID, [record], None) 
        
        if inserted['insertErrors']:
            print "record %s: %s" % (row, record)
            print inserted
            break
        
        row = row + 1

# # Submit an async query.
# job_id, _results = client.query('SELECT * FROM dataset.my_table LIMIT 1000')
# 
# # Check if the query has finished running.
# complete, row_count = client.check_job(job_id)
# 
# # Retrieve the results.
# results = client.get_query_rows(job_id)