import json

from bigquery import get_client
from bigquery import schema_from_record

from utils import *
from config import *

def generate_schema_from_tweet():
    
    record_str = read_file(SAMPLE_TWEET_FILE)
    record = json.loads(record_str)
    schema_str = schema_from_record(record)
    print schema_str
    exit()

def insert_record(client, record, schema_str=None):
     
    result = client.push_rows(DATASET_ID, TABLE_ID, [record], None) 
     
    if result.get('insertErrors', None):

        print "Record: %s" % (json.dumps(record))
        print "Error result: %s" % result
        
        if schema_str:
            print "Missing: "
            ak = all_keys(record)
      
            for k in ak:
                key = '"' + k + '"'
                has = key in schema_str 
                if not has:
                    print "%s: %s" % (key, has)
                    break

        return False
    
    return True

def main():
    
    enable_logging()
    
    # get client
    client = get_client(PROJECT_ID, service_account=SERVICE_ACCOUNT, private_key=KEY, readonly=False)
    client.swallow_results = False
    print "client: %s" % client
    
    schema_str = read_file(SCHEMA_FILE)
    schema = json.loads(schema_str)
    
    # un-comment to create table
    created = client.create_table(DATASET_ID, TABLE_ID, schema)
    print "created: %s" % created
    
    # un-comment to load a single file
#     record = json.loads(read_file(SAMPLE_TWEET_FILE))
#     success = insert_record(client, record, schema_str)
#     print "Single record: %s" % success
#     if not success:
#         return
    
    row = 0
    with open(SAMPLE_STREAM_FILE, "r") as f:
        
        for tweet in f:
            
            print "Process row: %s" % row
             
            record = json.loads(tweet)

            # ignore delete records for now            
            if record.get("delete", None):
                continue
            
            record_scrubbed = scrub(record)
            success = insert_record(client, record_scrubbed, schema_str)
            if not success:
                print "Record: %s" % (json.dumps(record))
                return
            
            row = row + 1

if __name__ == "__main__":
    main()

# # Submit an async query.
# job_id, _results = client.query('SELECT * FROM dataset.my_table LIMIT 1000')
# 
# # Check if the query has finished running.
# complete, row_count = client.check_job(job_id)
# 
# # Retrieve the results.
# results = client.get_query_rows(job_id)