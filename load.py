import json
import tweepy

from bigquery import get_client
from bigquery import schema_from_record

from utils import Utils, BigQueryListener
from config import *

def main():
    
    Utils.enable_logging()
    
    # get client
    client = get_client(PROJECT_ID, service_account=SERVICE_ACCOUNT, private_key=KEY, readonly=False)
    client.swallow_results = False
    print "client: %s" % client
    
    schema_str = Utils.read_file(SCHEMA_FILE)
    schema = json.loads(schema_str)
    
    # create table BigQuery table
    created = client.create_table(DATASET_ID, TABLE_ID, schema)
    print "created: %s" % created
    
#     l = BigQueryListener(client, DATASET_ID, TABLE_ID)
#     auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
#     auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
# 
#     stream = tweepy.Stream(auth, l)
#     # Choose stream: filtered or sample
#     # stream.filter(track=['programming'])
#     stream.sample()

    Utils.import_from_file(client, DATASET_ID, TABLE_ID, 'data/sample_stream.jsonr', single_tweet=False)

if __name__ == "__main__":
    main()