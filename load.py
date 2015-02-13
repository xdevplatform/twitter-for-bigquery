import json
import tweepy

from httplib import *

from bigquery import get_client
from bigquery import schema_from_record

from utils import Utils, BigQueryListener
from config import *

TRACK_ITEMS = [
    '@JetBlue',
    '@southwestair',
    '@AirAsia',
    '@AmericanAir',
    '@flyPAL',
    '@TAMAirlines',
    '@Delta',
    '@virginamerica',
    '@klm',
    '@turkishairlines',
    '@BritishAirways',
    '@usairways',
    '@British_Airways',
    '@westjet',
    '@MAS',
    '@United',
    '@baltiausa',
    '@Lufthansa_DE',
    '@virginatlantic',
    '@virginaustralia',
    '@AlaskaAir',
    '@DeltaAssist',
    '@aircanada',
    '@easyJet',
    '@vueling'
]

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
    print "created result: %s" % created
#     if (len(created) == 0):
#         print "failed to create table"
#         return
    
    l = BigQueryListener(client, DATASET_ID, TABLE_ID)
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
 
    stream = tweepy.Stream(auth, l)
    
    while True:
        try:
            
            # Choose stream: filtered or sample
            # stream.sample()
            stream.filter(track=TRACK_ITEMS)
            
        except BadStatusLine:
            pass
        except IncompleteRead:
            pass
        except:
            print "Unexpected error:", sys.exc_info()[0]

    # Can also test loading data from a local file
    # Utils.import_from_file(client, DATASET_ID, TABLE_ID, 'data/sample_stream.jsonr', single_tweet=False)

if __name__ == "__main__":
    main()
