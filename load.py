import os
import sys
import time
import logging.config

import json
import tweepy

from httplib import *

from bigquery import get_client

from config import *
from utils import Utils

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

# Write records to BigQuery
class BigQueryListener(tweepy.StreamListener):
    
    def __init__(self, client, dataset_id, table_id, logger=None):

      self.client = client
      self.dataset_id = dataset_id
      self.table_id = table_id
      self.count = 0
      self.logger = logger
      
    def on_data(self, data):
        
        # Twitter returns data in JSON format - we need to decode it first
        record = json.loads(data)
        
        if not record.get('delete', None):

            record_scrubbed = Utils.scrub(record)
            Utils.insert_record(self.client, self.dataset_id, self.table_id, record_scrubbed)
            
            if self.logger:
                self.logger.info('@%s: %s' % (record['user']['screen_name'], record['text'].encode('ascii', 'ignore')))
            
            self.count = self.count + 1
            
            return True

    #handle errors without closing stream:
    def on_error(self, status_code):

        if status_code == 420:

            time.sleep(60)

            if self.logger:
                self.logger.info('420, sleeping for 60 seconds')

            return True
        
        if self.logger:
            self.logger.info('Error with status code: %s' % status_code)

        return False 

    def on_timeout(self):
        
        time.sleep(60)

        if self.logger:
            self.logger.info('Timeout, sleeping for 60 seconds')

        return False 

    def on_exception(self, exception):
        
        if self.logger:
            self.logger.exception('Exception')

        return False 

def main():
    
    logger = Utils.enable_logging()
    
    # get client
    client = get_client(PROJECT_ID, service_account=SERVICE_ACCOUNT, private_key=KEY, readonly=False)
    client.swallow_results = False
    logger.info("client: %s" % client)
    
    schema_str = Utils.read_file(SCHEMA_FILE)
    schema = json.loads(schema_str)
    
    # create table BigQuery table
    created = client.create_table(DATASET_ID, TABLE_ID, schema)
    logger.info("created result: %s" % created)
#     if (len(created) == 0):
#         print "failed to create table"
#         return
    
    l = BigQueryListener(client, DATASET_ID, TABLE_ID, logger=logger)
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
 
    while True:

        stream = None

        try:
            
            # stream = tweepy.Stream(auth, l, headers = {"Accept-Encoding": "deflate, gzip"})
            stream = tweepy.Stream(auth, l)

            # Choose stream: filtered or sample
            # stream.sample()
            stream.filter(track=TRACK_ITEMS) # async=True
            
        except:

            logger.exception("Unexpected error:");

            if stream:
                stream.disconnect()

            time.sleep(60)

    # Can also test loading data from a local file
    # Utils.import_from_file(client, DATASET_ID, TABLE_ID, 'data/sample_stream.jsonr', single_tweet=False)

if __name__ == "__main__":
    main()
