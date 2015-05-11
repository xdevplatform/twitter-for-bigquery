import os
import sys
import time
import logging.config
import urllib2
import base64
import zlib
import threading
from threading import Lock
import json
import sys
import ssl
import tweepy
from httplib import *
from bigquery import get_client
from bigquery import schema_from_record
from config import *
from utils import Utils

CHUNKSIZE = 4*1024
GNIPKEEPALIVE = 30  # seconds
NEWLINE = '\r\n'

HEADERS = { 'Accept': 'application/json',
            'Connection': 'Keep-Alive',
            'Accept-Encoding' : 'gzip',
            'Authorization' : 'Basic %s' % base64.encodestring('%s:%s' % (GNIP_USERNAME, GNIP_PASSWORD))  }

print_lock = Lock()
err_lock = Lock()

class procEntry(threading.Thread):
    def __init__(self, buf):
        self.buf = buf
        threading.Thread.__init__(self)

    def run(self):
        for rec in [x.strip() for x in self.buf.split(NEWLINE) if x.strip() <> '']:
            try:
                jrec = json.loads(rec.strip())
                tmp = json.dumps(jrec)
                with print_lock:
                    print "\n"
                    #print(tmp)
            except ValueError, e:
                with err_lock:
                    sys.stderr.write("Error processing JSON: %s (%s)\n"%(str(e), rec))


#Passing a BigQueryGnipListener object by reference
def getStream(BigQueryGnipListener):
    req = urllib2.Request(GNIP_URL, headers=HEADERS)
    response = urllib2.urlopen(req, timeout=(1+GNIPKEEPALIVE))
    # header -  print response.info()
    decompressor = zlib.decompressobj(16+zlib.MAX_WBITS)
    remainder = ''
    while True:
        tmp = decompressor.decompress(response.read(CHUNKSIZE))
        if tmp == '':
            return
        [records, remainder] = ''.join([remainder, tmp]).rsplit(NEWLINE,1)
        #Method to 
        BigQueryGnipListener.on_data(records)
        print 
        procEntry(records).start()
# Write records to BigQuery

class BigQueryGnipListener(object):
    """docstring for ClassName"""
    def __init__(self, client, dataset_id, table_id, logger=None):

      self.client = client
      self.dataset_id = dataset_id
      self.table_id = table_id
      self.count = 0
      self.logger = logger
      
    def on_data(self, data):

        print "Data type: %s" % type(data)
        print "Data: " + data
        
        records_str = data.strip().split(NEWLINE)

        print "Records: %s" % len(records_str) 
        
        for r in records_str:
        
            # Twitter returns data in JSON format - we need to decode it first
#             temp_str = json.dumps(data)
            print "********************"
            print "Record type: %s" % type(r)
            print "Record: " + r 

            record = json.loads(r)
    
            if not record.get('delete', None):
    
                record_scrubbed = Utils.scrub(record)
                print "Scrubbed Record"
                print record_scrubbed
                Utils.insert_record(self.client, self.dataset_id, self.table_id, record_scrubbed)
                
                if self.logger:
                    self.logger.info('@%s: %s' % (record['actor']['preferredUsername'], record['body'].encode('ascii', 'ignore')))
                
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
    
    schema_str = Utils.read_file(GNIP_SCHEMA_FILE)
    schema = json.loads(schema_str)
    
    # create table BigQuery table
    created = client.create_table(DATASET_ID, TABLE_ID, schema)
    logger.info("created result: %s" % created)
#     if (len(created) == 0):
#         print "failed to create table"
#         return
    
    l = BigQueryGnipListener(client, DATASET_ID, TABLE_ID, logger=logger)
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
 
    while True:

        stream = None

        try:
            getStream(l)
            # stream = tweepy.Stream(auth, l, headers = {"Accept-Encoding": "deflate, gzip"})
            #stream = tweepy.Stream(auth, l)

            # Choose stream: filtered or sample
            # stream.sample()
            #stream.filter(track=TRACK_ITEMS) # async=True
            
        except:

            logger.exception("Unexpected error:");

            if stream:
                stream.disconnect()

            time.sleep(60)

    # Can also test loading data from a local file
    # Utils.import_from_file(client, DATASET_ID, TABLE_ID, 'data/sample_stream.jsonr', single_tweet=False)

if __name__ == "__main__":
    main()
