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
from config import Config
from bigquery import get_client
from bigquery import schema_from_record

from utils import Utils

f = file("./config")
config = Config(f)

SLEEP_TIME = 10
CHUNKSIZE = 4*1024
GNIPKEEPALIVE = 30  # seconds
NEWLINE = '\r\n'

HEADERS = { 'Accept': 'application/json',
            'Connection': 'Keep-Alive',
            'Accept-Encoding' : 'gzip',
            'Authorization' : 'Basic %s' % base64.encodestring('%s:%s' % (config.GNIP_USERNAME, config.GNIP_PASSWORD))  }

KEY = Utils.read_file(config.KEY_FILE)

print_lock = Lock()
err_lock = Lock()

class proc_entry(threading.Thread):
    
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
def get_stream(BigQueryGnipListener):
    req = urllib2.Request(config.GNIP_STREAM_URL, headers=HEADERS)
    response = urllib2.urlopen(req, timeout=(1+GNIPKEEPALIVE))
    # header -  print response.info()
    decompressor = zlib.decompressobj(16+zlib.MAX_WBITS)
    remainder = ''
    while True:
        tmp = decompressor.decompress(response.read(CHUNKSIZE))
        if tmp == '':
            return
        [records, remainder] = ''.join([remainder, tmp]).rsplit(NEWLINE,1)
        BigQueryGnipListener.on_data(records)
        proc_entry(records).start()
        
# Write records to BigQuery

class BigQueryGnipListener(object):
    
    """docstring for ClassName"""
    def __init__(self, client, schema, table_mapping, logger=None):

      self.client = client
      self.schema = schema
      self.table_mapping = table_mapping
      self.default_table = table_mapping.values()[0]
      self.count = 0
      self.logger = logger
      
    def on_data(self, data):

        # get bulk records, but process individually based on tag-based routing 
        records_str = data.strip().split(NEWLINE)
        for r in records_str:
        
            record = json.loads(r)
            if not record.get('delete', None):
                
                tags = self.get_table_tags(record)

                if not tags:
                    tags = [self.default_table]
                
                # process multiple tags on a record
                for tag in tags:
                
                    table = self.table_mapping.get(tag, None)
                    if not table:
                        table = tag.split(".")
                        created = self.client.create_table(table[0], table[1], self.schema)
                        if created:
                            self.table_mapping[tag] = table
                            self.logger.info('Created BQ table: %s' % tag)
    
                    record_scrubbed = Utils.scrub(record)
                    Utils.insert_record(self.client, table[0], table[1], record_scrubbed)
                
                if self.logger:
                    self.logger.info('@%s: %s (%s)' % (record['actor']['preferredUsername'], record['body'].encode('ascii', 'ignore'), tags))
                
                self.count = self.count + 1
                
        return True
    
    def get_table_tags(self, record):
        gnip = record.get('gnip', None)
        if gnip:
            matching_rules = gnip.get('matching_rules', None)
            if matching_rules:
                return [rule.get("tag", None) for rule in matching_rules]
            
        return None

    #handle errors without closing stream:
    def on_error(self, status_code):

        if status_code == 420:

            time.sleep(SLEEP_TIME)

            if self.logger:
                self.logger.info("420, sleeping for %s seconds" % SLEEP_TIME)

            return True
        
        if self.logger:
            self.logger.info("Error with status code: %s" % status_code)

        return False 

    def on_timeout(self):
        
        time.sleep(SLEEP_TIME)

        if self.logger:
            self.logger.info("Timeout, sleeping for %s seconds" % SLEEP_TIME)

        return False 

    def on_exception(self, exception):
        
        if self.logger:
            self.logger.exception('Exception')

        return False 

def main():
    
    logger = Utils.enable_logging()
    
    # get client
    client = get_client(config.PROJECT_ID, service_account=config.SERVICE_ACCOUNT, private_key=KEY, readonly=False)
    client.swallow_results = False
    logger.info("client: %s" % client)
    
    schema_str = Utils.read_file(config.SCHEMA_FILE)
    schema = json.loads(schema_str)
    
    # initialize table mapping for default table
    table_mapping = {
         config.DATASET_ID + "." + config.TABLE_ID : [config.DATASET_ID, config.TABLE_ID]
     }
    
    l = BigQueryGnipListener(client, schema, table_mapping, logger=logger)
 
    while True:

        stream = None

        try:
            get_stream(l)
            
        except:

            logger.exception("Unexpected error:");

            if stream:
                stream.disconnect()

            time.sleep(SLEEP_TIME)

    # Can also test loading data from a local file
    # Utils.import_from_file(client, DATASET_ID, TABLE_ID, 'data/sample_stream.jsonr', single_tweet=False)

if __name__ == "__main__":
    main()
