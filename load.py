import os
import sys
import time
import urllib2
import base64
import json
import ssl
import zlib
import threading
from threading import Lock

import logging.config
from httplib import *
from config import Config
import tweepy

from utils import Utils

NEWLINE = '\r\n'
SLEEP_TIME = 10

f = file("./config")
config = Config(f)

class GnipListener(object):
    
    CHUNK_SIZE = 4 * 1024
    KEEP_ALIVE = 30  # seconds
    
    HEADERS = { 'Accept': 'application/json',
                'Connection': 'Keep-Alive',
                'Accept-Encoding' : 'gzip',
                'Authorization' : 'Basic %s' % base64.encodestring('%s:%s' % (config.GNIP_USERNAME, config.GNIP_PASSWORD))  }    
    
    """docstring for ClassName"""
    def __init__(self, schema, table_mapping, logger=None):

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
                    Utils.insert_records(table[0], table[1], [record_scrubbed])
                
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

    @staticmethod
    def start(schema, logger):        
        
        # initialize table mapping for default table
        table_mapping = {
             config.DATASET_ID + "." + config.TABLE_ID : [config.DATASET_ID, config.TABLE_ID]
         }
        
        listener = GnipListener(schema, table_mapping, logger=logger)
     
        while True:
    
            stream = None
    
            try:
                
                req = urllib2.Request(config.GNIP_STREAM_URL, headers=GnipListener.HEADERS)
                response = urllib2.urlopen(req, timeout=(1+GnipListener.KEEP_ALIVE))
    
                decompressor = zlib.decompressobj(16+zlib.MAX_WBITS)
                remainder = ''
                while True:
                    tmp = decompressor.decompress(response.read(GnipListener.CHUNK_SIZE))
                    if tmp == '':
                        return
                    [records, remainder] = ''.join([remainder, tmp]).rsplit(NEWLINE,1)
                    listener.on_data(records)
    
                get_stream(listener)
                
            except:
    
                logger.exception("Unexpected error:");
    
                if stream:
                    stream.disconnect()
    
                time.sleep(SLEEP_TIME)
    
# Write records to BigQuery
class TwitterListener(tweepy.StreamListener):
    
    # items to track if you're doing a public track call
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
    
    def __init__(self, dataset_id, table_id, logger=None):

      self.dataset_id = dataset_id
      self.table_id = table_id
      self.count = 0
      self.logger = logger
      self.calm_count = 0
      
    def on_data(self, data):
        
        self.calm_count = 0
        
        # Twitter returns data in JSON format - we need to decode it first
        record = json.loads(data)
        
        if not record.get('delete', None):

            record_scrubbed = Utils.scrub(record)
            Utils.insert_records(self.dataset_id, self.table_id, [record_scrubbed])
            
            if self.logger:
                self.logger.info('@%s: %s' % (record['user']['screen_name'], record['text'].encode('ascii', 'ignore')))
            
            self.count = self.count + 1
            
            return True
        
    #handle errors without closing stream:
    def on_error(self, status_code):

        if status_code == 420:
            
            self.backoff('Status 420')
            return True
        
        if self.logger:
            self.logger.info('Error with status code: %s' % status_code)

        return False 

    # got disconnect notice
    def on_disconnect(self, notice):
        
        self.backoff('Disconnect')
        return False

    def on_timeout(self):
        
        self.backoff('Timeout')
        return False 

    def on_exception(self, exception):
        
        if self.logger:
            self.logger.exception('Exception')

        return False 
    
    def backoff(self, msg):

        self.calm_count = self.calm_count + 1
        sleep_time = 60 * self.calm_count
        
        if sleep_time > 320:
            sleep_time = 320
        
        if self.logger:
            self.logger.info(msg + ", sleeping for %s" % sleep_time)

        time.sleep(60 * self.calm_count)

        return
    
    @staticmethod
    def start(schema, logger):
        
        listener = TwitterListener(config.DATASET_ID, config.TABLE_ID, logger=logger)
        auth = tweepy.OAuthHandler(config.CONSUMER_KEY, config.CONSUMER_SECRET)
        auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)
    
        while True:
    
            logger.info("Connecting to Twitter stream")
    
            stream = None
    
            try:
                
                stream = tweepy.Stream(auth, listener, headers = {"Accept-Encoding": "deflate, gzip"})
    
                # Choose stream: filtered or sample
                stream.sample()
                # stream.filter(track=TwitterListener.TRACK_ITEMS) 
                
            except:
    
                logger.exception("Unexpected error");
    
                if stream:
                    stream.disconnect()
    
                time.sleep(60)  

def main():
    
    if config.MODE not in ['gnip', 'twitter']:
        print "Invalid mode: %s" % config.MODE
        exit()

    print "Running in mode: %s" % config.MODE 

    logger = Utils.enable_logging()
    
    schema_file = "./schema.json"
    schema_str = Utils.read_file(schema_file)
    schema = json.loads(schema_str)
    
    try:
        Utils.insert_table(config.DATASET_ID, config.TABLE_ID, schema)
        print "Created table: %s.%s" % (config.DATASET_ID, config.TABLE_ID)
    except Exception, e:
        print "Table already exists: %s" % e

    if config.MODE == 'gnip':
        GnipListener.start(schema, logger)
    elif config.MODE == 'twitter':
        TwitterListener.start(schema, logger)

if __name__ == "__main__":
    main()
