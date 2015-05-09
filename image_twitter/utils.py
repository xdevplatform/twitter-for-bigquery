import os
import sys
import time
import logging.config
import json

class Utils:

    @staticmethod
    def insert_record(client, dataset_id, table_id, record):
         
        result = client.push_rows(dataset_id, table_id, [record], None) 
         
        if result.get('insertErrors', None):
    
            print "Record: %s" % (json.dumps(record))
            print "Error result: %s" % result
            
            return False
        
        return True

    @staticmethod    
    def import_from_file(client, dataset_id, table_id, filename, single_tweet=False):
        
        if single_tweet:
            
            record = json.loads(Utils.read_file(SAMPLE_TWEET_FILE))
            success = Utils.insert_record(client, dataset_id, table_id, record)
            return success

        row = 0
        with open(filename, "r") as f:
             
            for tweet in f:
                 
                record = json.loads(tweet)
     
                # ignore delete records for now            
                if record.get("delete", None):
                    continue
                 
                record_scrubbed = Utils.scrub(record)
                success = Utils.insert_record(client, dataset_id, table_id, record_scrubbed)
                if not success:
                    print "Failed row: %s %s" % (row, json.dumps(record))
                    return
                else:
                    print "Processed row: %s" % row
                 
                row = row + 1

    @staticmethod
    def scrub(d):
    
        # d.iteritems isn't used as you can't del or the iterator breaks.
        for key, value in d.items():
            
            if value is None:
                del d[key]
            elif key == 'coordinates':
                del d[key]
            elif key == 'attributes': # in 'place' object 
                del d[key]
            elif key == 'bounding_box': # in 'place' object
                del d[key]
            elif key == 'retweeted_status':
                del d[key]
            elif key == 'created_at':
                d[key] = Utils.convert_timestamp(value)
            elif isinstance(value, dict):
                Utils.scrub(value)
        return d  # For convenience
    
    @staticmethod
    def convert_timestamp(str):
        
        ts = time.strptime(str,'%a %b %d %H:%M:%S +0000 %Y')
        ts = time.strftime('%Y-%m-%d %H:%M:%S', ts)
        
        return ts
    
    @staticmethod
    def read_file(fn):
        
        data = ""
        with open(fn, "r") as f:
            for line in f:
                data = data + line
                
        return data    
    
    @staticmethod 
    def generate_schema_from_tweet():
        
        record_str = Utils.read_file(SAMPLE_TWEET_FILE)
        record = json.loads(record_str)
        schema_str = schema_from_record(record)
        
        return schema_str
    
    @staticmethod
    def enable_logging(logging_config):
    
        logging.config.fileConfig(logging_config)
        root = logging.getLogger("root")
    
        return root

