import sys
import time
import logging
import json
import tweepy

# Write records to BigQuery
class BigQueryListener(tweepy.StreamListener):
    
    def __init__(self, client, dataset_id, table_id):

      self.client = client
      self.dataset_id = dataset_id
      self.table_id = table_id
      
    def on_data(self, data):
        
        # Twitter returns data in JSON format - we need to decode it first
        record = json.loads(data)
        
        if not record.get('delete', None):

            record_scrubbed = Utils.scrub(record)
            Utils.insert_record(self.client, self.dataset_id, self.table_id, record_scrubbed)
            
            print '@%s: %s' % (record['user']['screen_name'], record['text'].encode('ascii', 'ignore'))
            
            return True

    def on_error(self, status):
        print status
        
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
        
        record_str = read_file(SAMPLE_TWEET_FILE)
        record = json.loads(record_str)
        schema_str = schema_from_record(record)
        
        return schema_str
    
    @staticmethod
    def enable_logging():
    
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)
         
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        root.addHandler(ch)

