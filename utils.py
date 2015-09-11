import os, sys

BASE_DIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, "%s/libs" % BASE_DIR)

import time
import json
from datetime import datetime, timedelta

import httplib2
import logging.config
from config import Config

from apiclient.discovery import build
from apiclient.errors import HttpError

from gnippy import rules, searchclient

f = file("./config")
config = Config(f)

class Utils:
    
    BQ_CLIENT = None
    
    @staticmethod
    def get_bq():
        
        if Utils.BQ_CLIENT:
            
            return Utils.BQ_CLIENT
        
        BQ_CREDENTIALS = None
        
        # If runing on Google stack, authenticate natively
        if Utils.isGae():
            
            from oauth2client import appengine
            BQ_CREDENTIALS = appengine.AppAssertionCredentials(scope='https://www.googleapis.com/auth/bigquery')

        else:
            
            from oauth2client.client import SignedJwtAssertionCredentials
            KEY = Utils.read_file(config.KEY_FILE)
            BQ_CREDENTIALS = SignedJwtAssertionCredentials(config.SERVICE_ACCOUNT, KEY, 'https://www.googleapis.com/auth/bigquery')
    
        BQ_HTTP = BQ_CREDENTIALS.authorize(httplib2.Http())
        Utils.BQ_CLIENT = build('bigquery', 'v2', http=BQ_HTTP)
        
        return  Utils.BQ_CLIENT
    
    @staticmethod
    def isGae():
        
        # http://stackoverflow.com/questions/1916579/in-python-how-can-i-test-if-im-in-google-app-engine-sdk
        software = os.environ.get('SERVER_SOFTWARE', None) 
        return software and ("Google App Engine" in software or "Development" in software)
    
    @staticmethod
    def get_gnip():
        g = searchclient.SearchClient(config.GNIP_USERNAME, config.GNIP_PASSWORD, config.GNIP_SEARCH_URL)
        return g
    
    @staticmethod
    def insert_table(dataset_id, table_id):
        
        schema_file = None

        if "gnip" in dataset_id:
            dataset_id = "gnip"
            schema_file = "./schema/schema_gnip.json"
        else:
            dataset_id = "twitter"
            schema_file = "./schema/schema_twitter.json"
        
        schema_str = Utils.read_file(schema_file)
        schema = json.loads(schema_str)
        
        body = {
            "tableReference" : {
                "projectId" : config.PROJECT_ID,
                "tableId" : table_id,
                "datasetId" : dataset_id
            },
            "schema" : {
                "fields" : schema
            }
        }

        response = None
        try:
            response = Utils.get_bq().tables().insert(projectId=config.PROJECT_ID, datasetId=dataset_id, body=body).execute()
        except HttpError, e:
            # HttpError 409 when requesting URI returned 
            # "Already Exists: Table twitter-for-bigquery:gnip.tweets_nbafinals"
            if e.resp.status == 409:
                response = True
            else:
                raise e

        return response
    
    @staticmethod
    def insert_records(dataset_id, table_id, tweets):
         
        # ensure insertId to avoid duplicate records
        body = {
            "kind": "bigquery#tableDataInsertAllRequest",
            "rows": [{ "insertId" : t["id"], "json" : Utils.scrub(t) } for t in tweets ]
        }

        response = Utils.get_bq().tabledata().insertAll(projectId=config.PROJECT_ID, datasetId=dataset_id, tableId=table_id, body=body).execute()
        return response
         
    @staticmethod    
    def import_from_file(dataset_id, table_id, filename, single_tweet=False):
        
        records = []
        if single_tweet:
            
            records = [json.loads(Utils.read_file(SAMPLE_TWEET_FILE))]
            success = Utils.insert_records(dataset_id, table_id, records)
            return success

        else:
            
            with open(filename, "r") as f:
    
                records = [Utils.scrub(json.loads(tweet)) for tweet in f if json.loads(tweet).get("delete", None) == None]             
                success = Utils.insert_records(dataset_id, table_id, [record])
                
    @staticmethod
    def get_config(config_file):
        props = {}
        for name in (name for name in dir(config) if not name.startswith('_')):
            props[name] = getattr(config, name, '')
        return props

    @staticmethod
    def enable_logging():
    
        root = None
    
        if Utils.isGae():
            logging.getLogger().setLevel(logging.DEBUG)
        else:
            path = "./logging.conf"
            logging.config.fileConfig(path)
            root = logging.getLogger("root")
    
        return root

    @staticmethod
    # BUGBUG: aim to NOT scrub results
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
    def read_file(fn):
        
        data = ""
        with open(fn, "r") as f:
            for line in f:
                data = data + line
                
        return data    
    
    @staticmethod
    def convert_timestamp(str):
        
        ts = time.strptime(str,'%a %b %d %H:%M:%S +0000 %Y')
        ts = time.strftime('%Y-%m-%d %H:%M:%S', ts)
        
        return ts

    @staticmethod
    def millis_to_date(ts):
        return datetime.fromtimestamp(ts/1000)
    
    @staticmethod
    def millis_to_str(ts, format='%Y-%m-%d %H:%M'):
        return Utils.millis_to_date(ts).strftime(format)
        
    @staticmethod
    def parse_bqid(id):
        if id:
            import re
            return re.split('\:|\.', id)
        return None

    @staticmethod    
    def make_tag(dataset, table):
        return "%s.%s" % (dataset, table)
    
# main() generates a schema from a tweet. It requires the following 
# library to work, and is not included in this TwitterDev package
# https://github.com/tylertreat/BigQuery-Python
# def main():
# 
#     from bigquery import schema_from_record
#     
#     tweet_str = Utils.read_file("data/sample_tweet_powertrack.json")
#     record = json.loads(record_str)
#     schema = schema_from_record(record)
#     schema = json.dumps(schema)
#     print schema
#     
#     with open('data/schema.json', 'wt') as out:
#         res = json.dump(schema, out, sort_keys=False, indent=4, separators=(',', ': '))
#     
# if __name__ == "__main__":
#     main()    
    