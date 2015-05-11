import os
import sys
import time

import json

from config import *
from utils import Utils

def main():
    
    tweet_str = Utils.read_file("data/sample_tweet_powertrack.json")
#     print "generating schema for %s" % tweet_str
    
    schema = Utils.generate_schema_from_tweet(tweet_str)
    schema = json.dumps(schema)
#     schema.replace("'", '"')
    print schema
    
#     with open('data/schema.json', 'wt') as out:
#         res = json.dump(schema, out, sort_keys=False, indent=4, separators=(',', ': '))
    
main()    
    
    