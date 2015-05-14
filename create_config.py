#!/usr/bin/python

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

main_config_file = open("./config.py", 'r')
# Twitter app keys and tokens
CONSUMER_KEY = ""
CONSUMER_SECRET = ""
ACCESS_TOKEN = ""
ACCESS_TOKEN_SECRET = ""
GNIP_URL = 'https://stream.gnip.com:443/accounts/piyush-kumar/publishers/twitter/streams/track/prod.json'
GNIP_USERNAME = 'ilyanshee@gmail.com'
GNIP_PASSWORD = 'R6FHNGgC2E'

GNIP_SCHEMA_FILE = "./schema_powertrack.json"
GNIP_SAMPLE_TWEET_FILE = "./sample_tweet_powertrack.json"

project_id = ""
for line in main_config_file:
	line_array = line.split("=")
	if line_array[0].strip() == "PROJECT_ID":
		project_id = line_array[1].strip().strip("\"")
	elif (line_array[0].strip()  == "CONSUMER_KEY"):
		CONSUMER_KEY = line_array[1].strip()
	elif (line_array[0].strip()  == "CONSUMER_SECRET"):
		CONSUMER_SECRET = line_array[1].strip()
	elif (line_array[0].strip()  == "ACCESS_TOKEN"):
		ACCESS_TOKEN = line_array[1].strip()
	elif (line_array[0].strip()  == "ACCESS_TOKEN_SECRET"):
		ACCESS_TOKEN_SECRET = line_array[1].strip()

container_template = open("./container.yaml", 'r')
container_gnip = open("./image_gnip/container.yaml", 'w')
container_twitter = open("./image_gnip/container.yaml", 'w')

#Create appropriate yaml files here
for line in container_template:
	line_array = line.split(":")
	print line_array
	if line_array[0].strip() == "image":
		cur_line = line_array[0]+': gcr.io/'+project_id+'/image_gnip'+'\n'
		container_gnip.write(cur_line)
	elif "name" in line_array[0].strip():
		cur_line_gnip =line_array[0]+": "+ "image_gnip\n"
		container_gnip.write(cur_line_gnip)
		cur_line_twitter = line_array[0]+": "+"image_twitter\n"
		container_twitter.write(cur_line_twitter)
	else:
		container_gnip.write(line)
		container_twitter.write(line)


#Create relevant Twitter/GNIP config files
main_config_template =  open("./config.py", 'r')
twitter_config_template =  open("./image_twitter/config.py", 'w')
gnip_config_template =  open("./image_gnip/config.py", 'w')

for line in main_config_template:
	if "GNIP" in line:
		gnip_config_template.write(line)
	elif "TWITTER" in line:
		twitter_config_template.write(line)
	else:
		twitter_config_template.write(line)
		gnip_config_template.write(line)
