import os
import re
import httplib2
import json
import webapp2
import logging

from google.appengine.ext.webapp import template
from google.appengine.api import memcache

from apiclient.discovery import build
from apiclient import errors

from oauth2client.appengine import oauth2decorator_from_clientsecrets

from config import *

SCOPES = [
    'https://www.googleapis.com/auth/bigquery'
]

decorator = oauth2decorator_from_clientsecrets(
    filename=CLIENT_SECRETS,
    scope=SCOPES,
    cache=memcache)

REMOVE_HTML = re.compile(r'<.*?>')

def get_service():
    
    return build('bigquery', 'v2', http=decorator.http())

class ShowChartPage(webapp2.RequestHandler):
    
    @decorator.oauth_required
    def get(self):
    	temp_data = {}
    	temp_path = 'templates/chart.html'
    	queryData = {'query':'SELECT source, count(*) as count FROM [tweets.2015_01_09] GROUP by source ORDER BY count DESC LIMIT 1000'}
    	tableData = get_service().jobs()
    	response = tableData.query(projectId=PROJECT_NUMBER, body=queryData).execute()
    	self.response.out.write(response)
    	# self.response.out.write(template.render(temp_path,temp_data))
	
class ShowHome(webapp2.RequestHandler):
    
    @decorator.oauth_required
    def get(self):
        template_data = {}
        template_path = 'templates/index.html'
        self.response.out.write(template.render(template_path, template_data))

#         queryData = {'query':'SELECT SUM(word_count) as WCount,corpus_date,group_concat(corpus) as Work FROM '
#                      '[publicdata:samples.shakespeare] WHERE word="' + inputData + '" and corpus_date>0 GROUP BY corpus_date ORDER BY WCount'}

class Data(webapp2.RequestHandler):
    
    @decorator.oauth_required
    def get(self):
        
        source = self.request.get("source")
        pivot = self.request.get("pivot")
        charttype = self.request.get("charttype")
        hashtags = self.request.get("hashtags")

        title = None
        query = None
        args = {}
        
        if source == 'sources':

            if pivot == 'hour' or charttype == 'timeseries':

                query = {'query': 'SELECT source as source, HOUR(TIMESTAMP(created_at)) AS create_hour, count(*) as count FROM [tweets.2015_01_09] WHERE source contains \'Twitter for\' GROUP by create_hour, source ORDER BY source ASC, create_hour ASC'}
                
                tableData = get_service().jobs()
                dataList = tableData.query(projectId=PROJECT_NUMBER, body=query).execute()
                
                # key: source, value: [source, d1, d2, d3...]
                buckets = {}
                columns = [['x', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23' ]]
                if 'rows' in dataList:
                    for row in dataList['rows']:
                        for key, dict_list in row.iteritems():
                            source = REMOVE_HTML.sub('', dict_list[0]['v'])
                            hour = int(dict_list[1]['v'])
                            count = int(dict_list[2]['v'])
                            
                            column = buckets.get(source, None)
                            if not column:
                                column = [0] * 25
                                column[0] = source
                                buckets[source] = column

                            column[hour + 1] = count
                else:
                    columns.append([])
                    
                for key, value in buckets.iteritems():
                    columns.append(value)
                    
                # FORMAT: timeseries
                args = {
                    'data' : {
                        'x' : 'x',
                        'columns' : columns 
                    },
                }

            elif charttype == 'donut' or charttype == 'bar':
                
                query = {'query': 'SELECT source as source, count(*) as count FROM [tweets.2015_01_09] GROUP by source ORDER BY count DESC LIMIT 20'}
        
                tableData = get_service().jobs()
                dataList = tableData.query(projectId=PROJECT_NUMBER, body=query).execute()
         
                columns = []
                if 'rows' in dataList:
                    for row in dataList['rows']:
                        for key, dict_list in row.iteritems():
                            source = REMOVE_HTML.sub('', dict_list[0]['v'])
                            count = int(dict_list[1]['v'])
                            columns.append([source, count])
                else:
                    columns.append([])

                # FORMAT: donut
                args = {
                    'data' : {
                        'columns' : columns,
                        'type' : charttype
                    },
                    'donut' : {
                        'title' : "Tweet sources"
                    },
                    'bar': {
                        'width': {
                            'ratio': 0.5 
                        }
                    }

                }
                
            elif pivot == 'location' or charttype == 'map':

                pass
            
        elif source == 'hashtags':

            hashtags = hashtags.split(',')
            for idx, val in enumerate(hashtags):
                h = "'" + val + "'"
                hashtags[idx] = h

            if pivot == 'hour' or charttype == 'timeseries':

                query = {'query': 'SELECT source as source, HOUR(TIMESTAMP(created_at)) AS create_hour, count(*) as count FROM [tweets.2015_01_09] WHERE source contains \'Twitter for\' GROUP by create_hour, source ORDER BY source ASC, create_hour ASC'}
                
                tableData = get_service().jobs()
                dataList = tableData.query(projectId=PROJECT_NUMBER, body=query).execute()
                
                # key: source, value: [source, d1, d2, d3...]
                buckets = {}
                columns = [['x', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23' ]]
                if 'rows' in dataList:
                    for row in dataList['rows']:
                        for key, dict_list in row.iteritems():
                            source = REMOVE_HTML.sub('', dict_list[0]['v'])
                            hour = int(dict_list[1]['v'])
                            count = int(dict_list[2]['v'])
                            
                            column = buckets.get(source, None)
                            if not column:
                                column = [0] * 25
                                column[0] = source
                                buckets[source] = column

                            column[hour + 1] = count
                else:
                    columns.append([])
                    
                for key, value in buckets.iteritems():
                    columns.append(value)
                    
                # FORMAT: timeseries
                args = {
                    'data' : {
                        'x' : 'x',
                        'columns' : columns 
                    },
                }

            elif charttype == 'donut' or charttype == 'bar':

                terms = ','.join(hashtags) 
                query = {'query': "SELECT entities.hashtags.text, count(*) as count FROM [tweets.2015_01_09] WHERE LOWER(entities.hashtags.text) in (%s) GROUP by entities.hashtags.text ORDER BY count" % (terms)}
                print query 
        
                tableData = get_service().jobs()
                dataList = tableData.query(projectId=PROJECT_NUMBER, body=query).execute()
         
                columns = []
                if 'rows' in dataList:
                    for row in dataList['rows']:
                        for key, dict_list in row.iteritems():
                            source = REMOVE_HTML.sub('', dict_list[0]['v'])
                            count = int(dict_list[1]['v'])
                            columns.append([source, count])
                else:
                    columns.append([])

                # FORMAT: donut
                args = {
                    'data' : {
                        'columns' : columns,
                        'type' : charttype
                    },
                    'donut' : {
                        'title' : "Hashtags"
                    },
                    'bar': {
                        'width': {
                            'ratio': 0.5 
                        }
                    }

                }
                
            elif pivot == 'location' or charttype == 'map':

                pass

        self.response.headers['Content-Type'] = 'application/json'
        self.response.out.write(json.dumps(args))

class Chart(webapp2.RequestHandler):
    
    @decorator.oauth_required
    def get(self):
        template_data = {}
        template_path = 'templates/chart.html'
        self.response.out.write(template.render(template_path, template_data))

application = webapp2.WSGIApplication([
    ('/chart', Chart),
    ('/data', Data),
    ('/', ShowHome),
    (decorator.callback_path, decorator.callback_handler())
], debug=True)
