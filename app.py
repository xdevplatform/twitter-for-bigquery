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

class Data(webapp2.RequestHandler):
    
    @decorator.oauth_required
    def get(self):
        
        source = self.request.get("source")
        pivot = self.request.get("pivot")
        charttype = self.request.get("charttype")
        
#         queryData = {'query':'SELECT SUM(word_count) as WCount,corpus_date,group_concat(corpus) as Work FROM '
#                      '[publicdata:samples.shakespeare] WHERE word="' + inputData + '" and corpus_date>0 GROUP BY corpus_date ORDER BY WCount'}

#         queryData = {'query': 'SELECT source as source, count(*) as count FROM [tweets.2015_01_09] GROUP by source ORDER BY count DESC LIMIT 20'}
#         tableData = get_service().jobs()
#         dataList = tableData.query(projectId=PROJECT_NUMBER, body=queryData).execute()
# 
#         p = re.compile(r'<.*?>')
#     
#         resp = []
#         if 'rows' in dataList:
#             for row in dataList['rows']:
#                 for key, dict_list in row.iteritems():
#                     source = p.sub('', dict_list[0]['v'])
#                     count = int(dict_list[1]['v'])
#                     resp.append([source, count])
#         else:
#             resp.append([])

        # FORMAT: donut
#         args = {
#             'data' : {
#                 'columns' : [ [ 'data1', 30 ], [ 'data2', 120 ], ],
#                 'type' : 'donut'
#             },
#             'donut' : {
#                 'title' : "Iris Petal Width"
#             }
#         }

        # FORMAT: line
#         args = {
#             'data': {
#                 'columns': [
#                     ['data1', 30, 200, 100, 400, 150, 250],
#                     ['data2', 50, 20, 10, 40, 15, 25]
#                 ]
#             }
#         }

        # FORMAT: timeseries
        args = {
            'data' : {
                'x' : 'x',
                # xFormat: '%Y%m%d', // 'xFormat' can be used as custom format
                # of 'x'
                'columns' : [
                        [ 'x', '2013-01-01', '2013-01-02', '2013-01-03',
                                '2013-01-04', '2013-01-05', '2013-01-06' ],
                        # ['x', '20130101', '20130102', '20130103', '20130104',
                        # '20130105', '20130106'],
                        [ 'data1', 30, 200, 100, 400, 150, 250 ],
                        [ 'data2', 130, 340, 200, 500, 250, 350 ] ]
            },
            'axis' : {
                'x' : {
                    'type' : 'timeseries',
                    'tick' : {
                        'format' : '%Y-%m-%d'
                    }
                }
            }
        }
    
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
