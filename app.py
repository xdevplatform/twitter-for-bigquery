import sys
sys.path.insert(0, 'libs')

import os
import re
import httplib2
import json
import webapp2

from datetime import datetime
import time

from google.appengine.ext.webapp import template
from google.appengine.api import memcache

from apiclient.discovery import build
from apiclient import errors

from oauth2client import appengine

from gnippy import rules
from gnippy.errors import RulesGetFailedException

from config import *

_SCOPE = 'https://www.googleapis.com/auth/bigquery'

credentials = appengine.AppAssertionCredentials(scope=_SCOPE)
http = credentials.authorize(httplib2.Http())

# [tweets:2015_01_09]
FROM_CLAUSE = "[%s.%s]" % (DATASET_ID, TABLE_ID) 

ONE_DAY = 1000 * 60 * 60 * 24
REMOVE_HTML = re.compile(r'<.*?>')

def get_service():
    
    return build('bigquery', 'v2', http=http)

class Data(webapp2.RequestHandler):
    
    def get(self):
        
        source = self.request.get("source")
        charttype = self.request.get("charttype")
        interval = self.request.get("interval")
        terms = self.request.get("terms")

        object = None
        prefix = None
        col = None
        flattenby = None

        if source == 'hashtags':
            object = 'Hashtags'
            prefix = '#'
            col = 'entities.hashtags.text'
            flattenby = 'entities.hashtags'
        elif source == 'mentions':
            object = 'User mentions'
            prefix = '@'
            col = 'entities.user_mentions.screen_name'
            flattenby = 'entities.user_mentions'            
        elif source == 'sources':
            object = 'Tweet sources'
            prefix = '@'
            col = 'source'           
            
        if terms:
            terms = terms.lower().split(',')
            for idx, val in enumerate(terms):
                h = "'" + val + "'"
                terms[idx] = h
            terms = ','.join(terms) 
        else:
            terms = "java,python,ruby,javascript,haskell,swift"

        dt = datetime.now()
        time_limit = time.mktime(dt.timetuple())
        if interval == 1:
            time_limit = time_limit - (ONE_DAY)
        elif interval == 31:
            time_limit = time_limit - (ONE_DAY * 31)
        else: # interval == 7
            time_limit = time_limit - (ONE_DAY * 7)
        filter_time = "created_at > %s" % time_limit
        
        select = "%s as value,count(*) as count" % col 
        fromclause = "flatten(%s, %s)" % (FROM_CLAUSE, flattenby) if flattenby else FROM_CLAUSE
        filter = "%s is not null AND created_at > 826691172.0" % col
        groupby = "value"
        orderby = "count DESC"
        limit = 20
        
        if charttype == "timeseries":
            select = select + ",HOUR(TIMESTAMP(created_at)) AS create_hour"
            # for timeseries, only show top N occurring by subselect
            filter = filter + """
                %s in (
                    select top from (
                        SELECT 
                            %s as top, 
                            count(*) AS occur 
                        FROM %s 
                        WHERE
                            %s is not null AND
                            %s 
                        GROUP BY top 
                        ORDER BY occur DESC 
                        LIMIT %s
                    )
                ) 
                AND """ % (col, col, FROM_CLAUSE, col, filter_time, limit)
            groupby = "value, create_hour" 
            orderby_extra = "value ASC, create_hour ASC" 
            limit = 24 * limit

        query = None
        args = {}
        
        query = """
            SELECT 
                %s 
            FROM 
                %s 
            WHERE 
                %s 
            GROUP BY
                %s
            ORDER BY 
                %s 
            LIMIT %s""" % (select, fromclause, filter, groupby, orderby, limit)
            
        print query
        
        tableData = get_service().jobs()
        results = tableData.query(projectId=PROJECT_NUMBER, body={'query':query}).execute()

        if charttype == 'donut' or charttype == 'bar':
                 
            columns = []
            if 'rows' in results:
                for row in results['rows']:
                    for key, dict_list in row.iteritems():
                        value = dict_list[0]['v']
                        if value:
                            source = REMOVE_HTML.sub('', value)
                            count = int(dict_list[1]['v'])
                            columns.append([source, count])
            else:
                columns.append([])

            # http://c3js.org/samples/chart_donut.html
            # query and title are returned for display in UI
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
                },
                'query' : query,
                'title' : "Sources by type"
            }
            
        elif charttype == 'timeseries':

            # BUGBUG: this should return last 24 hours or last N days in order, not just random hours.
            
            # key: source, value: [source, d1, d2, d3...]
            buckets = {}
            columns = [['x', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23' ]]
            
            if 'rows' in results:
                for row in results['rows']:
                    
                    for key, dict_list in row.iteritems():
                        value = dict_list[0]['v']
                        if value:
                            value = REMOVE_HTML.sub('', value)
                            count = int(dict_list[1]['v'])
                            hour = int(dict_list[2]['v'])

                            column = buckets.get(value, None)
                            if not column:
                                column = [0] * 25
                                column[0] = value
                                buckets[value] = column
    
                            column[hour + 1] = count
            else:
                columns.append([])

            for key, value in buckets.iteritems():
                columns.append(value)

            # http://c3js.org/samples/simple_multiple.html
            # query and title are returned for display in UI
            args = {
                'data' : {
                    'x' : 'x',
                    'columns' : columns 
                },
                'query' : query,
                'title' : "Sources by hour" 
            }

        self.response.headers['Content-Type'] = 'application/json'
        self.response.out.write(json.dumps(args))

class Chart(webapp2.RequestHandler):
    
    def get(self):
        template_data = {}
        template_path = 'templates/chart.html'
        self.response.out.write(template.render(template_path, template_data))

class Rules(webapp2.RequestHandler):
    
    def get(self):
        
        template_data = {"tag": "{{tag}}", "value": "{{value}}", "count": "{{count}}"}
        template_path = 'templates/rules.html'
        self.response.out.write(template.render(template_path, template_data))
        
class RulesList(webapp2.RequestHandler):
    
    def get(self):
        
        response = None
        
        try:
            response = rules.get_rules(url=GNIP_URL, auth=(GNIP_USERNAME, GNIP_PASSWORD))
        except RulesGetFailedException:
            pass # uh oh
        
        self.response.headers['Content-Type'] = 'application/json'   
        self.response.out.write(json.dumps(response))
        
class RulesAdd(webapp2.RequestHandler):
    
    def get(self):
        
        rule = self.request.get("rule")
        tag = self.request.get("tag")

        if not rule or not tag:
            raise Exception("missing parameter")

        response = None
        
        try:
            rules.add_rule(rule, tag=tag, url=GNIP_URL, auth=(GNIP_USERNAME, GNIP_PASSWORD))
        except RulesGetFailedException:
            pass # uh oh
        
        self.response.headers['Content-Type'] = 'application/json'   
        self.response.out.write(json.dumps(response))
        
class RulesDelete(webapp2.RequestHandler):
    
    def get(self):
        
        rule_index = int(self.request.get("index"))
        
        response = None
        
        try:
            rules_list = rules.get_rules(url=GNIP_URL, auth=(GNIP_USERNAME, GNIP_PASSWORD))
            rule_delete = rules_list[rule_index]
            
            print rules_list
            print rule_delete
            
            r = rule_delete
            
            if not isinstance(r, dict):
                print "1"
    
            if "value" not in r:
                print "2"
    
            if not isinstance(r['value'], basestring):
                print "3"
    
            if "tag" in r and not isinstance(r['tag'], basestring):
                print "4"
    
#             for k in r:
#                 if k not in expected:
#                     fail()
            
            response = rules.delete_rule(rule_delete, url=GNIP_URL, auth=(GNIP_USERNAME, GNIP_PASSWORD))
        except RulesGetFailedException:
            pass # uh oh
        
        self.response.headers['Content-Type'] = 'application/json'   
        self.response.out.write(json.dumps(response))

class Admin(webapp2.RequestHandler):
    
    def get(self):
        template_data = {}
        template_path = 'templates/admin.html'
        self.response.out.write(template.render(template_path, template_data))

application = webapp2.WSGIApplication([
    
    ('/rules/list', RulesList),
    ('/rules/add', RulesAdd),
    ('/rules/delete', RulesDelete),
    
    ('/data', Data),

    ('/rules', Rules),
    ('/admin', Admin),
    ('/chart', Chart),
    ('/', Chart),
    
], debug=True)
