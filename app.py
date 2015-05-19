import os, sys

BASE_DIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, "%s/libs" % BASE_DIR)

import jinja2
import webapp2

import re
import httplib2
import json
import time
import logging

from datetime import datetime, timedelta

from requests.exceptions import *
from google.appengine.ext.webapp import template
from google.appengine.api import memcache
from apiclient.discovery import build
from apiclient import errors
from apiclient.errors import HttpError
from oauth2client import appengine

from gnippy import rules
from gnippy.errors import RuleDeleteFailedException, RulesGetFailedException

from config import *
from utils import Utils

JINJA = jinja2.Environment(
    loader=jinja2.FileSystemLoader("%s/templates" % BASE_DIR),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)

_SCOPE = 'https://www.googleapis.com/auth/bigquery'

credentials = appengine.AppAssertionCredentials(scope=_SCOPE)
http = credentials.authorize(httplib2.Http())

ONE_DAY = 1000 * 60 * 60 * 24
REMOVE_HTML = re.compile(r'<.*?>')

class Chart(webapp2.RequestHandler):
    
    def get(self):
        
        template_data = {}
        self.response.out.write(JINJA.get_template('chart.html').render(template_data))

class ChartData(webapp2.RequestHandler):
    
    def get(self):
        
        table = self.request.get("table")
        field = self.request.get("field")
        charttype = self.request.get("charttype")
        interval = int(self.request.get("interval")) if self.request.get("interval") else 7

        builder = QueryBuilder(QueryBuilder.GNIP if "gnip" in table else QueryBuilder.PUBLIC, table, field, charttype, interval) 
        query = builder.query()
        print query
        
        results = get_service().jobs().query(projectId=PROJECT_NUMBER, body={'query':query}).execute()

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
                'query' : query.strip() if query else query,
                'title' : "Sources by type"
            }
            
        elif charttype == 'timeseries':

            # BUGBUG: this should return last 24 hours or last N days in order, not just random hours.
            
            # key: source, value: [source, d1, d2, d3...]
            now = datetime.now()
            buckets = {}
            header = None
            header_lookup = None

            if interval == 1:
                header = ['x', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23' ]
                header_lookup = header
            else : # interval == 31 or interval == 7:
                header = ['x']
                header_lookup = ['x']
                start = now - timedelta(days=interval)
                while True:
                    index = start.strftime("%Y-%m-%d")
                    
                    # header returned to c3 needs to be 0-padded dates
                    header.append(index)
                    
                    # matching index with bigquery results is not 0-padded dates
                    header_lookup.append(index.replace("-0", "-"))
                    start = start + timedelta(days=1)
                    if start > now:
                        break
            
            columns = [header]
            
            print columns
            
            if 'rows' in results:
                for row in results['rows']:
                    
                    for key, dict_list in row.iteritems():
                        value = dict_list[0]['v']
                        if value:
                            value = REMOVE_HTML.sub('', value)
                            count = int(dict_list[1]['v'])
                            time_interval = str(dict_list[2]['v'])

                            column = buckets.get(value, None)
                            if not column:
                                column = [0] * len(header)
                                column[0] = value
                                buckets[value] = column
    
                            column[header_lookup.index(time_interval)] = count
                            
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
                'axis': {
                    'x': {
                        'type': 'timeseries',
                        'tick': {
                            'format': '%Y-%m-%d'
                        }
                    }
                },
                'query' : query,
                'title' : "Sources by hour" 
            }

        self.response.headers['Content-Type'] = 'application/json'
        self.response.out.write(json.dumps(args))

class TableList(webapp2.RequestHandler):
    
    def get(self):
        
        template_data = {
             "id": "{{id}}", 
             "projectId": "{{projectId}}",
             "datasetId": "{{datasetId}}",
             "tableId": "{{tableId}}",
             "rulesStart" : "{{#rules}}",
             "rulesValue" : "{{.}}",
             "rulesEnd" : "{{/rules}}",
             "projectIdName" : PROJECT_ID
        }
        self.response.out.write(JINJA.get_template('table_list.html').render(template_data))
        
class ApiTableList(webapp2.RequestHandler):
    
    def get(self):

        response = None
        
        response = rules.get_rules(url=GNIP_URL, auth=(GNIP_USERNAME, GNIP_PASSWORD))
        
        print response

        tables = get_datasets()
        for t in tables:
            tag = "%s.%s" % (t['datasetId'], t['tableId'])
            rs = [r['value'] for r in response if r['tag'] == tag]
            t['rules'] = rs
        
        self.response.headers['Content-Type'] = 'application/json'   
        self.response.out.write(json.dumps(tables))   
        
class ApiTableAdd(webapp2.RequestHandler):
    
    def get(self):
        
        type = self.request.get("type")

        dataset = self.request.get("dataset")
        if "gnip" in dataset:
            dataset = "gnip"
        else:
            dataset = "twitter"
        
        table = self.request.get("name")
        rule_list = self.request.get("rules")
        imprt = self.request.get("import")

        schema_file = GNIP_SCHEMA_FILE if type == "gnip" else SCHEMA_FILE
        schema_str = Utils.read_file(schema_file)
        schema = json.loads(schema_str)
        
        body = {
            "tableReference" : {
                "projectId" : PROJECT_ID,
                "tableId" : table,
                "datasetId" : dataset
            },
            "schema" : {
                "fields" : schema
            }
        }

        response = get_service().tables().insert(projectId=PROJECT_ID, datasetId=dataset, body=body).execute()
            
        name = "%s.%s" % (dataset, table)
        rule_list = [s.strip() for s in rule_list.splitlines()]
        for r in rule_list:
            rules.add_rule(r, tag=name, url=GNIP_URL, auth=(GNIP_USERNAME, GNIP_PASSWORD))

        self.response.headers['Content-Type'] = 'application/json'   
        self.response.out.write(json.dumps(response)) 
        
class ApiTableDelete(webapp2.RequestHandler):
    
    def get(self):
        
        id = self.request.get("id")
        (project, dataset, table) = parse_bqid(id)
        
        try:
            response = get_service().tables().delete(projectId=project, datasetId=dataset, tableId=table).execute()
        except:
            # OK to ignore here if it's already deleted; continue onto deleting rules
            pass
        
        tag = dataset + "." + table

        rules_list = rules.get_rules(url=GNIP_URL, auth=(GNIP_USERNAME, GNIP_PASSWORD))
        rules_list = [r for r in rules_list if r['tag'] == tag]
        response = rules.delete_rules(rules_list, url=GNIP_URL, auth=(GNIP_USERNAME, GNIP_PASSWORD))

        self.response.headers['Content-Type'] = 'application/json'   
        self.response.out.write(json.dumps(response))
                       
class TableDetail(webapp2.RequestHandler):
    
    def get(self, id):
        
        (project, dataset, table) = parse_bqid(id)
        response = get_service().tables().get(projectId=project, datasetId=dataset, tableId=table).execute()
        self.response.out.write(JINJA.get_template('table_detail.html').render(response))

class RuleList(webapp2.RequestHandler):
    
    def get(self):
        
        template_data = {}
        self.response.out.write(JINJA.get_template('rule_list.html').render(template_data))
                
class ApiRuleList(webapp2.RequestHandler):
    
    def get(self):
        
        table = self.request.get("table", None)
        
        response = rules.get_rules(url=GNIP_URL, auth=(GNIP_USERNAME, GNIP_PASSWORD))
        
        if table:
            response = [r for r in response if r['tag'] in table]
        
        self.response.headers['Content-Type'] = 'application/json'   
        self.response.out.write(json.dumps(response))
        
class ApiRuleAdd(webapp2.RequestHandler):
    
    def get(self):
        
        rule = self.request.get("rule")
        tag = self.request.get("tag")

        if not rule or not tag:
            raise Exception("missing parameter")

        response = rules.add_rule(rule, tag=tag, url=GNIP_URL, auth=(GNIP_USERNAME, GNIP_PASSWORD))
        
        self.response.headers['Content-Type'] = 'application/json'   
        self.response.out.write(json.dumps(response))
        
class ApiRuleDelete(webapp2.RequestHandler):
    
    def get(self):
        
        value = self.request.get("value")
        rule_delete = {
            "value" : value
        }

        response = rules.delete_rule(rule_delete, url=GNIP_URL, auth=(GNIP_USERNAME, GNIP_PASSWORD))
        
        self.response.headers['Content-Type'] = 'application/json'   
        self.response.out.write(json.dumps(response))

class Admin(webapp2.RequestHandler):
    
    def get(self):
        template_data = {}
        self.response.out.write(JINJA.get_template('admin.html').render(template_data))
        
def get_datasets():
    
    tables = []
    
    response = get_service().datasets().list(projectId=PROJECT_ID).execute()
    datasets = response.get("datasets", None)
    
    for d in datasets:
        ref = d.get("datasetReference", None)
        response = get_service().tables().list(projectId=ref.get("projectId"), datasetId=ref.get("datasetId")).execute()
        for t in response.get("tables", None):
            id = t.get("id")
            ref = t.get("tableReference", None)
            tables.append({
                "id": id, 
                "projectId": ref.get("projectId", None), 
                "datasetId": ref.get("datasetId", None), 
                "tableId": ref.get("tableId", None) 
            })
            
    return tables

class QueryBuilder():
    
    PUBLIC = "public"
    GNIP = "gnip"
    
    type = None
    table = None
    field = None
    charttype = None
    interval = None
    FROM_CLAUSE = None

    def __init__(self, type, table, field, charttype, interval):
        self.type = type
        self.table = table
        self.field = field
        self.charttype = charttype
        self.interval = interval
        self.FROM_CLAUSE = "[%s]" % self.table
        print self.FROM_CLAUSE
        
    def query(self):
        
        # these query fields can changed based on instance vars
        object = None
        prefix = None
        col = None
        flatten_field = None
        created_field = None

        if self.type == QueryBuilder.PUBLIC:
            created_field = 'created_at'
            if self.field == 'hashtags':
                object = 'Hashtags'
                prefix = '#'
                col = 'entities.hashtags.text'
                flatten_field = 'entities.hashtags'
            elif self.field == 'mentions':
                object = 'User mentions'
                prefix = '@'
                col = 'entities.user_mentions.screen_name'
                flatten_field = 'entities.user_mentions'            
            elif self.field == 'sources':
                object = 'Tweet sources'
                prefix = '@'
                col = 'source' 
        else:
            created_field = 'postedTime'
            if self.field == 'hashtags':
                object = 'Hashtags'
                prefix = '#'
                col = 'twitter_entities.hashtags.text'
                flatten_field = 'twitter_entities.hashtags'
            elif self.field == 'mentions':
                object = 'User mentions'
                prefix = '@'
                col = 'twitter_entities.user_mentions.screen_name'
                flatten_field = 'twitter_entities.user_mentions'            
            elif self.field == 'sources':
                object = 'Tweet sources'
                prefix = '@'
                col = 'source'
                            
        dt = datetime.now()
        time_limit = time.mktime(dt.timetuple())
        if self.interval == 1:
            time_limit = time_limit - (ONE_DAY)
        elif self.interval == 31:
            time_limit = time_limit - (ONE_DAY * 31)
        else: # interval == 7
            time_limit = time_limit - (ONE_DAY * 7)
        
        select = "%s as value,count(*) as count" % col 
        fromclause = "flatten(%s, %s)" % (self.FROM_CLAUSE, flatten_field) if flatten_field else self.FROM_CLAUSE
        filter = "%s is not null AND %s > %s" % (col, created_field, time_limit)
        groupby = "value"
        orderby = "count DESC"
        limit = 20
        
        # requires join AND tranlating all tables to scoped t1/t2
        if self.charttype == "timeseries":
            select = "t1.%s, CONCAT('2015-', STRING(MONTH(TIMESTAMP(t1.%s))), '-', STRING(DAY(TIMESTAMP(t1.%s)))) AS create_hour" % (select, created_field, created_field)
            fromclause = "flatten(%s, %s)" % (self.FROM_CLAUSE, flatten_field) if flatten_field else self.FROM_CLAUSE
            fromclause = """
                %s t1
                inner join each 
                    (
                        SELECT 
                            %s, 
                            count(*) AS occur 
                        FROM %s
                        WHERE
                            %s is not null AND
                            %s > %s 
                        GROUP BY %s 
                        ORDER BY occur DESC 
                        LIMIT 20
                    ) t2 
                ON t1.%s = t2.%s
                """ % (fromclause, col, fromclause, col, created_field, time_limit, col, col, col)
            filter = "t1.%s is not null AND t1.%s > %s" % (col, created_field, time_limit)
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
            
        return query
    
def get_service():
    return build('bigquery', 'v2', http=http)

def parse_bqid(id):
    import re
    return re.split('\:|\.', id)
    
application = webapp2.WSGIApplication([
    
    # API calls supporting JSON
    ('/api/rule/list', ApiRuleList),
    ('/api/rule/add', ApiRuleAdd),
    ('/api/rule/delete', ApiRuleDelete),
    ('/api/table/list', ApiTableList),
    ('/api/table/add', ApiTableAdd),
    ('/api/table/delete', ApiTableDelete),
    
    # web pages
    ('/table/list', TableList),
    ('/table/([A-Za-z0-9\-\_\:\.]+)', TableDetail),
    ('/rule/list', RuleList),
    ('/chart/data', ChartData),
    ('/chart', Chart),
    ('/admin', Admin),

    ('/', Chart),
    
], debug=True)

def handle_500(request, response, exception):

    status = 500
    message = "Unknown error occurred"
    try:
        raise exception
    except RulesGetFailedException, e:
        status = 500
        message = e.message
    except RuleDeleteFailedException, e:
        status = 500
        message = e.message
    except ConnectionError, e:
        status = 500
        message = str(e)
    except HttpError, e:
        status = e.resp.status
        message = e._get_reason()
    except:
        logging.exception(exception)

    response.set_status(status)
    response.out.write(message)

application.error_handlers[500] = handle_500
