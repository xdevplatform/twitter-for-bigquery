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
from google.appengine.api import taskqueue
from google.appengine.api.taskqueue import TombstonedTaskError

from apiclient.discovery import build
from apiclient import errors
from apiclient.errors import HttpError

from oauth2client import appengine

from gnippy import rules, searchclient
from gnippy.errors import RuleDeleteFailedException, RulesGetFailedException

from utils import Utils
from config import *

JINJA = jinja2.Environment(
    loader=jinja2.FileSystemLoader("%s/templates" % BASE_DIR),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)

REMOVE_HTML = re.compile(r'<.*?>')
TABLE_CACHE = {}

class Home(webapp2.RequestHandler):
    
    def get(self):
        
        template_data = {}
        self.response.out.write(JINJA.get_template('home.html').render(template_data))

class TableList(webapp2.RequestHandler):
    
    def get(self):
        
        template_data = {"projectId" : PROJECT_ID}
        self.response.out.write(JINJA.get_template('table_list.html').render(template_data))
        
class ApiTableList(webapp2.RequestHandler):
    
    def get(self):

        tables = []
    
        if TABLE_CACHE.get("cache", None) == None:
    
            datasets = get_bq().datasets().list(projectId=PROJECT_ID).execute()
            datasets = datasets.get("datasets", None)
            
            for d in datasets:
                ref = d.get("datasetReference", None)
                bq_tables = get_bq().tables().list(projectId=ref.get("projectId"), datasetId=ref.get("datasetId")).execute()
                for t in bq_tables.get("tables", None):
                    id = t.get("id")
                    ref = t.get("tableReference", None)
                    tables.append({
                        "id": id, 
                        "projectId": ref.get("projectId", None), 
                        "datasetId": ref.get("datasetId", None), 
                        "tableId": ref.get("tableId", None) 
                    })
                    
            rules_list = rules.get_rules(url=GNIP_STREAM_URL, auth=(GNIP_USERNAME, GNIP_PASSWORD))
            for t in tables:
                tag = make_tag(t['datasetId'], t['tableId'])
                rs = [r['value'] for r in rules_list if r['tag'] == tag]
                t['rules'] = rs
                
            TABLE_CACHE["cache"] = tables
            
        else:
            
            tables = TABLE_CACHE.get("cache")
                
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

        schema_file = "./schema.json"
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

        response = get_bq().tables().insert(projectId=PROJECT_ID, datasetId=dataset, body=body).execute()
        TABLE_CACHE.clear()
            
        name = make_tag(dataset, table)
        rule_list = [s.strip() for s in rule_list.splitlines()]
        for r in rule_list:
            rules.add_rule(r, tag=name, url=GNIP_STREAM_URL, auth=(GNIP_USERNAME, GNIP_PASSWORD))
            TABLE_CACHE.clear()

        self.response.headers['Content-Type'] = 'application/json'   
        self.response.out.write(json.dumps(response)) 
        
class ApiTableDelete(webapp2.RequestHandler):
    
    def get(self, id):
        
        (project, dataset, table) = parse_bqid(id)
        
        try:
            response = get_bq().tables().delete(projectId=project, datasetId=dataset, tableId=table).execute()
            TABLE_CACHE.clear()
        except:
            # OK to ignore here if it's already deleted; continue onto deleting rules
            pass
        
        tag = dataset + "." + table

        rules_list = rules.get_rules(url=GNIP_STREAM_URL, auth=(GNIP_USERNAME, GNIP_PASSWORD))
        rules_list = [r for r in rules_list if r['tag'] == tag]
        response = rules.delete_rules(rules_list, url=GNIP_STREAM_URL, auth=(GNIP_USERNAME, GNIP_PASSWORD))
        TABLE_CACHE.clear()

        self.response.headers['Content-Type'] = 'application/json'   
        self.response.out.write(json.dumps(response))
                   
class ApiTableData(webapp2.RequestHandler):
    
    def get(self, table):
        
        field = self.request.get("field")
        charttype = self.request.get("charttype")
        interval = int(self.request.get("interval")) if self.request.get("interval") else 7

        builder = QueryBuilder(QueryBuilder.GNIP if "gnip" in table else QueryBuilder.PUBLIC, table, field, charttype, interval) 
        query = builder.query()
        
        results = get_bq().jobs().query(projectId=PROJECT_NUMBER, body={'query':query}).execute()
        args = builder.c3_args(query, results) 

#         print query
#         print args
        
        self.response.headers['Content-Type'] = 'application/json'
        self.response.out.write(json.dumps(args))
                               
class TableDetail(webapp2.RequestHandler):
    
    def get(self, id):
        
        (project, dataset, table) = parse_bqid(id)
        response = get_bq().tables().get(projectId=project, datasetId=dataset, tableId=table).execute()
        
        created = float(response['creationTime'])
        response['creationTime'] = millis_to_date(created)
        
        updated = float(response['lastModifiedTime'])
        response['lastModifiedTime'] = millis_to_date(updated)
        
        self.response.out.write(JINJA.get_template('table_detail.html').render(response))

class RuleList(webapp2.RequestHandler):
    
    def get(self):
        
        template_data = {}
        self.response.out.write(JINJA.get_template('rule_list.html').render(template_data))
                
class ApiRuleList(webapp2.RequestHandler):
    
    def get(self):
        
        table = self.request.get("table", None)
        
        response = rules.get_rules(url=GNIP_STREAM_URL, auth=(GNIP_USERNAME, GNIP_PASSWORD))
        
        if table:
            (project, dataset, table) = parse_bqid(table)
            tag = make_tag(dataset, table)
            response = [r for r in response if r['tag'] == tag]
            
        self.response.headers['Content-Type'] = 'application/json'   
        self.response.out.write(json.dumps(response))
        
class ApiRuleAdd(webapp2.RequestHandler):
    
    def get(self):
        
        rule = self.request.get("rule")
        tag = self.request.get("tag")

        if not rule or not tag:
            raise Exception("missing parameter")

        response = rules.add_rule(rule, tag=tag, url=GNIP_STREAM_URL, auth=(GNIP_USERNAME, GNIP_PASSWORD))
        TABLE_CACHE.clear()
        
        self.response.headers['Content-Type'] = 'application/json'   
        self.response.out.write(json.dumps(response))
        
class ApiRuleTest(webapp2.RequestHandler):
    
    def get(self):

        rule = self.request.get("rule")

        if not rule:
            raise Exception("missing parameter")

        g = get_gnip()
        end = datetime.now()
        start = end - timedelta(days=7)
        timeline = g.query(rule, 0, record_callback=None, use_case="timeline", start=start, end=end, count_bucket="day")
        timeline = json.loads(timeline)
        
        count = 0 
        for r in timeline["results"]:
            count = count + r["count"]
        timeline['count'] = count

        self.response.headers['Content-Type'] = 'application/json'   
        self.response.out.write(json.dumps(timeline))
        
class ApiRuleBackfill(webapp2.RequestHandler):
    
    # get is async task
    def get(self):
        
        print "GET ApiRuleBackfill"

        rule = self.request.get("rule", None)
        table = self.request.get("table", None)

        params = {
            "rule": rule,
            "table": table
        }
        
        date = datetime.now().strftime("%Y%m%d_%H%M")
        name = "%s_%s" % (rule, table)
        name = re.sub("[\W\d]", "_", name.strip()) 
        name = "Backfill_%s_%s" % (date, name)

        print "GET task: %s" % name
                
        try:
            task = taskqueue.add(name=name, url='/api/rule/backfill', params=params)
        except TombstonedTaskError, e:
            raise Exception("Task for '%s' is already in the queue." % rule)
            
        response = {
            "enqueued" : True
        }
        self.response.headers['Content-Type'] = 'application/json'   
        self.response.out.write(json.dumps(response))

    def post(self):
        
        print "POST ApiRuleBackfill"
        
        rule = self.request.get("rule", None)
        table = self.request.get("table", None)
        (dataset, table) = parse_bqid(table)  

        print "POST variables: %s %s %s" % (rule, dataset, table)

        def record_callback(tweets):
            
            print "POST record_callback: %s" % len(tweets)
            
            body = {
                "kind": "bigquery#tableDataInsertAllRequest",
                "rows": [{ "insertId" : t["id"], "json" : Utils.scrub(t) } for t in tweets ]
            }
    
            response = get_bq().tabledata().insertAll(projectId=PROJECT_ID, datasetId=dataset, tableId=table, body=body).execute()
            print "POST BQ Response: %s" % response
            
            return response
        
        g = get_gnip()
        end = datetime.now()
        start = end - timedelta(days=7)
        g.query(rule, 0, record_callback=record_callback, use_case="tweets", start=start, end=end)

        response = {
            "completed" : True
        }
        print "POST Response: %s" % response
        
        self.response.headers['Content-Type'] = 'application/json'   
        self.response.out.write(json.dumps(response))
        
class ApiRuleDelete(webapp2.RequestHandler):
    
    def get(self):
        
        value = self.request.get("value")
        rule_delete = {
            "value" : value
        }

        response = rules.delete_rule(rule_delete, url=GNIP_STREAM_URL, auth=(GNIP_USERNAME, GNIP_PASSWORD))
        TABLE_CACHE.clear()
        
        self.response.headers['Content-Type'] = 'application/json'   
        self.response.out.write(json.dumps(response))

class Admin(webapp2.RequestHandler):
    
    def get(self):
        template_data = {}
        self.response.out.write(JINJA.get_template('admin.html').render(template_data))
        
class QueryBuilder():
    
    PUBLIC = "public"
    GNIP = "gnip"
    
    type = None
    table = None
    field = None
    charttype = None
    interval = None
    from_clause = None
    prefix = ""

    def __init__(self, type, table, field, charttype, interval):
        self.type = type
        self.table = table
        self.field = field
        self.charttype = charttype
        self.interval = interval
        self.from_clause = "[%s]" % self.table
        
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
                self.prefix = '#'
                col = 'entities.hashtags.text'
                flatten_field = 'entities.hashtags'
            elif self.field == 'mentions':
                object = 'User mentions'
                self.prefix = '@'
                col = 'entities.user_mentions.screen_name'
                flatten_field = 'entities.user_mentions'            
            elif self.field == 'sources':
                object = 'Tweet sources'
                self.prefix = ''
                col = 'source' 
        else:
            created_field = 'postedTime'
            if self.field == 'hashtags':
                object = 'Hashtags'
                self.prefix = '#'
                col = 'twitter_entities.hashtags.text'
                flatten_field = 'twitter_entities.hashtags'
            elif self.field == 'mentions':
                object = 'User mentions'
                self.prefix = '@'
                col = 'twitter_entities.user_mentions.screen_name'
                flatten_field = 'twitter_entities.user_mentions'            
            elif self.field == 'sources':
                object = 'Tweet sources'
                self.prefix = ''
                col = 'generator.displayName'
                            
        select = "%s as value,count(*) as count" % col 
        fromclause = "flatten(%s, %s)" % (self.from_clause, flatten_field) if flatten_field else self.from_clause
        time_filter = "DATE_ADD(CURRENT_TIMESTAMP(), -%s, 'DAY')" % (self.interval)
        filter = "%s is not null AND %s > %s" % (col, created_field, time_filter)
        groupby = "value"
        orderby = "count DESC"
        limit = 20
        
        # requires join AND tranlating all tables to scoped t1/t2
        if self.charttype == "timeseries":
            
            # default create hour: daily
            create_hour = "CONCAT('2015-', STRING(MONTH(TIMESTAMP(t1.%s))), '-', STRING(DAY(TIMESTAMP(t1.%s))), ' 00:00')" % (created_field, created_field)
            
            # create hour: hourly
            if self.interval == 1:
                create_hour = "CONCAT('2015-', STRING(MONTH(TIMESTAMP(t1.%s))), '-', STRING(DAY(TIMESTAMP(t1.%s))), ' ', LPAD(STRING(HOUR(TIMESTAMP(t1.%s))), 2, '0'), ':00')" % (created_field, created_field, created_field)
                
            select = "t1.%s, %s AS create_hour" % (select, create_hour)
            fromclause = "flatten(%s, %s)" % (self.from_clause, flatten_field) if flatten_field else self.from_clause
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
                """ % (fromclause, col, fromclause, col, created_field, time_filter, col, col, col)
            filter = "t1.%s is not null AND t1.%s > %s" % (col, created_field, time_filter)
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
    
    def c3_args(self, query, results):
        
        args = {}
        
        if self.charttype == 'donut' or self.charttype == 'bar':
                 
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
                    'type' : self.charttype
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
            
        elif self.charttype == 'timeseries':

            # key: source, value: [source, d1, d2, d3...]
            now = datetime.now()
            buckets = {}

            delta = timedelta(days=1)
            timeformat = "%Y-%m-%d 00:00"
             
            if self.interval == 1:
                delta = timedelta(hours=1)
                timeformat = "%Y-%m-%d %H:00"
                
            header = ['x']
            header_lookup = ['x']
            start = now - timedelta(days=self.interval)
            while True:
                index = start.strftime(timeformat)
                
                # header returned to c3 needs to be 0-padded dates
                header.append(index)
                
                # matching index with bigquery results is not 0-padded dates
                header_lookup.append(index.replace("-0", "-"))
                start = start + delta
                if start > now:
                    break
            
            columns = [header]
            
#             print columns
#             print results
            
            if 'rows' in results:
                for row in results['rows']:
                    
                    for key, dict_list in row.iteritems():
                        value = dict_list[0]['v']
                        if value:
                            value = self.prefix + REMOVE_HTML.sub('', value)
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
                    'xFormat' : '%Y-%m-%d %H:%M',
                    'columns' : columns 
                },
                'axis': {
                    'x': {
                        'type': 'timeseries',
                        'tick': {
                            'format': '%Y-%m-%d %H:%M'
                        }
                    }
                },
                'query' : query.strip() if query else query,
                'title' : "Sources by hour" 
            }
            
        return args
            
    
BQ_CREDENTIALS = appengine.AppAssertionCredentials(scope='https://www.googleapis.com/auth/bigquery')
BQ_HTTP = BQ_CREDENTIALS.authorize(httplib2.Http())

def get_bq():
    return build('bigquery', 'v2', http=BQ_HTTP)

def get_gnip():
    g = searchclient.SearchClient(GNIP_USERNAME, GNIP_PASSWORD, GNIP_SEARCH_URL)
    return g

def millis_to_date(ts):
    return datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M')
    
def parse_bqid(id):
    if id:
        import re
        return re.split('\:|\.', id)
    return None

def make_tag(dataset, table):
    return "%s.%s" % (dataset, table)
    
application = webapp2.WSGIApplication([
    
    # API calls supporting JSON
    ('/api/rule/list', ApiRuleList),
    ('/api/rule/test', ApiRuleTest),
    ('/api/rule/add', ApiRuleAdd),
    ('/api/rule/delete', ApiRuleDelete),
    ('/api/rule/backfill', ApiRuleBackfill),
    ('/api/table/list', ApiTableList),
    ('/api/table/add', ApiTableAdd),
    ('/api/table/([A-Za-z0-9\-\_\:\.]+)/delete', ApiTableDelete),
    ('/api/table/([A-Za-z0-9\-\_\:\.]+)/data', ApiTableData),
    
    # web pages
    ('/table/list', TableList),
    ('/table/([A-Za-z0-9\-\_\:\.]+)', TableDetail),
    ('/rule/list', RuleList),
    ('/admin', Admin),
    ('/', Home),
    
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
    except HttpError, e:
        status = e.resp.status
        message = e._get_reason()
    except Exception, e:
        status = 500
        message = e.message
    except:
        status = 500
        message = str(exception)
        logging.exception(exception)

    response.set_status(status)
    response.out.write(message)

# application.error_handlers[500] = handle_500