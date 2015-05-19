#!/usr/bin/env python
# -*- coding: UTF-8 -*-
__author__="Scott Hendrickson, Josh Montague" 

import sys
import requests
import json
import codecs
import datetime
import time
import os
import re

reload(sys)
sys.stdout = codecs.getwriter('utf-8')(sys.stdout)
sys.stdin = codecs.getreader('utf-8')(sys.stdin)

# formatter of data from API 
TIME_FMT = "%Y%m%d%H%M"
PAUSE = 3 # seconds between page requests

#############################################
# Some constants to configure column retrieval from TwacsCSV
DATE_INDEX = 1
TEXT_INDEX = 2
LINKS_INDEX = 3
USER_NAME_INDEX = 7 

class GnipSearchAPI(object):
    
    def __init__(self
            , user
            , password
            , stream_url
            , paged = False
            , output_file_path = None
            , token_list_size = 20
            ):
        #############################################
        # set up query paramters
        # default tokenizer and character limit
        self.token_list_size = int(token_list_size)
        self.output_file_path = output_file_path
        self.paged = paged
        self.user = user
        self.password = password
        self.stream_url = stream_url
        # get a parser for the twitter columns
        # TODO: use the updated retriveal methods in gnacs instead of this
#         self.twitter_parser = TwacsCSV(",", None, False, True, False, True, False, False, False)

    def set_index(self, use_case, count_bucket):
        self.use_case = use_case
        space_tokenizer = False
        char_upper_cutoff = 20  # longer than for normal words because of user names
        if use_case.startswith("links"):
            char_upper_cutoff=100
            space_tokenizer = True
#         self.freq = SimpleNGrams(charUpperCutoff=char_upper_cutoff, space_tokenizer=space_tokenizer)
#         self.freq = SimpleNGrams(char_upper_cutoff=char_upper_cutoff, tokenizer="space")
        if use_case.startswith("user"):
            self.index = USER_NAME_INDEX
        elif use_case.startswith("wordc"):
            self.index = TEXT_INDEX
        elif use_case.startswith("rate"):
            self.index = DATE_INDEX
        elif use_case.startswith("link"):
            self.index = LINKS_INDEX
        elif use_case.startswith("time"):
            if not self.stream_url.endswith("counts.json"): 
                self.stream_url = self.stream_url[:-5] + "/counts.json"
            if count_bucket not in ['day', 'minute', 'hour']:
                print >> sys.stderr, "Error. Invalid count bucket: %s \n"%str(count_bucket)
                sys.exit()
        
    def set_dates(self, start, end):
        # re for the acceptable datetime formats
        timeRE = re.compile("([0-9]{4}).([0-9]{2}).([0-9]{2}).([0-9]{2}):([0-9]{2})")
        if start:
            dt = re.search(timeRE, start)
            if not dt:
                print >> sys.stderr, "Error. Invalid start-date format: %s \n"%str(start)
                sys.exit()    
            else:
                f =''
                for i in range(re.compile(timeRE).groups):
                    f += dt.group(i+1) 
                self.fromDate = f
        if end:
            dt = re.search(timeRE, end)
            if not dt:
                print >> sys.stderr, "Error. Invalid end-date format: %s \n"%str(end)
                sys.exit()
            else:
                e =''
                for i in range(re.compile(timeRE).groups):
                    e += dt.group(i+1) 
                self.toDate = e

    def name_munger(self, f):
        """Creates a file name per input rule when reading multiple input rules."""
        f = re.sub(' +','_',f)
        f = f.replace(':','_')
        f = f.replace('"','_Q_')
        f = f.replace('(','_p_') 
        f = f.replace(')','_p_') 
        self.file_name_prefix = f[:42]

    def req(self):
        try:
            s = requests.Session()
            s.headers = {'Accept-encoding': 'gzip'}
            s.auth = (self.user, self.password)
            res = s.post(self.stream_url, data=json.dumps(self.rule_payload))
        except requests.exceptions.ConnectionError, e:
            print >> sys.stderr, "Error (%s). Exiting without results."%str(e)
            sys.exit()
        except requests.exceptions.HTTPError, e:
            print >> sys.stderr, "Error (%s). Exiting without results."%str(e)
            sys.exit()
        return res.text

    def parse_JSON(self):
        acs = []
        repeat = True
        page_count = 0
        while repeat:
            doc = self.req()
            try:
                tmp_response =  json.loads(doc)
                if "results" in tmp_response:
                    acs.extend(tmp_response["results"])
                if "error" in tmp_response:
                    print >> sys.stderr, "Error, invalid request"
                    print >> sys.stderr, "Query: %s"%self.rule_payload
                    print >> sys.stderr, "Response: %s"%doc
                    raise Exception(tmp_response["error"]["message"])
            except ValueError:
                print >> sys.stderr, "Error, results not parsable"
                print >> sys.stderr, doc
                sys.exit()

            repeat = False
            if self.paged:
                if len(acs) > 0:
                    if self.output_file_path is not None:
                        file_name = self.output_file_path + "/{0}_{1}.json".format(
                                str(datetime.datetime.utcnow().strftime(
                                    "%Y%m%d%H%M%S"))
                              , str(self.file_name_prefix))
                        with codecs.open(file_name, "wb","utf-8") as out:
                            print >> sys.stderr, "(writing to file ...)"
                            for item in tmp_response["results"]:
                                out.write(json.dumps(item)+"\n")
#                     else:
#                         # if writing to file, don't keep track of all the data in memory
#                         acs = []
                else:
                    print >> sys.stderr, "no results returned for rule:{0}".format(str(self.rule_payload))
                if "next" in tmp_response:
                    self.rule_payload["next"]=tmp_response["next"]
                    repeat = True
                    page_count += 1
                    print >> sys.stderr, "Fetching page {}...".format(page_count)
                else:
                    if "next" in self.rule_payload:
                        del self.rule_payload["next"]
                    repeat = False
                time.sleep(PAUSE)
        return acs

    def query_api(self
            , pt_filter
            , max_results = 100
            , use_case = "wordcount"
            , start = None
            , end = None
            , count_bucket = "day" 
            , csv_flag = False
            , query = False):
        self.set_index(use_case, count_bucket)
        self.set_dates(start, end)
        self.name_munger(pt_filter)
        if self.paged:
            # avoid making many small requests
            max_results = 500
        self.rule_payload = {
                                'query': pt_filter
                         , 'maxResults': int(max_results)
                          , 'publisher': 'twitter'
                            }
        if start:
            self.rule_payload["fromDate"] = self.fromDate
        if end:
            self.rule_payload["toDate"] = self.toDate
        if use_case.startswith("time"):
            self.rule_payload["bucket"] = count_bucket
        if query:
            print >>sys.stderr, "API query:"
            print >>sys.stderr, self.rule_payload
            sys.exit() 
        #
        self.doc = []
        self.res_cnt = 0
        self.delta_t = 1    # keeps non-'rate' use-cases from crashing 
        # default delta_t = 30d & search only goes back 30 days
        self.oldest_t = datetime.datetime.utcnow() - datetime.timedelta(days=30)
        self.newest_t = datetime.datetime.utcnow()
        for rec in self.parse_JSON():
            self.res_cnt += 1
            if use_case.startswith("rate"):
                t_str = self.twitter_parser.procRecordToList(rec)[self.index]
                t = datetime.datetime.strptime(t_str,"%Y-%m-%dT%H:%M:%S.000Z")
                if t < self.oldest_t:
                    self.oldest_t = t
                if t > self.newest_t:
                    self.newest_t = t
                self.delta_t = (self.newest_t - self.oldest_t).total_seconds()/60.
            elif use_case.startswith("links"):
                link_str = self.twitter_parser.procRecordToList(rec)[self.index]
                print "+"*20
                print link_str
                if link_str != "GNIPEMPTYFIELD" and link_str != "None":
                    exec("link_list=%s"%link_str)
                    for l in link_list:
                        self.freq.add(l)
                else:
                    self.freq.add("NoLinks")
            elif use_case.startswith("json"):
                self.doc.append(json.dumps(rec))
            elif use_case.startswith("tweets"):
                self.doc.append(rec)
            elif use_case.startswith("geo"):
                lat, lng = None, None
                if "geo" in rec:
                    if "coordinates" in rec["geo"]:
                        [lat,lng] = rec["geo"]["coordinates"]
                record = { "id": rec["id"].split(":")[2]
                        , "postedTime": rec["postedTime"].strip(".000Z")
                        , "latitude": lat
                        , "longitude": lng }
                self.doc.append(record)
                #self.doc.append(json.dumps(record))
            elif use_case.startswith("time"):
                self.doc.append(rec)
            else:
                thing = self.twitter_parser.procRecordToList(rec)[self.index]
                self.freq.add(thing)
        return self.get_repr(pt_filter, csv_flag)

    def get_repr(self, pt_filter, csv_flag):
        WIDTH = 60
        res = [u"-"*WIDTH]
        if self.use_case.startswith("rate"):
            rate = float(self.res_cnt)/self.delta_t
            unit = "Tweets/Minute"
            if rate < 0.01:
                rate *= 60.
                unit = "Tweets/Hour"
            res.append("   PowerTrack Rule: \"%s\""%pt_filter)
            res.append("Oldest Tweet (UTC): %s"%str(self.oldest_t))
            res.append("Newest Tweet (UTC): %s"%str(self.newest_t))
            res.append("         Now (UTC): %s"%str(datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
            res.append("      %5d Tweets: %6.3f %s"%(self.res_cnt, rate, unit))
            res.append("-"*WIDTH)
        elif self.use_case.startswith("geo"):
            if csv_flag:
                res = []
                for x in self.doc:
                    try:
                        res.append("{},{},{},{}".format(x["id"], x["postedTime"], x["longitude"], x["latitude"]))
                    except KeyError, e:
                        print >> sys.stderr, str(e)
            else:
                res = [json.dumps(x) for x in self.doc]
        elif self.use_case.startswith("json"):
            res = self.doc
        elif self.use_case.startswith("tweets"):
            res = self.doc
            return res
        elif self.use_case.startswith("word") or self.use_case.startswith("user"):
            res.append("%22s -- %10s     %8s (%d)"%( "terms", "mentions", "activities", self.res_cnt))
            res.append("-"*WIDTH)
            for x in self.freq.get_tokens(self.token_list_size):
                res.append("%22s -- %4d  %5.2f%% %4d  %5.2f%%"%(x[4], x[0], x[1]*100., x[2], x[3]*100.))
            res.append("-"*WIDTH)
        elif self.use_case.startswith("time"):
            if csv_flag:
                res = ["{:%Y-%m-%dT%H:%M:%S},{}".format(
                    datetime.datetime.strptime(x["timePeriod"]
                  , TIME_FMT)
                  , x["count"]) 
                        for x in self.doc]
            else:
                res = [json.dumps({"results": self.doc})] 
        else:
            res[-1]+=u"-"*WIDTH
            res.append("%100s -- %10s     %8s (%d)"%("links", "mentions", "activities", self.res_cnt))
            res.append("-"*2*WIDTH)
            for x in self.freq.get_tokens(self.token_list_size):
                res.append("%100s -- %4d  %5.2f%% %4d  %5.2f%%"%(x[4], x[0], x[1]*100., x[2], x[3]*100.))
            res.append("-"*WIDTH)
        return "\n".join(res)

if __name__ == "__main__":
    g = GnipSearchAPI("USER"
            , "PASSWORD"
            , "STREAM_URL",
            )
    
    term = "captain america"
    
    print g.query_api(term)
    print g.query_api(term, 50)
    print g.query_api(term, 10, "json")
    print g.query_api(term, 0, "timeline")
    print g.query_api(term, 10, "users")
    print g.query_api(term, 10, "rate")
    print g.query_api(term, 10, "links")
