import os
import sys
import time
import urllib2
import base64
import json
import ssl
import zlib
import threading

from threading import Lock
from httplib import *
from config import Config
from apiclient.errors import *
import logging.config
import tweepy

from utils import Utils


NEWLINE = '\r\n'
SLEEP_TIME = 10

f = file("./config")
config = Config(f)

class GnipListener(object):

    CHUNK_SIZE = 4 * 1024
    KEEP_ALIVE = 30  # seconds

    HEADERS = { 'Accept': 'application/json',
                'Connection': 'Keep-Alive',
                'Accept-Encoding' : 'gzip',
                'Authorization' : 'Basic %s' % base64.encodestring('%s:%s' % (config.GNIP_USERNAME, config.GNIP_PASSWORD))  }

    """docstring for ClassName"""
    def __init__(self, schema, table_mapping, logger=None):

      self.schema = schema
      self.table_mapping = table_mapping
      self.default_table = table_mapping.values()[0]
      self.count = 0
      self.logger = logger

    def on_data(self, data):
        # get bulk records, but process individually based on tag-based routing
        records_str = data.strip().split(NEWLINE)
        for r in records_str:
            record = json.loads(r)
            if not record.get('delete', None):
                tags = self.get_table_tags(record)

                if not tags:
                    tags = [self.default_table]
                # process multiple tags on a record
                for tag in tags:
                    table = None
                    if not tag:
                        table = self.default_table
                    else:
                        table = self.table_mapping.get(tag, None)
                        if not table:
                            table = tag.split(".")
                            created = Utils.insert_table(table[0], table[1]) #, self.schema)
                            
                            # Brand new table 
                            if created and created != True:
                                self.logger.info('Created BQ table: %s' % tag)
                                
                            self.table_mapping[tag] = table

                    record_scrubbed = Utils.scrub(record)
                    Utils.insert_records(table[0], table[1], [record_scrubbed])

                if self.logger:
                    self.logger.info('@%s: %s (%s)' % (record['actor']['preferredUsername'], record['body'].encode('ascii', 'ignore'), tags))

                self.count = self.count + 1

        return True

    def get_table_tags(self, record):
        gnip = record.get('gnip', None)
        if gnip:
            matching_rules = gnip.get('matching_rules', None)
            if matching_rules:
                return [rule.get("tag", None) for rule in matching_rules]

        return None

    @staticmethod
    def start(schema, logger):

        # initialize table mapping for default table
        # BUGBUG: initialize based on query to prod
        table_mapping = {
             config.DATASET_ID + "." + config.TABLE_ID : [config.DATASET_ID, config.TABLE_ID]
         }

        datasets = Utils.get_bq().datasets().list(projectId=config.PROJECT_ID).execute()
        datasets = datasets.get("datasets", None)

        for d in datasets:
            ref = d.get("datasetReference", None)
            bq_tables = Utils.get_bq().tables().list(projectId=ref.get("projectId"), datasetId=ref.get("datasetId")).execute()
            if bq_tables['totalItems'] > 0:
                for t in bq_tables.get("tables", None):
                    ref = t.get("tableReference", None)
                    dataset_id = ref.get("datasetId", None)
                    table_id = ref.get("tableId", None)
                    key = Utils.make_tag(dataset_id, table_id)
                    table_mapping[key] = [dataset_id, table_id]

        print("Initialized tables: %s" % table_mapping)

        listener = GnipListener(schema, table_mapping, logger=logger)

        while True:

            stream = None

            try:
                # clean gnip headers
                _headers = GnipListener.HEADERS
                headers = {}
                for k, v in _headers.items():
                    headers[k] = v.strip()

                #req = urllib2.Request(config.GNIP_STREAM_URL, headers=GnipListener.HEADERS)
                req = urllib2.Request(config.GNIP_STREAM_URL, headers=headers)
                response = urllib2.urlopen(req, timeout=(1+GnipListener.KEEP_ALIVE))

                decompressor = zlib.decompressobj(16+zlib.MAX_WBITS)
                remainder = ''
                while True:
                    tmp = decompressor.decompress(response.read(GnipListener.CHUNK_SIZE))
                    if tmp == '':
                        return
                    [records, remainder] = ''.join([remainder, tmp]).rsplit(NEWLINE,1)
                    listener.on_data(records)

                get_stream(listener)

            except:

                logger.exception("Unexpected error:");

                if stream:
                    stream.disconnect()

                time.sleep(SLEEP_TIME)

# Write records to BigQuery
class TwitterListener(tweepy.StreamListener):

    # items to track if you're doing a public track call
    _TRACK_ITEMS = [
        '@JetBlue',
        '@southwestair',
        '@AirAsia',
        '@AmericanAir',
        '@flyPAL',
        '@TAMAirlines',
        '@Delta',
        '@virginamerica',
        '@klm',
        '@turkishairlines',
        '@BritishAirways',
        '@usairways',
        '@British_Airways',
        '@westjet',
        '@MAS',
        '@United',
        '@baltiausa',
        '@Lufthansa_DE',
        '@virginatlantic',
        '@virginaustralia',
        '@AlaskaAir',
        '@DeltaAssist',
        '@aircanada',
        '@easyJet',
        '@vueling'
    ]
    TRACK_ITEMS = [
    '#data15',
    '#SelfServeAnalytics',
    '#SelfServeAtTableau',
    '#ServerEmailAlert',
    '#ServerPermission',
    '#ServerToolbox',
    '#ShipTableau',
    '#SlalomDrive',
    '#SmartDriveData',
    '#SolarWindsData',
    '#SquareData',
    '#StatControl',
    '#StateStreetData',
    '#StJosephData',
    '#StoryPointsData',
    '#StubbornCalcs',
    '#SwedishData',
    '#SyscoData',
    '#SyscoSupplyChain',
    '#TabCmD',
    '#Tabjolt',
    '#TableauAcrossDepartments',
    '#TableauAuth',
    '#TableauAWSEC2',
    '#TableauConsultant',
    '#TableauDataGov',
    '#TableauForecast',
    '#TableauGA',
    '#TableauITService',
    '#TableauJedi',
    '#TableauJediCalcs',
    '#TableauLogs',
    '#TableauOLAP',
    '#TableauOnlineAdmin',
    '#TableauSAP',
    '#TableauSFDC',
    '#TableauShoestring',
    '#TableauSith',
    '#TableauSocialData',
    '#TacomaCCData',
    '#TargetData',
    '#TCFData',
    '#TDBankData',
    '#TDEFast',
    '#TDEROI',
    '#TimeInTableau',
    '#ToughData',
    '#TripAdvisorData',
    '#TruecarData',
    '#UnderstandingLOD',
    '#UndertoneData',
    '#UnifundData',
    '#UnlockData',
    '#UpliftData',
    '#UpPerformance',
    '#USDeptLaborData',
    '#UtahStateData',
    '#UTAustinData',
    '#UTElPasoData',
    '#VisualPipeline',
    '#VitamixData',
    '#VizBestPractice',
    '#VizTips',
    '#VSPData',
    '#WalmartData',
    '#WantToAdmin',
    '#WayfairData',
    '#WebDataConnect',
    '#WildData',
    '#YouDidWhat',
    '#ZenAppDesign',
    '#ZenJourney',
    '#ZilliantData',
    '#ZotecData',
    '#TCRumors',
    '#TCCRumors',
    '#Rundata15',
    '#Watchmeviz',
    '#tableauontableau',
    '#data15rumors',
    '#tableaugroups',
    '#datanightout',
    '#data15keynote',
    '#datapluswomen',
    '#tableauzenmaster',
    '#ironviz',
    '#salesdashboards',
    '#3tierdata',
    '#50shadesdata',
    '#AddValueJSAPI',
    '#ADPdata',
    '#advancedcalcs',
    '#advancedlods',
    '#advancedmaps',
    '#advancedrstats',
    '#advtablecalcs',
    '#aljazeeradata',
    '#allstatedata',
    '#amazondata',
    '#AnalyticsAtScale',
    '#AnalyticsPane',
    '#ArbysData',
    '#AxisData',
    '#BasicStats',
    '#BCBSData',
    '#BeyondMarkTypes',
    '#BeyondSparkler',
    '#BIalyticsStories',
    '#BJCData',
    '#BlendingQuestions',
    '#BoeingData',
    '#BoogalooViz',
    '#BostonData',
    '#BostonSciData',
    '#CachingQueries',
    '#tableau',
    '#CalcMethods',
    '#CareerbuilderData',
    '#CarlsonRezidorData',
    '#CarsData',
    '#CartographerTips',
    '#CaterpillarData',
    '#CentsOfData',
    '#CernerData',
    '#CiscoBigDataViz',
    '#CiscoData',
    '#CiscoSupplyChain',
    '#ClevelandClinicData',
    '#ClimateCorpData',
    '#Cloud9Tableau',
    '#ColumbiaData',
    '#ComcastData',
    '#ComicsStorytelling',
    '#ConcurData',
    '#ConEdData',
    '#CoreQuery',
    '#CreativeCalcs',
    '#CreditSuisseData',
    '#CustomAdminViews',
    '#CustomSQL',
    '#DashboardImpossible',
    '#DashboardsMyWay',
    '#DashboardTurbo',
    '#DataToTheCloud',
    '#DataWrangle',
    '#DCPSData',
    '#DearDataTwo',
    '#DeepDiveQueries',
    '#DeloitteData',
    '#DemystifyR',
    '#DenseData',
    '#DePaulData',
    '#DesMoinesData',
    '#DiscoveryStats',
    '#DisneyData',
    '#DoubleDownDataServer',
    '#DrawWithTableau',
    '#DriveAnalytics',
    '#DukeData',
    '#DwollaData',
    '#easyJetData',
    '#eBayData',
    '#EbolaData',
    '#EmbedSFDC',
    '#EmbedTableau',
    '#EMCData',
    '#EnvironicsData',
    '#ExcelWithTableau',
    '#ExelonData',
    '#ExtractAPIPython',
    '#ExtremeParameters',
    '#ExtremeViz',
    '#EYData',
    '#FacebookData',
    '#FloridaDJJData',
    '#FreescaleData',
    '#FresnoStateData',
    '#GamesInTableau',
    '#GetMoreREST',
    '#GlidewellData',
    '#GoogleData',
    '#GrouponData',
    '#GuaranteedRateData',
    '#HadoopItRight',
    '#HandsOnMapping',
    '#HandsOnStats',
    '#HCAData',
    '#HireRockstars',
    '#HomeDepotData',
    '#HootsuiteData',
    '#HotDirtySets',
    '#InteractionsData',
    '#InteractiveParameters',
    '#IntroAPIS',
    '#IntroCalcs',
    '#IntroMapping',
    '#IntroToLOD',
    '#JandJData',
    '#KaiserData',
    '#KantarData',
    '#KatyISDData',
    '#KiewitData',
    '#KKIntlData',
    '#KoboData',
    '#LargestDeployment',
    '#LieWithStats',
    '#LifelineData',
    '#LinguisticData',
    '#LinkedInAnalytics',
    '#LODsOfFun',
    '#LovesSets',
    '#MacysData',
    '#MagicWithMarks',
    '#MapboxFab',
    '#MaximData',
    '#MerkleData',
    '#MichiganData',
    '#MinorityReportUX',
    '#MobileWithTableau',
    '#MtSinaiData',
    '#MylanData',
    '#NetAppData',
    '#NetflixData',
    '#NeuroscienceStorytell',
    '#NextelData',
    '#NotInShowMe',
    '#OIdata',
    '#OldcastleData',
    '#OptimizeLiveQuery',
    '#OrderOfOps',
    '#PaloAltoData',
    '#PiedmontData',
    '#PluralsightData',
    '#PracticalDashboards',
    '#ProgressiveData',
    '#QuestionDrivenViz',
    '#QuickenData',
    '#RDeepDive',
    '#RealtorData',
    '#RenderTableau',
    '#RetailMeNotData',
    '#RosettaData',
    '#RubbermaidData',
    '#RunTheTable',
    ]

    def __init__(self, dataset_id, table_id, logger=None):

      self.dataset_id = dataset_id
      self.table_id = table_id
      self.count = 0
      self.logger = logger
      self.calm_count = 0

    def on_data(self, data):

        self.calm_count = 0

        # Twitter returns data in JSON format - we need to decode it first
        record = json.loads(data)

        if not record.get('delete', None):

            record_scrubbed = Utils.scrub(record)
            Utils.insert_records(self.dataset_id, self.table_id, [record_scrubbed])

            if self.logger:
                self.logger.info('@%s: %s' % (record['user']['screen_name'], record['text'].encode('ascii', 'ignore')))

            self.count = self.count + 1

            return True

    #handle errors without closing stream:
    def on_error(self, status_code):

        if status_code == 420:

            self.backoff('Status 420')
            return True

        if self.logger:
            self.logger.info('Error with status code: %s' % status_code)

        return False

    # got disconnect notice
    def on_disconnect(self, notice):

        self.backoff('Disconnect')
        return False

    def on_timeout(self):

        self.backoff('Timeout')
        return False

    def on_exception(self, exception):

        if self.logger:
            self.logger.exception('Exception')

        return False

    def backoff(self, msg):

        self.calm_count = self.calm_count + 1
        sleep_time = 60 * self.calm_count

        if sleep_time > 320:
            sleep_time = 320

        if self.logger:
            self.logger.info(msg + ", sleeping for %s" % sleep_time)

        time.sleep(60 * self.calm_count)

        return

    @staticmethod
    def start(schema, logger):

        listener = TwitterListener(config.DATASET_ID, config.TABLE_ID, logger=logger)
        auth = tweepy.OAuthHandler(config.CONSUMER_KEY, config.CONSUMER_SECRET)
        auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)

        while True:

            logger.info("Connecting to Twitter stream")

            stream = None

            try:

                stream = tweepy.Stream(auth, listener, headers = {"Accept-Encoding": "deflate, gzip"})

                # Choose stream: filtered or sample
                stream.sample()
                # stream.filter(track=TwitterListener.TRACK_ITEMS)

            except:

                logger.exception("Unexpected error");

                if stream:
                    stream.disconnect()

                time.sleep(60)

def main():

    if config.MODE not in ['gnip', 'twitter']:
        print "Invalid mode: %s" % config.MODE
        exit()

    logger = Utils.enable_logging()
    print "Running in mode: %s" % config.MODE

    schema_file = None
    if config.MODE == 'gnip':
        schema_file = "./schema/schema_gnip.json"
    else:
        schema_file = "./schema/schema_twitter.json"
    schema_str = Utils.read_file(schema_file)
    schema = json.loads(schema_str)

    Utils.insert_table(config.DATASET_ID, config.TABLE_ID, schema)
    print "Default table: %s.%s" % (config.DATASET_ID, config.TABLE_ID)

    if config.MODE == 'gnip':
        GnipListener.start(schema, logger)
    elif config.MODE == 'twitter':
        TwitterListener.start(schema, logger)

if __name__ == "__main__":
    main()
