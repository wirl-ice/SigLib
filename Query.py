#Dependencies

from datetime import datetime, timezone
import os
import getpass
import logging
import sys
import requests
import json
from osgeo import ogr, osr
import geopandas
from eodms_api_client import EodmsAPI
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt
import csv


class Query(object):
    """
    Query class used to search for imagery from a variety of sources

        **Parameters**
            
            *roi*       : name of roi shapefile     

            *srid*      : srid of roi shapefile   

            *dir*       : directory that contains roi shapefile

            *method*    : user selected query method, ie metadata, cis, EODMS

    """

    def __init__(self, db, roi, srid, roidir, localtable, spatialrel, outputDir, method, loghandler = None):

        self.status = "ok"  ### For testing
        
        if loghandler != None:
            self.loghandler = loghandler               #Logging setup if loghandler sent, otherwise, set up a console only logging system
            self.logger = logging.getLogger(__name__)
            self.logger.addHandler(loghandler)
            self.logger.propagate = False
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger = logging.getLogger(__name__)                        
            self.logger.setLevel(logging.DEBUG)
            self.logger.addHandler(logging.StreamHandler())

        self.db = db
        self.roi = roi
        self.roiSRID = srid
        self.roiDir = roidir
        self.table_to_query = localtable
        self.spatialrel = spatialrel
        self.outputDir = outputDir

        print ('Queriying')
        #self.read_shp()


        if method == 'metadata':
            copylist, instimg = db.qrySelectFromAvailable(self.roi, self.table_to_query, self.spatialrel, self.roiSRID)
            filename = self.create_filename(self.outputDir, roi, method)
            print ('Results saved to {}'.format(filename))
            db.exportToCSV_Tmp(instimg,filename)
            return
        elif method == 'cis':
            #do cis query
            pass
        elif method == 'EODMS': #set up to query RSAT-1 data
            #query from EODMS
            allison=False
            if allison:
                roiShpFile = os.path.join(self.roiDir, roi)
                queryParams = self.readShpFile(roiShpFile)
                records = self.queryEODMS(queryParams)
            else:
                records = self.queryEODMS_MB(self.roiDir, roi, 'Radarsat1')

            filename = self.create_filename(self.outputDir, roi, method)
            db.exportToCSV_Tmp(records, filename)
            print('File saved to {}'.format(filename))

        elif method == 'ORDER_EODMS':
            filename = input("Enter CSV filename of images to order: ")
            record_id = self.get_EODMS_ids_from_csv(filename)
            print ('Ordering {} images: '.format(len(record_id)))
            print(record_id)
            submit_order = input("Would you like to order {} images? [Y/N]\t".format(len(record_id)))
            if submit_order.lower() == 'y':
                self.order_to_EODMS(record_id)
        elif method == 'SENTINEL':
            records = self.queryCopernicus(self.roiDir, roi, 'Sentinel-2')
            filename = self.create_filename(self.outputDir, roi, method)
            db.exportToCSV_Tmp(records, filename)
            print('File saved to {}'.format(filename))
        elif method =='DOWNLOAD_EODMS':
            self.download_images_from_EODMS(self.outputDir)
        elif method =='DOWNLOAD_SENTINEL':
            self.download_images_from_Copernicus(self.outputDir)
        else:
            self.logger.error("Valid Query Method not selected, cannot complete task.")
            return

    def create_filename(self, outputDir, roi, method):

        now = datetime.now()
        dt_string = now.strftime("%d-%m-%Y %H:%M:%S")
        filename = roi + '_' + method + '_' + dt_string + '.csv'
        fullpath = os.path.join(outputDir,filename)
        return fullpath


    def queryEODMS(self, queryParams):
        """
        Query the EODMS database for records matching a given spatial and 
        temperoral region

        **Parameters**
            
            *queryParams*  : (dict) geometrical and temporal parameters to query on

        """
        
        session = requests.Session()
        print("Querying the EODMS database.")
        username = input("Enter your EODMS username: ")
        password = getpass.getpass("Enter your EODMS password: ")
        session.auth = (username, password)

        #Query EODMS
        records = []
        for query_id, item in queryParams.items():

            start_date = datetime.strptime(item['fromdate'], "%Y/%m/%d")
            #if start_date.tzinfo is None:
            #    start_date = start_date.replace(tzinfo=timezone.utc)
            end_date = datetime.strptime(item['todate'], '%Y/%m/%d')
            #if end_date.tzinfo is None:
            #    end_date = end_date.replace(tzinfo=timezone.utc)

            print('ROI start date: {}  end date: {}'.format(start_date, end_date))
            coordinates = item['geom']
            records.append(self.getEODMSRecords(session, start_date, end_date, coordinates))
            

        order = []
        for record in records:
            for item in record:
                order.append(item)
        n = len(order)
        print("Found {} records!".format(n))
        
        #Submit Order
        submit_order = input("Would you like to order {} images? [Y/N]\t".format(n))
        if submit_order.lower() == 'y':
            orderquery = self.buildQuery(records)
            self.submit_post(orderquery)

        return records

    def getEODMSRecords(self, session, start_date, end_date, coords):
        """ 
        query EODMS database for records within region and date range
            
            **Parameters**

                *session*       : requests session for HTTP requests
                
                *start_date*    : (datetime) find images after this date
                
                *end_date*      : (datetime) find images before this date
                
                *coords*        : region on interest

        """

        # convert coordiantes to string
        polygon_coordinates = ''
        for i in range(len(coords)):
            polygon_coordinates += str(coords[i][0]) + '+' + str(coords[i][1])
            if i < len(coords)-1:
                polygon_coordinates += '%2C'

        #query EODMS database
        query_url = '''https://www.eodms-sgdot.nrcan-rncan.gc.ca/wes/rapi/search?collection=Radarsat1&''' + \
                    '''query=CATALOG_IMAGE.THE_GEOM_4326+INTERSECTS+POLYGON+%28%28''' + \
                        polygon_coordinates + '''%29%29&''' + \
                    '''resultField=RSAT1.BEAM_MNEMONIC&maxResults=5000&format=json'''

        response = session.get(query_url)
        rsp = response.content
        response_dict = json.loads(rsp.decode('utf-8'))


        results = response_dict['results']
        n = len(results)    
        if n == 1000:
            print("Warning: limit of records has been reached, results may be incomplete")
            print("Try reducing the region of interest to avoid reaching the query limit")

        #Filter query results for specified daterange
        records = []  #records to order
        for result in results:
            if result["isOrderable"]:
                for item in result["metadata2"]:
                    if item["id"] == "CATALOG_IMAGE.START_DATETIME":
                        #import pandas
                        #date = pandas.to_datetime(item["value"])
                        date = datetime.strptime(item["value"], "%Y-%m-%d %H:%M:%S %Z")
                if date >= start_date and date <= end_date:
                    record_id = result["recordId"]
                    collection_id = result["collectionId"]
                    records.append([record_id, collection_id])
        
        return records

    def buildQuery(self, records):
        """ 
        builds query to order records 
            
            **Parameters**
            
                *records*   : (list) record and collection ids from EODMS query result

        """

        query = {"destinations": [], "items": []}

        for item in records:
            query['items'].append({"collectionId": item[1], "recordId": item[0]})
        
        return query

    def submit_post(session, query):
        """ 
        submits order to EODMS
            
            **Parameters**
            
                *query*     : (dict) query with record and collection ids

        """

        rest_url = "https://www.eodms-sgdot.nrcan-rncan.gc.ca/wes/rapi/order"
        response = session.post(rest_url, data=str(query))

    def readShpFile(self, filename):
        """ 
        extracts coordinate geometry and fromdate/todate from an ESRI shapefile

        **Parameters**
            
            *filename*  : (string) path+name of roi shpfile

        """

        driver = ogr.GetDriverByName('ESRI Shapefile')
        shpfile = driver.Open(filename+'.shp')
        shape = shpfile.GetLayer(0)

        #Ensure query is done in WGS84
        sourceSR = shape.GetSpatialRef()
        targetSR = osr.SpatialReference()
        targetSR.ImportFromEPSG(4326) # WGS84
        coordTrans = osr.CoordinateTransformation(sourceSR,targetSR)

        query_dict = {}

        for i in range(len(shape)):

            feature = shape.GetNextFeature()
            geom = feature.GetGeometryRef()
            geom.Transform(coordTrans)

            jsn = feature.ExportToJson()
            dct = json.loads(jsn)

            geom = dct["geometry"]
            coords = geom["coordinates"]
            properties = dct["properties"]

            shape_id = str(properties['OBJ']) + str(properties['INSTID'])
            fromdate = properties["FROMDATE"]
            todate = properties["TODATE"]
            
            query_dict[shape_id] = {'geom': coords[0], 'fromdate': fromdate, 'todate': todate}
            
        return query_dict


    def read_shp(self):
        roiShapeFile = "/Users/jazminromero/Desktop/shp/ArcticBay.shp"
        print ('Reading shape file: {}'.format(roiShapeFile))
        shapefile = geopandas.read_file(roiShapeFile, driver='ESRI')
        print(shapefile)

        roiGeojson = "/Users/jazminromero/Desktop/shp/ArcticBay.geojson"
        print('Writing geojson file: {}'.format(roiGeojson))
        shapefile.to_file(roiGeojson, driver='GeoJSON')


    def queryEODMS_MB(self, roiDir, roi, collection):

        # 1. Transform shapefile into geojson
        shpFilename = os.path.join(roiDir, roi)
        shpFilename = shpFilename + '.shp'
        shapefile = geopandas.read_file(shpFilename, driver='ESRI Shapefile')

        geojsonFilename = roi + '.geojson'
        geojsonFilename = os.path.join(roiDir, geojsonFilename)
        shapefile.to_file(geojsonFilename, driver='GeoJSON')

        # 2. Retrieve fromDate and startDate from file
        f = open(geojsonFilename)
        data = json.load(f)
        fromdate = data['features'][0]['properties']['FROMDATE']
        todate = data['features'][0]['properties']['TODATE']


        # 3. Query EODMS
        client = EodmsAPI(collection=collection)
        client.query(start=fromdate, end=todate, geometry=geojsonFilename)
        len(client.results)

        record_ids = client.results.to_dict()
        print(record_ids)

        return record_ids


    def read_csv(self, filename):
        with open(filename, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                print(row)

        return reader

    def get_EODMS_ids_from_csv(self, filename):
        record_id =[]
        with open(filename, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                record_id.append(row['EODMS RecordId'])

        return record_id


    def order_to_EODMS(self, record_ids):
        collection = input("Enter Collection for order: ")
        client = EodmsAPI(collection=collection)
        order_id = client.order(record_ids)
        print ('Order {} submitted. Wait for confirmation email.'.format(order_id))


    def download_images_from_EODMS(self, output_dir):
        collection = input("Enter Collection for downloading: ")
        client = EodmsAPI(collection=collection)
        order_item_id = input("Enter order_item_id:")
        client.download(order_item_id, output_dir)

    def queryCopernicus(self, roiDir, roi, platform):

        # 1. Transform shapefile into geojson
        shpFilename = os.path.join(roiDir, roi)
        shpFilename = shpFilename + '.shp'
        shapefile = geopandas.read_file(shpFilename, driver='ESRI Shapefile')

        geojsonFilename = roi + '.geojson'
        geojsonFilename = os.path.join(roiDir, geojsonFilename)
        shapefile.to_file(geojsonFilename, driver='GeoJSON')

        # 2. Retrieve fromDate and startDate from file
        f = open(geojsonFilename)
        data = json.load(f)
        fromdate = data['features'][0]['properties']['FROMDATE']
        todate = data['features'][0]['properties']['TODATE']
        fromdate_obj = datetime.strptime(fromdate, '%Y-%m-%d')
        todate_obj = datetime.strptime(todate, '%Y-%m-%d')

        # 3. Query Sentinel
        footprint = geojson_to_wkt(read_geojson(geojsonFilename))
        username = input("Enter your Copernicus username: ")
        password = getpass.getpass("Enter your Copernicus password: ")
        api = SentinelAPI(username, password, 'https://scihub.copernicus.eu/dhus')
        products = api.query(footprint,
                             date = (fromdate_obj,todate_obj),
                             platformname = platform,
                             processinglevel='Level-2A')
        record_ids = api.to_dataframe(products)

        #products = api.query(footprint,
        #                     date=(fromdate_obj, todate_obj),
        #                     platformname=platform,
        #                     processinglevel='Level-2A',
        #                     producttype='GRD',
        #                     sensoroperationalmode='SM')
        #record_ids = api.to_dataframe(products)

        #sensoroperationalmode=Possible values are: SM, IW, EW, WV


        #products = api.query(footprint,
        #                     date=('20100101', '20201230'),
        #                     platformname=platform,
        #                     processinglevel='Level-2A')
        #record_ids = api.to_dataframe(products)

        return record_ids

    def download_images_from_Copernicus(self, output_dir):

        #Read record UUID from filelocation
        filename = input("Enter filelocation of images ids: ")
        records = []
        with open(filename, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                #records.append(row['uuid'])
                records.append({"title": row['title'], "uuid": row['uuid']})

        #print(records)

        username = input("Enter your Copernicus username: ")
        password = getpass.getpass("Enter your Copernicus password: ")
        api = SentinelAPI(username, password, 'https://scihub.copernicus.eu/dhus')


        for record in records:
        # download the file
         try:
            id = record['uuid']
            title = record['title']
            is_online = api.is_online(id)
            if is_online:
                 print (title + ' is online')
                 #api.download(id,output_dir)
                 print('Download successfull')
                 #break
            else:
                print(title + ' is offline')
         except:
            print ('Try next file')




