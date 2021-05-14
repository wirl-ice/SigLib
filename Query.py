#Dependencies
import psycopg2
import psycopg2.extras
from psycopg2.extensions import AsIs
from datetime import datetime, timezone
import numpy
import scipy.stats as stats
import glob
import getpass
import logging
import sys
import requests
import json
from osgeo import ogr, osr

class Query(object):
    """
    Query class used to search for imagery from a variety of sources

        **Parameters**
            
            *roi*       : name of roi shapefile     

            *srid*      : srid of roi shapefile   

            *dir*       : directory that contains roi shapefile

            *method*    : user selected query method, ie metadata, cis, EODMS

    """

    def __init__(self, roi, srid, roidir, method, loghandler = None):

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

        self.roi = roi
        self.roiSRID = srid
        self.roiDir = roidir

        
        if method == 'metadata':
            #query from metadata table
            pass
        elif method == 'cis':
            #do cis query
            pass
        elif method == 'EODMS':
            #query from EODMS
            roiShpFile = os.path.join(self.roiDir, roi)
            queryParams = self.readShpFile(roiShpFile)
            self.queryEODMS(queryParams)
        else:
            self.logger.error("Valid Query Method not selected, cannot complete task.")
            return

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
            if start_date.tzinfo is None:
                start_date = start_date.replace(tzinfo=timezone.utc)
            end_date = datetime.strptime(item['todate'], '%Y/%m/%d')
            if end_date.tzinfo is None:
                end_date = end_date.replace(tzinfo=timezone.utc)

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
        if ans.lower() == 'y':
            orderquery = self.buildQuery(records)
            self.submit_post(orderquery)
                 

    def getEODMSRecords(self, session, start_date, end_date, coords):
        """ 
        query EODMS database for records within region and date range
            
            **Parameters**

                *session*       : requests session for HTTP requests
                
                *start_date*    : (pandas datetime) find images after this date
                
                *end_date*      : (pandas datetime) find images before this date
                
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
                        date = pandas.to_datetime(item["value"])
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

    def submit_post(query):
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
        shpfile = driver.Open(filename)

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

    
    