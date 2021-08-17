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
import shutil


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



        if method == 'metadata':
            self.query_local_table(db, self.roi, self.table_to_query, self.spatialrel, self.roiSRID, method)
            return
        elif method =='download_metabdata':
            self.download_local_table(self, db, roi, method)
            return
        elif method == 'cis':
            self.query_local_table(db, self.roi, 'tblcisarchive', self.spatialrel, self.roiSRID, method)
            return
        elif method == 'EODMS': #set up to query RSAT-1 data
            self.query_eodms(db, self.roi, self.roiDir, method, self.outputDir)
        elif method == 'ORDER_EODMS':
            self.order_eodms(db)
        elif method == 'SENTINEL':
            self.query_sentinel(db, self.roi, self.roiDir, method, self.outputDir)
        elif method =='DOWNLOAD_EODMS':
            self.download_images_from_eodms(self.outputDir)
        elif method =='DOWNLOAD_SENTINEL':
            self.download_images_from_sentinel(db, self.outputDir)
        elif method == 'RAW_SQL':
            self.execute_raw_query(db,self.outputDir)
        else:
            self.logger.error("Valid Query Method not selected, cannot complete task.")
            return

    def create_filename(self, outputDir, roi, method, extension):

        now = datetime.now()
        dt_string = now.strftime("%d-%m-%Y %H:%M:%S")
        filename = roi + '_' + method + '_' + dt_string + extension
        fullpath = os.path.join(outputDir,filename)
        return fullpath

    def create_tablename(self, roi, method):

        now = datetime.now()
        dt_string = now.strftime("%d%m%Y_%H%M")
        tablename = roi + '_' + method + '_' + dt_string
        return tablename


    def save_filepaths(self, output_dir, roi, method, copied_locations):
        try:
            outputName = self.create_filename(output_dir, roi, method + '_PATHS', '.txt')
            with open(outputName, "w") as output:
                for location in copied_locations:
                    output.write(location + "\n")

            print('\nList of downloaded images: {} '.format(outputName))
        except Exception as e:
            print('The following exception occurred when saving the list of downloaded images to a txt file.')
            print(e)
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



    def query_local_table(self, db, roi, tablename, spatialrel, roiSRID, method):
        """
            Query a roi shapefile in a local table.

            **Parameters**
                *db* : An instance of the database class.
                *roi* : The roi shape file.
                *tablename* : The name of the table to query.
                *spatialrel* : Spatial relation to be used in the query (ST_INTERSECTS OR ST_OVERLAPS)
                *roiSRID* : Spatial projection of the roi shapefile.
                *method* : query method

        """

        try:
            copylist, instimg = db.qrySelectFromAvailable(roi, tablename, spatialrel, roiSRID)

            #Options to output results
            print('Query completed. Write query results to: ')
            print('1: CSV file')
            print('2: Local table')
            print('3: Both')
            typOut = input('Enter your option (1,2,3): ')

            downloaded=False
            if typOut == '1' or typOut == '3':
                filename = self.create_filename(self.outputDir, roi, self.table_to_query, '.csv')
                db.exportDict_to_CSV(instimg, filename)
                print('Results saved to {} '.format(filename))

                answer = input('Download {} images to output directory [Y/N]? '.format(len(copylist)))
                if answer.lower() == 'y':
                    self.download_from_csv_tblmetadata(self.outputDir, filename, roi, method)
                    downloaded = True

            if typOut == '2' or typOut == '3':
                tablename = self.create_tablename(roi, method)
                success = db.create_table_from_dict(tablename, instimg)
                if success:
                    success = db.insert_table_from_dict(tablename, instimg)
                    print('{} at creating Table {}'.format(success,tablename))
                    if success and not downloaded:
                        answer = input('Download {} images to output directory [Y/N]? '.format(len(copylist)))
                        if answer.lower() == 'y':
                            self.download_from_table_tblmetadata(db, self.outputDir, tablename, roi, method)

        except Exception as e:
            print ('The following exception occurred when query a local table.')
            print(e)
        return

    def download_local_table(self, db, roi, method):
        """
                   Download images from a local table.
                   It calls the appropriate function if the ids of the images are in a CSV or a table.

                   **Parameters**
                           *db* : An instance of the database class.
                           *output_dir* : The directory where the images will be stored.
                           *csv_filename* : Name of the csv file that contains the images ids.
                           *roi* : The roi shapefile to be queried
                           *method* : query method

               """
        try:
            sourcename = input("Enter CSV filename or tablename of images to download: ")
            if '.csv' in sourcename.lower():
                self.download_from_csv_tblmetadata(self.outputDir, sourcename, roi, method)
            else:
                self.download_from_table_tblmetadata(db, self.outputDir, sourcename, roi, method)
        except Exception as e:
            print('The following exception occurred when query a local table.')
            print(e)
        return

    def download_from_csv_tblmetadata(self, output_dir, csv_filename, roi, method):

        """
             Download images from a local table
             The id's of the images are contained in a csv file.
             It also outputs the list of downloaded images into a txt file. One image per line.

             **Parameters**
                     *db* : An instance of the database class.
                     *output_dir* : The directory where the images will be stored.
                     *csv_filename* : Name of the csv file that contains the images ids.
                     *roi* : The roi shapefile to be queried
                     *method* : query method

         """

        locations = []
        copied_locations = []
        with open(csv_filename, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                locations.append(row['location'])


        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        for filepath in locations:
            if os.path.exists(filepath):
                head_tail = os.path.split(filepath)
                new_location = os.path.join(output_dir, head_tail[1])
                shutil.copy(filepath, output_dir)
                copied_locations.append(new_location)
            else:
                print('File {} not found.'.format(filepath))

        self.save_filepaths(output_dir, roi, method, copied_locations)


    def download_from_table_tblmetadata(self, db, output_dir, tablename, roi, method):

        """
            Download images from a local table.
            The id's of the images are contained in a table.
            It also outputs the list of downloaded images into a txt file. One image per line.

            **Parameters**
                    *db* : An instance of the database class.
                    *output_dir* : The directory where the images will be stored.
                    *tablename* : Name of the table that contains the images ids.
                    *roi* : The roi shapefile to be queried
                    *method* : query method

        """

        try:
            copied_locations = []

            query = 'SELECT location FROM {}'.format(tablename)
            locations = db.execute_raw_sql_query(query)


            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            for tuple in locations:
                filepath = tuple[0]
                if os.path.exists(filepath):
                    head_tail = os.path.split(filepath)
                    new_location = os.path.join(output_dir, head_tail[1])
                    shutil.copy(filepath, output_dir)
                    copied_locations.append(new_location)
                else:
                    print('File {} not found.'.format(filepath))

            self.save_filepaths(output_dir, roi, method, copied_locations)
        except Exception as e:
            print('The following exception occurred when downloading images from eodms:')
            print(e)
        return


    def query_eodms(self, db, roi, roiDir, method, outputDir):
        """
            Calls the function _query_eodms which in turns call queryEODMSAPI to download images from EODMS.
            Saves the results of the query into a CSV file, a local table or both.
            It also downloads the images from the query, if the user selects that option.

            **Parameters**
                    *db* : An instance of the database class.
                    *roiDir* : Directory where the shapefile is located
                    *roi*: Name of the shapefile to be queried.
                    *method*: Name of the query method (EODMS)
                    *outputDir*: where the CSV will be saved.
        """

        # query from EODMS
        try:
            allison = False
            if allison:
                roiShpFile = os.path.join(self.roiDir, roi)
                queryParams = self.readShpFile(roiShpFile)
                records = self.queryEODMS(queryParams)
            else:
                collection = input('Enter collection to query or press enter for default "Radarsat1": ')
                if collection =='\n' or collection=='':
                   collection='Radarsat1'
                records = self.queryEODMS_MB(roiDir, roi, collection)

            # Options to output results
            print('Query completed. Write query results to: ')
            print('1: CSV file')
            print('2: Local table')
            print('3: Both')
            typOut = input('Enter your option (1,2,3): ')

            if typOut == '1' or typOut == '3':
                filename = self.create_filename(outputDir, roi, method, '.csv')
                db.exportDict_to_CSV(records, filename)
                print('Results saved to {}'.format(filename))


            if typOut == '2' or typOut == '3':
                tablename = self.create_tablename(roi, method)
                #tablename = 'EXAMPLE_EODMS'
                success = db.create_query_table(tablename, records)
                if success:
                    success = db.insert_query_table(tablename, records)
                    print('Table {} created {}'.format(tablename, success))

            if len(records)>0:
                submit_order = input("Would you like to order the images? [Y/N]")
                if submit_order.lower() == 'y':
                    self._order_to_eodms(records)
                    print('Images ordered to EODMS. Wait for confirmation email.')
        except Exception as e:
            print('The following exception occurred when querying images from eodms:')
            print(e)
        return


    def queryEODMS_MB(self, roiDir, roi, collection):
        """
            Calls EodmsAPI to download images from copernicus.

            **Parameters**

                *roiDir* : Directory where the shapefile is located
                *roi*: Name of the shapefile to be queried.
                *collection* : Collection parameter for EodmsAPI

            **Returns**
                *records* : Records returned by EodmsAPI
        """

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
        print ('ROI start date {} end date {}'.format(fromdate,todate))


        # 3. Query EODMS
        client = EodmsAPI(collection=collection)
        client.query(start=fromdate, end=todate, geometry=geojsonFilename)
        len(client.results)

        record_ids = client.results.to_dict()

        return record_ids

    def order_eodms(self, db):
        """
            Order images to eodms
                    **Parameters**
                            *db* : An instance of the database class.
        """
        try:
            sourcename = input("Enter CSV filename or tablename of images to order: ")
            if '.csv' in sourcename.lower():
                record_id = self.get_EODMS_ids_from_csv(sourcename)
            else:
                record_id = self.get_EODMS_ids_from_table(db, sourcename.lower())

            print('Ordering {} images: '.format(len(record_id)))
            print(record_id)
            submit_order = input("Would you like to order {} images? [Y/N]\t".format(len(record_id)))
            if submit_order.lower() == 'y':
                self._order_to_eodms(record_id)
                print('Images ordered to EODMS. Wait for confirmation email.')
        except Exception as e:
            print('The following exception occurred when ordering images from eodms.')
            print(e)
        return


    def download_images_from_eodms(self, output_dir):
        """
           Downloads images from eodms.
           It also outputs the list of downloaded images into a txt file. One image per line.

            **Parameters**

                *output_dir* : Directory where the images will be saved

         """
        try:
            source = input("Enter an order_item_id or a filename with a list of order_item_ids: ")
            ids = []
            if '.txt' in source.lower():
                with open(source) as file:
                    ids = file.readlines()
            else:
                ids.append(source)

            collection = input('Enter collection to download or press enter for default "Radarsat1": ')
            if collection == '\n' or collection == '':
                collection = 'Radarsat1'
            client = EodmsAPI(collection=collection)
            copied_locations = []
            for order_item_id in ids:
                order_item_id = order_item_id.replace('\n', '')
                try:
                    list_files = client.download(order_item_id, output_dir)
                    if len(list_files)==0:
                        print("\nOrder_item_id {} is *not* available to download. ".format(order_item_id))
                    else:
                        print ("\nOrder_item_id {} sucessfully downloaded. ".format(order_item_id))
                        copied_locations.append(list_files[0])
                except:
                    print ('Problem when downloading order_item_id {}'.format(order_item_id))

            if len(copied_locations)>0:
                self.save_filepaths(output_dir, '_', 'eodms', copied_locations)

        except Exception as e:
            print('The following exception occurred when downloading images from eodms:')
            print(e)
        return

    #Gets called from order_eodms
    def get_EODMS_ids_from_csv(self, filename):
        """
            Obtain the images ids from a CSV file which contains the results of a query to EODMS.

                         **Parameters**

                             *filename* : The CSV file that contains the images ids.

                         **Returns**
                             *record_id* : A list of images id from EODMS
        """
        record_id = []
        with open(filename, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                record_id.append(row['EODMS RecordId'])

        return record_id

    #Gets called from order_eodms
    def get_EODMS_ids_from_table(self, db, tablename):
        """
                       Obtain the images ids from a local table which contains the results of a query to EODMS.

                         **Parameters**
                             *db* : An instance of the database class
                             *tablename* : The table that contains the images ids.

                         **Returns**
                             *record_id* : A list of images id from EODMS
        """
        query = 'SELECT eodms_recordid FROM {}'.format(tablename)
        result = db.execute_raw_sql_query(query)
        record_id = []
        for record in result:
            record_id.append(record[0])
        return record_id


    #Gets called from order_eodms
    def _order_to_eodms(self, record_ids):
        """
                 Order images to EODMS

                     **Parameters**

                         *record_ids* : The list of records_ids of the images to order. One image per item in record_ids.

                 """

        #eodms = ['Radarsat1', 'Radarsat2', 'RCMImageProducts', 'NAPL', 'PlanetScope']
        collection = input('Enter collection to order or press enter for default "Radarsat1": ')
        if collection == '\n' or collection == '':
            collection = 'Radarsat1'
        client = EodmsAPI(collection=collection)
        order_id = client.order(record_ids)
        print('Order {} submitted. Wait for confirmation email.'.format(order_id))


    def query_sentinel(self, db, roi, roiDir, method, outputDir):

        """
            Calls the function _query_sentinel which in turns call SentinelAPI to download images from copernicus.
            Saves the results of the query into a CSV file, a local table or both.
            It also downloads the images from the query, if the user selects that option.

            **Parameters**
                    *db* : An instance of the database class.
                    *roiDir* : Directory where the shapefile is located
                    *roi*: Name of the shapefile to be queried.
                    *method*: Name of the query method (SENTINEL)
                    *outputDir*: where the CSV will be saved.
        """

        satellite = input('Enter satellite to query or press enter for default "Sentinel-2": ')
        if satellite == '\n' or satellite == '':
            satellite = 'Sentinel-2'

        product = input('Enter product type or press enter for "None": ')
        if product == '\n' or product == '':
            product = None

        sensoroperationalmode = input('Enter sensor type ("SM", "IW", "EW", "WV") or press enter for None: ')
        if sensoroperationalmode == '\n' or sensoroperationalmode == '':
            sensoroperationalmode = None

        records = self._query_sentinel(roiDir, roi, satellite, product, sensoroperationalmode)
        if len(records)==0:
            print ('No images were found for the Shapefile and parameters selected.')
            return

        # Options to output results
        print('Query completed. Write query results to: ')
        print('1: CSV file')
        print('2: Local table')
        print('3: Both')
        typOut = input('Enter your option (1,2,3): ')

        if typOut == '1' or typOut == '3':
            filename = self.create_filename(outputDir, roi, method, '.csv')
            db.exportDict_to_CSV(records, filename)
            print('File saved to {}'.format(filename))

        if typOut == '2' or typOut == '3':
            tablename = self.create_tablename(roi, method)
            #tablename = 'EXAMPLE_SENTINEL'
            records_dict = records.to_dict()
            success = db.create_query_table(tablename, records_dict)
            if success:
                success = db.insert_query_table(tablename, records_dict)
                print('Table {} created {}'.format(tablename, success))

        answer = input("Would you like to download {} images? [Y/N]\t".format(len(records)))
        if answer == True:
            self._download_images_from_sentinel(records, self.outputDir)

        return


    def download_images_from_sentinel(self, db, outputDir):
        """
                 Call the procedure to download images from Copernicus.
                 The records of images can come either from a CSV or from a table into the database.

                     **Parameters**

                         *db* : An instance of the Database class.
                         *outputDir* : Directory where the csv results will be saved.

                 """
        try:
            sourcename = input("Enter CSV filename or tablename of images to download: ")
            if '.csv' in sourcename.lower():
                records = self.get_Sentinel_ids_from_csv(sourcename)
            else:
                records = self.get_Sentinel_ids_from_table(db, sourcename.lower())

            self._download_images_from_sentinel(records, outputDir)

        except Exception as e:
            print('The following exception occurred when downloading images from Copernicus:')
            print(e)
        return

    #Gets called from query_sentinel
    def _query_sentinel(self, roiDir, roi, satellite, product=None, sensoroperationalmode=None):
        """
            Calls SentinelAPI to download images from copernicus.

            **Parameters**

                    *roiDir* : Directory where the shapefile is located
                    *roi*: Name of the shapefile to be queried.
                    *satellite* : Satellite parameter for SentinelAPI
                    *product*: product parameter for SentinelAPI (optional)
                    *sensoroperationalmode* : sensoroperationalmode parameter for SentinelAPI (optional)

            **Returns**
                    *records* : Records returned by SentinelAPI
        """
        try:
            records=[]
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
            print('ROI start date {} end date {}'.format(fromdate, todate))

            # 3. Query Sentinel
            footprint = geojson_to_wkt(read_geojson(geojsonFilename))
            username = input("Enter your Copernicus username: ")
            password = getpass.getpass("Enter your Copernicus password: ")
            api = SentinelAPI(username, password, 'https://scihub.copernicus.eu/dhus')

            products = api.query(footprint,
                                 date=(fromdate_obj, todate_obj),
                                 platformname=satellite,
                                 processinglevel='Level-2A',
                                 producttype=product,
                                 sensoroperationalmode=sensoroperationalmode)
            records = api.to_dataframe(products)

            #products = api.query(footprint,
            #                     date=('20100101', '20201230'),
            #                     platformname=satellite,
            #                     processinglevel='Level-2A',
            #                     producttype=product)
            #records = api.to_dataframe(products)
        except Exception as e:
            print('The following exception occurred when querying SentinelAPI:')
            print(e)

        return records


    # Called from download_images_from_sentinel
    def get_Sentinel_ids_from_csv(self, filename):
        """
                 Obtain the images uuids from a CSV file which contains the results of a query to Sentinel.

                     **Parameters**

                         *filename* : The CSV file that contains the images ids.

                     **Returns**
                         *records* : A list of uuids per image
                 """
        try:
            records = []
            with open(filename, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # records.append(row['uuid'])
                    records.append({"title": row['title'], "uuid": row['uuid']})
                    # print(records)
        except:
            print('An exception occurred when opening the file. Please check file location.')

        return records


    # Called from download_images_from_sentinel
    def get_Sentinel_ids_from_table(self, db, tablename):
        """
            Obtain the images uuids from a local table which contains the results of a query to Sentinel.

                **Parameters**
                        *db* : An instance of the database class.
                        *tablename* : The tablename that contains the images ids.

                **Returns**
                        *records* : A list of uuids per image
        """
        try:
            query = 'SELECT title, uuid FROM {}'.format(tablename)
            result = db.execute_raw_sql_query(query)
            records = []
            for record in result:
                records.append({"title": record[0], "uuid": record[1]})
        except:
            print('An exception occurred when querying {}.'.format(tablename))

        return records


    #Called from download_images_from_sentinel
    def _download_images_from_sentinel(self, records, output_dir):

        """
                 Downloads an image per record from Copernicus and stores them into output_dir.
                 It also outputs the list of downloaded images into a txt file. One image per line.

                     **Parameters**

                         *records* : The list of records to donwload.
                         *output_dir* : Directory where the csv results will be saved.

                 """
        try:
            username = input("Enter your Copernicus username: ")
            password = getpass.getpass("Enter your Copernicus password: ")
            api = SentinelAPI(username, password, 'https://scihub.copernicus.eu/dhus')
        except:
            print('An exception occurred when accessing Copernicus. Please check username and/or password.')
            return


        copied_locations = []
        offline_uuids = []
        for record in records:
         try:
            id = record['uuid']
            title = record['title']
            if title!='':
                is_online = api.is_online(id)
                if is_online:
                    dict = api.download(id,output_dir)
                    print("Title {} sucessfully downlaoded. ".format(title))
                    copied_locations.append(dict['path'])
                else:
                    print(title + ' is offline - Retry later.')
                    offline_uuids.append(id)
                    api.download(id, output_dir)

         except:
            print ('Try next file.')

        if len(offline_uuids)>0:
            print ('There were {} offline images. Retry to download thme later.'.format(len(offline_uuids)))

        if len(copied_locations)>0:
            self.save_filepaths(output_dir, '_', 'sentinel', copied_locations)


    def execute_raw_query(self, db, outputDir):
        """
                 Call database class to execute a raw sql query passed as a parameter. Saves the results of the query
                 into a csv file.

                     **Parameters**

                         *db* : An instance of the Database class.
                         *outputDir* : Directory where the csv results will be saved.
                 """
        try:
            filename = input('Enter file with SQL query: ')
            with open(filename) as file:
                sql_query = file.read()
                qryOutput = db.execute_raw_sql_query(sql_query)

            outputName = self.create_filename(outputDir, '', 'SQL', '.csv')
            db.exportDict_to_CSV(qryOutput, outputName)
            print('Results saved to {}'.format(outputName))
        except Exception as e:
            print('The following exception occurred when executing a SQL query:')
            print(e)
        return