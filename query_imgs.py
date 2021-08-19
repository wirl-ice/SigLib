import psycopg2
import psycopg2.extras
import os
import sys
from configparser import RawConfigParser
from datetime import datetime, timedelta
import pandas as pd
import shutil
import csv
import geopandas
from eodms_api_client import EodmsAPI
import json
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt
import getpass

def read_config():

    cfg = os.path.expanduser((sys.argv[1]))
    config = RawConfigParser()  # Needs to be tested for python2 compatibility 
    config.read(cfg)
    vectDir = str(os.path.abspath(os.path.expanduser(config.get("Directories", "vectDir"))))
    outDir = str(os.path.abspath(os.path.expanduser(config.get("Directories", "outDir"))))

    dbName = str(config.get("Database", "db"))
    dbHost = str(config.get("Database", "host"))
    table_to_query = str(config.get("Database", "table"))

    roi = str(config.get('MISC', "roi"))
    roiProjSRID = str(config.get('MISC', "roiProjSRID"))
    spatialrel = str(config.get('MISC', "spatialrel"))

    return vectDir, outDir, dbName, dbHost, table_to_query, roi, roiProjSRID, spatialrel



def connect_to_db(dbname, port, host, user=None):

    password = "dbpass"

    if user == None:
        connectionSetUp = "dbname=" + dbname + " port=" + port + " host=" + host
    else:
        connectionSetUp = "dbname=" + dbname + " user=" + user + " password=" + password + " port=" + port + " host=" + host

    connection = psycopg2.connect(connectionSetUp)

    return connection


def qrySelectFromAvailable(connection, roi, selectFrom, spatialrel, srid):
        """
        Given a roi table name (with polygons, from/todates), determine the scenes that cover the area
        from start (str that looks like iso date) to end (same format).

        Eventually include criteria:
            subtype - a single satellite name: ALOS_AR, RADAR_AR, RSAT2_AR (or ANY)
            beam - a beam mode

        Returns a list of images+inst - the bounding box

        **Parameters**

            *roi*        : region of interest table in the database

            *spatialrel* : spatial relationship (i.e. ST_Contains or ST_Intersect).  Does the image contain the ROI polygon or just intersect with it?

            *srid*       : srid of desired projecton

            *selectFrom* : table in the database to find the scenes

        **Returns**

            *copylist* : a list of image catalog ids

            *instimg*  : a list of each instance and the images that correspond
        """

        assert spatialrel.lower() == 'st_contains' or spatialrel.lower() == 'st_intersects'

        curs = connection.cursor()

        # Check if local table to query exists
        # Tablenames are stored in lower case and are case sensistive
        curs.execute("select exists(select * from information_schema.tables where table_name=%s)",
                     (selectFrom.lower(),))
        table_exists = curs.fetchone()[0]
        if not table_exists:
            return None, None

        # Check if ROI table exists
        curs.execute("select exists(select * from information_schema.tables where table_name=%s)", (roi.lower(),))
        table_exists = curs.fetchone()[0]
        if not table_exists:
            return None, None

        curs.execute('SELECT inst, fromdate, todate FROM ' + roi + ';')

        instances = curs.fetchall()

        n_poly = len(instances)

        instimg = [] # make a list of dictionaries to create the instimg table
        copyfiles = [] # make a list to contain all the files to copy

        # The query is set up to take the most recent addition to the archive of
            # the SAME file - ie the date/time and satellite match fully, however, it
            # is desirable to match on more than one image as the time span allows.
        #Note DO NOT do this comparison in WGS84-lat.lon  - use a projection)

        for i in range(n_poly): # for each polygon in tblROI, get images that

            inst = instances[i][0]

            fromdate = instances[i][1] + timedelta(seconds=1)
            fromdate = fromdate.strftime('%Y-%m-%d')

            #allow for truncation errors
            todate = instances[i][2] + timedelta(seconds=1)
            todate = todate.strftime('%Y-%m-%d')   #%H:%M:%S'

            param = {'inst': inst, 'fromdate' : fromdate, 'todate' : todate, 'srid' : int(srid)}
            print('ROI start date: {}  end date: {}'.format(fromdate, todate))

            if selectFrom == 'tblcisarchive':
                sql1 = """SELECT DISTINCT ON (substring("File Name", 1, 27)) 
                "File Name", "File Path", "SubType", "Valid Time", "Catalog Id" """
                sql4 = 'AND "Valid Time" >= %(fromdate)s '
                sql5 = 'AND "Valid Time" <= %(todate)s '
                sql9 = """ORDER BY substring("File Name", 1, 27), "File Name" DESC"""

            else:
                sql1 = """SELECT DISTINCT ON (substring("granule", 1, 27)) 
                "granule", "location", "sattype", "beam", "acdatetime" """
                sql4 = 'AND "acdatetime" >= %(fromdate)s '
                sql5 = 'AND "acdatetime" <= %(todate)s '
                sql9 = """ORDER BY substring("granule", 1, 27), "granule" DESC"""

            sql2 = 'FROM ' + selectFrom +', ' +roi+ ' '
            sql3 = 'WHERE ' + roi + '.inst = %(inst)s '
            sql6 = 'AND ' + spatialrel + ' '
            sql7 = '(ST_Transform('+selectFrom+'.geom, %(srid)s), ST_Transform('
            sql8 =  roi+'.geom, %(srid)s)) '
            qry = sql1 + sql2 + sql3 + sql4 + sql5 + sql6 + sql7 + sql8+ sql9

            curs.execute(qry,param)
            connection.commit()
            rows = curs.fetchall()

            for i in range(len(rows)):

                if selectFrom == 'tblcisarchive':

                    granule= rows[i][0]
                    catid= rows[i][4]
                    acTime= rows[i][3]

                    copyfiles.append(catid)

                    instimg.append({"inst": inst, "granule": granule, "catid": catid, "time": acTime})

                else:
                    granule = rows[i][0]
                    location = rows[i][1]
                    acTime = rows[i][4]

                    copyfiles.append(location)

                    instimg.append({"inst" : inst, "granule" : granule, "location" : location, "time": acTime})

        # make sure there are no repeat occurrances of the files to copy
        copylist = sorted(dict.fromkeys(copyfiles))
        copylist.reverse()
        return copylist, instimg


def execute_raw_sql_query(connection, sql_query):
        """
           Executes a sql query passed as a parameter.

               **Parameters**

                   *sql_query* : The sql_query to be executed

               **Returns**
                   *result* : The result of the sql query or empty string if failed.
           """

        result = ''
        curs = connection.cursor()
        try:
            curs.execute(sql_query)
            result = curs.fetchall()

        except Exception as e:
            print(e)
            connection.rollback()
        return result



 # Creates a table that will contain records from a query
def create_table_from_dict (connection, table_name, list):

        """
                Creates a table to store results from a query. The structure of the table follows the information from list.

                    **Parameters**

                        *table_name* : The name of the table to be created.
                        *list*: A list of records from where the table structure (name and data type of columns) will be obtained.


                    **Returns**
                        *success* : True if the records were inserted into the table
                """

        success = True
        curs = connection.cursor()

        try:
            sql_query = 'DROP TABLE IF EXISTS {}'.format(table_name)
            curs.execute(sql_query)
            connection.commit()
        except Exception as e:
            print(e)
            connection.rollback()
            success = False

        if len(list) > 0:
            dict = list[0]


        # Get columns and its types
        columns = tuple(dict)
        columns_name = []
        total_columns = len(dict)
        types = []

        for i in range(total_columns):
            name = columns[i]
            name = name.replace(' ', '_')
            name = name.replace('(', '')
            name = name.replace(')', '')
            columns_name.append(name)
            value = dict[name]
            types.append(type(value).__name__)

        sql_types = []

        for t in types:
            if t == 'int':
                sql_types.append('integer')
            elif t == 'str':
                sql_types.append('varchar')
            elif t == 'float':
                sql_types.append('double precision')
            elif t.lower() == 'polygon':
                sql_types.append('varchar')
            elif t.lower() =='datetime':
                sql_types.append(' timestamp')
            else:
                sql_types.append(t)

        sql_query = 'CREATE TABLE {} ('.format(table_name)
        for i in range(0, total_columns - 1):
            sql_query = sql_query + columns_name[i] + ' ' + sql_types[i] + ', '

        sql_query = sql_query + columns_name[total_columns - 1] + ' ' + sql_types[total_columns - 1] + ");"

        try:
            curs.execute(sql_query)
            connection.commit()
        except Exception as e:
            print(e)
            connection.rollback()
            success = False

        return success

# Inserts records into a insert_table
# The records are in a different format as in insert_query_table
def insert_table_from_dict(connection, table_name, list):
        """
               Inserts records into a table to store results from a query. The structure of the table follows the information from records.

                   **Parameters**

                       *table_name* : The name of the table to be created.
                       *list*: A list of records from where the table structure (name and data type of columns) will be obtained.


                   **Returns**
                       *success* : True if the records were inserted into the table
               """

        try:
            success = True

            if len(list) > 0:
                dict = list[0]

                # Get columns and its types
            columns = tuple(dict)
            columns_name = []
            total_columns = len(dict)

            for i in range(total_columns):
                name = columns[i]
                name = name.replace(' ', '_')
                name = name.replace('(', '')
                name = name.replace(')', '')
                columns_name.append(name)

            total_rows = len(list)

            for row in range(0, total_rows):
                sql_query = 'INSERT INTO {} ('.format(table_name)
                for i in range(0, total_columns - 1):
                    sql_query = sql_query + columns_name[i] + ', '

                sql_query = sql_query + columns_name[total_columns - 1] + ') VALUES ('

                values = []
                dict = list[row]
                for i in range(total_columns - 1):
                        if (type(dict[columns_name[i]]).__name__) == 'Polygon':
                            v = dict[columns_name[i]]
                            p = v.wkt
                            values.append(p)
                        else:
                            values.append(dict[columns_name[i]])
                        sql_query = sql_query + '%s, '

                if (type(dict[columns_name[total_columns-1]]).__name__) == 'Polygon':
                    v = dict[columns_name[total_columns-1]]
                    p = v.wkt
                    values.append(p)
                else:
                    values.append(dict[columns_name[total_columns-1]])

                sql_query = sql_query + '%s)'

                curs = connection.cursor()

                try:
                    curs.execute(sql_query, values)
                    connection.commit()
                except Exception as e:
                    print(e)
                    connection.rollback()
                    success = False
        except Exception as e:
            print(e)
            connection.rollback()
            success = False

        return success


   #Creates a table that will contain records from a query


def create_query_table(connection, table_name, records):
        """
               Creates a table to store results from a query. The structure of the table follows the information from records.

                   **Parameters**

                       *table_name* : The name of the table to be created.
                       *records*: A list of records from where the table structure (name and data type of columns) will be obtained.


                   **Returns**
                       *success* : True if the records were inserted into the table
               """
        try:
            success = True
            curs = connection.cursor()

            try:
                sql_query = 'DROP TABLE IF EXISTS {}'.format(table_name)
                curs.execute(sql_query)
                connection.commit()
            except Exception as e:
                print(e)
                connection.rollback()
                success = False

            # Get columns and its types
            columns = tuple(records)
            columns_name = []
            total_columns = len(records)
            types = []

            for i in range(total_columns):
                name = columns[i]
                name = name.replace(' ', '_')
                name = name.replace('(', '')
                name = name.replace(')', '')
                columns_name.append(name)
                if (type(records[columns[i]]).__name__) == 'dict':
                    dict = records[columns[i]]
                    dict_cols = tuple(dict)
                    types.append(type(dict[dict_cols[0]]).__name__)

            sql_types = []

            for t in types:
                if t == 'int':
                    sql_types.append('integer')
                elif t == 'str':
                    sql_types.append('varchar')
                elif t == 'float':
                    sql_types.append('double precision')
                elif t.lower() == 'polygon':
                    sql_types.append('varchar')
                else:
                    sql_types.append(t)

            sql_query = 'CREATE TABLE {} ('.format(table_name)
            for i in range(0, total_columns - 1):
                sql_query = sql_query + columns_name[i] + ' ' + sql_types[i] + ', '

            sql_query = sql_query + columns_name[total_columns - 1] + ' ' + sql_types[total_columns - 1] + ");"

            try:
                curs.execute(sql_query)
                connection.commit()
            except Exception as e:
                print(e)
                connection.rollback()
                success=False

        except Exception as e:
            print(e)
            success = False

        return success


    #Inserts records into a table.
    #The records are in a different format as in insert_table_from_dict.

def insert_query_table(connection, table_name,records):
        """
               Inserts records in a table_name.

                   **Parameters**

                       *table_name* : The name of the table in which the records will be inserted.


                   **Returns**
                       *success* : True if the records were inserted into the table
               """

        try:
            success = True
            #Get columns and its types
            columns = tuple(records)
            columns_name = []
            total_columns = len(records)

            for i in range(total_columns):
                name = columns[i]
                name = name.replace(' ', '_')
                name = name.replace('(', '')
                name = name.replace(')', '')
                columns_name.append(name)


            total_rows = len(records[columns[0]])

            for row in range(0, total_rows):
                sql_query = 'INSERT INTO {} ('.format(table_name)
                for i in range(0, total_columns-1):
                    sql_query = sql_query + columns_name[i] + ', '

                sql_query = sql_query + columns_name[total_columns-1] + ') VALUES ('

                values = []
                for i in range(total_columns-1):
                    if (type(records[columns[i]]).__name__) == 'dict':
                        dict = records[columns[i]]
                        dict_cols = tuple(dict)
                        if (type(dict[dict_cols[row]]).__name__) == 'Polygon':
                            v = dict[dict_cols[row]]
                            p = v.wkt
                            values.append(p)
                        else:
                            values.append(dict[dict_cols[row]])
                        sql_query = sql_query +  '%s, '


                dict = records[columns[total_columns - 1]]
                dict_cols = tuple(dict)
                if (type(dict[dict_cols[row]]).__name__) == 'Polygon':
                    v = dict[dict_cols[row]]
                    p = v.wkt
                    values.append(p)
                else:
                    values.append(dict[dict_cols[row]])

                sql_query = sql_query + '%s)'

                curs = connection.cursor()

                try:
                    curs.execute(sql_query, values)
                    connection.commit()
                except Exception as e:
                    print(e)
                    connection.rollback()
                    success = False
        except Exception as e:
            success = False
            print(e)
            connection.rollback()

        return success




def exportDict_to_CSV(qryOutput, outputName):
        """
        Given a dictionary of results from the database and a filename puts all the results
        into a csv with the filename outputName

        **Parameters**

            *qryOutput*  : output from a query - needs to be a tupple - numpy data and list of column names

            *outputName* : the file name
        """

        tmp = pd.DataFrame.from_dict(qryOutput)
        for col in tmp.columns:
            if tmp[col].dtype == 'O' or tmp[col].dtype == 'S':
                stripped = tmp[col].str.rstrip()  # somehow there are plenty of spaces in some cols
                if not stripped.isnull().all():  # sometimes this goes horribly wrong (datetimes)
                    tmp[col] = stripped
        tmp.to_csv(outputName, index=False)


def create_filename(outputDir, roi, method, extension):

        now = datetime.now()
        dt_string = now.strftime("%d-%m-%Y %H:%M:%S")
        filename = roi + '_' + method + '_' + dt_string + extension
        fullpath = os.path.join(outputDir,filename)
        return fullpath

def create_tablename(roi, method):

        now = datetime.now()
        dt_string = now.strftime("%d%m%Y_%H%M")
        tablename = roi + '_' + method + '_' + dt_string
        return tablename


def save_filepaths(output_dir, roi, method, copied_locations):
    try:
        outputName = create_filename(output_dir, roi, method + '_PATHS', '.txt')
        with open(outputName, "w") as output:
            for location in copied_locations:
                output.write(location + "\n")

        print('\nList of downloaded images: {} '.format(outputName))
    except Exception as e:
        print('The following exception occurred when saving the list of downloaded images to a txt file.')
        print(e)
    return


def download_from_csv_tblmetadata(output_dir, csv_filename, roi, method):
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

    save_filepaths(output_dir, roi, method, copied_locations)




def download_from_table_tblmetadata(connection, output_dir, tablename, roi, method):

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
            locations = execute_raw_sql_query(connection, query)


            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            for tuple in locations:
                filepath = tuple[0]
                if os.path.exists(filepath):
                    head_tail = os.path.split(filepath)
                    new_location = os.path.join(output_dir,head_tail[1])
                    shutil.copy(filepath,output_dir)
                    copied_locations.append(new_location)
                else:
                    print('File {} not found.'.format(filepath))

            save_filepaths(output_dir, roi, method, copied_locations)
        except Exception as e:
            print('The following exception occurred when downloading images from eodms:')
            print(e)
        return



def query_local_table(connection, outputDir, roi, table_to_query, spatialrel, roiSRID, method):
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
            copylist, instimg = qrySelectFromAvailable(connection, roi, table_to_query, spatialrel, roiSRID)

            #Options to output results
            print('Query completed. Write query results to: ')
            print('1: CSV file')
            print('2: Local table')
            print('3: Both')
            typOut = input('Enter your option (1,2,3): ')

            downloaded=False
            if typOut == '1' or typOut == '3':
                filename = create_filename(outputDir, roi, table_to_query, '.csv')
                exportDict_to_CSV(instimg, filename)
                print('Results saved to {} '.format(filename))

                answer = input('Download {} images to output directory [Y/N]? '.format(len(copylist)))
                if answer.lower() == 'y':
                    download_from_csv_tblmetadata(outputDir, filename, roi, method)
                    downloaded = True

            if typOut == '2' or typOut == '3':
                tablename = create_tablename(roi, method)
                success = create_table_from_dict(connection,tablename, instimg)
                if success:
                    success = insert_table_from_dict(connection, tablename,  instimg)
                    print('{} at creating Table {}'.format(success,tablename))
                    if success and not downloaded:
                        answer = input('Download {} images to output directory [Y/N]? '.format(len(copylist)))
                        if answer.lower() == 'y':
                            download_from_table_tblmetadata(connection, outputDir, tablename, roi, method)

        except Exception as e:
            print ('The following exception occurred when query a local table.')
            print(e)
        return


def download_local_table(connection, outputDir, roi, method):
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
                download_from_csv_tblmetadata(outputDir, sourcename, roi, method)
            else:
                download_from_table_tblmetadata(connection, outputDir, sourcename, roi, method)
        except Exception as e:
            print('The following exception occurred when query a local table.')
            print(e)
        return


#Gets called from order_eodms
def get_EODMS_ids_from_csv(filename):
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
def get_EODMS_ids_from_table(connection, tablename):
        """
                       Obtain the images ids from a local table which contains the results of a query to EODMS.

                         **Parameters**
                             *db* : An instance of the database class
                             *tablename* : The table that contains the images ids.

                         **Returns**
                             *record_id* : A list of images id from EODMS
        """
        query = 'SELECT eodms_recordid FROM {}'.format(tablename)
        result = execute_raw_sql_query(connection, query)
        record_id = []
        for record in result:
            record_id.append(record[0])
        return record_id


#Gets called from order_eodms
def _order_to_eodms(record_ids):
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



def order_eodms(connection):
        """
            Order images to eodms
                    **Parameters**
                            *db* : An instance of the database class.
        """
        try:
            sourcename = input("Enter CSV filename or tablename of images to order: ")
            if '.csv' in sourcename.lower():
                record_id = get_EODMS_ids_from_csv(sourcename)
            else:
                record_id = get_EODMS_ids_from_table(connection, sourcename.lower())

            print('Ordering {} images: '.format(len(record_id)))
            print(record_id)
            submit_order = input("Would you like to order {} images? [Y/N]\t".format(len(record_id)))
            if submit_order.lower() == 'y':
                _order_to_eodms(record_id)
                print('Images ordered to EODMS. Wait for confirmation email.')
        except Exception as e:
            print('The following exception occurred when ordering images from eodms.')
            print(e)
        return


def download_images_from_eodms(output_dir):
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
                save_filepaths(output_dir, '_', 'eodms', copied_locations)

        except Exception as e:
            print('The following exception occurred when downloading images from eodms:')
            print(e)
        return


def queryEODMS_MB(roiDir, roi, collection):
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


def query_eodms(connection, roi, roiDir, method, outputDir):
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
            collection = input('Enter collection to query or press enter for default "Radarsat1": ')
            if collection =='\n' or collection=='':
                collection='Radarsat1'
            records = queryEODMS_MB(roiDir, roi, collection)

            # Options to output results
            print('Query completed. Write query results to: ')
            print('1: CSV file')
            print('2: Local table')
            print('3: Both')
            typOut = input('Enter your option (1,2,3): ')

            if typOut == '1' or typOut == '3':
                filename = create_filename(outputDir, roi, method, '.csv')
                exportDict_to_CSV(records, filename)
                print('Results saved to {}'.format(filename))


            if typOut == '2' or typOut == '3':
                tablename = create_tablename(roi, method)
                success = create_query_table(connection, tablename, records)
                if success:
                    success = insert_query_table(connection, tablename, records)
                    print('Table {} created {}'.format(tablename, success))

            if len(records)>0:
                submit_order = input("Would you like to order the images? [Y/N]")
                if submit_order.lower() == 'y':
                    _order_to_eodms(records)
                    print('Images ordered to EODMS. Wait for confirmation email.')
        except Exception as e:
            print('The following exception occurred when querying images from eodms:')
            print(e)
        return


def get_Sentinel_ids_from_csv(filename):
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
def get_Sentinel_ids_from_table(connection, tablename):
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
            result = execute_raw_sql_query(connection, query)
            records = []
            for record in result:
                records.append({"title": record[0], "uuid": record[1]})
        except:
            print('An exception occurred when querying {}.'.format(tablename))

        return records


#Called from download_images_from_sentinel
def _download_images_from_sentinel(records, output_dir):

        """
                 Downloads an image per record from Copernicus and stores them into output_dir.
                 It also outputs the list of downloaded images into a txt file. One image per line.

                     **Parameters**

                         *records* : The list of records to download.
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
            save_filepaths(output_dir, '_', 'sentinel', copied_locations)


def download_images_from_sentinel(connection, outputDir):
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
                records = get_Sentinel_ids_from_csv(sourcename)
            else:
                records = get_Sentinel_ids_from_table(connection, sourcename.lower())

            total_records = len(records)
            answer = input("Do you want to download {} images [Y/N] ".format(total_records))
            if answer.lower() == 'y':
                _download_images_from_sentinel(records, outputDir)

        except Exception as e:
            print('The following exception occurred when downloading images from Copernicus:')
            print(e)
        return


#Gets called from query_sentinel
def _query_sentinel(roiDir, roi, satellite, product=None, sensoroperationalmode=None):
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


def query_sentinel(connection, roi, roiDir, method, outputDir):

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

        records = _query_sentinel(roiDir, roi, satellite, product, sensoroperationalmode)
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
            filename = create_filename(outputDir, roi, method, '.csv')
            exportDict_to_CSV(records, filename)
            print('File saved to {}'.format(filename))

        if typOut == '2' or typOut == '3':
            tablename = create_tablename(roi, method)
            #tablename = 'EXAMPLE_SENTINEL'
            records_dict = records.to_dict()
            success = create_query_table(connection, tablename, records_dict)
            if success:
                success = insert_query_table(connection, tablename, records_dict)
                print('Table {} created {}'.format(tablename, success))

        answer = input("Would you like to download {} images? [Y/N]\t".format(len(records)))
        if answer == True:
            _download_images_from_sentinel(records, outputDir)

        return



def execute_raw_query(connection, outputDir):
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
                qryOutput = execute_raw_sql_query(connection, sql_query)

            outputName = create_filename(outputDir, '', 'SQL', '.csv')
            exportDict_to_CSV(qryOutput, outputName)
            print('Results saved to {}'.format(outputName))
        except Exception as e:
            print('The following exception occurred when executing a SQL query:')
            print(e)
        return

def query_menu(connection, roiDir, outputDir, table_to_query, roi, roiSRID, spatialrel):
    query_methods = {'1': 'metadata', '2': 'download_metabdata', '3': 'cis', '4': 'EODMS', '5': 'ORDER_EODMS',
                     '6': 'DOWNLOAD_EODMS', '7': 'SENTINEL', '8': 'DOWNLOAD_SENTINEL', '9': 'RAW_SQL', '0': 'EXIT'}
    print("Available Query Methods:\n")
    print("1: {}: Query".format(table_to_query))
    print("2: {}: Download".format(table_to_query))
    print("3: CIS Archive (WIRL users only)")
    print("4: EODMS: Query")
    print("5: EODMS: Order")
    print("6: EODMS: Download")
    print("7: Copernicus: Query")
    print("8: Copernicus: Download")
    print("9: Execute Raw Sql Query")
    print("0: Exit")
    ans = input("Please select the desired query method (1,2,3,4,5,6,7,8,9):\t")
    method = query_methods[ans]
    if method == 'metadata':
        query_local_table(connection, outputDir, roi, table_to_query, spatialrel, roiSRID, method)
        return
    elif method == 'download_metabdata':
        download_local_table(connection, outputDir, roi, method)
        return
    elif method == 'cis':
        query_local_table(connection, outputDir, roi, 'tblcisarchive', spatialrel, roiSRID, method)
        return
    elif method == 'EODMS':  # set up to query RSAT-1 data
        query_eodms(connection, roi, roiDir, method, outputDir)
    elif method == 'ORDER_EODMS':
        order_eodms(connection)
    elif method == 'SENTINEL':
        query_sentinel(connection, roi, roiDir, method, outputDir)
    elif method == 'DOWNLOAD_EODMS':
        download_images_from_eodms(outputDir)
    elif method == 'DOWNLOAD_SENTINEL':
        download_images_from_sentinel(connection, outputDir)
    elif method == 'RAW_SQL':
        execute_raw_query(connection, outputDir)
    elif method == 'EXIT':
        return
    else:
        print ("Valid Query Method not selected, cannot complete task.")
    return


def run():

    port = '5432'
    vectDir, outDir, dbName, dbHost, table_to_query, roi, roiProjSRID, spatialrel = read_config()
    connection = connect_to_db(dbName, port, dbHost)
    menu_option = query_menu(connection, vectDir, outDir, table_to_query, roi, roiProjSRID, spatialrel)
    
    
if __name__ == "__main__":
    run()
   
    
