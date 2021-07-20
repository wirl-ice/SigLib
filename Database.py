# -*- coding: utf-8 -*-
"""
**Database.py**

**Created on** Tue Feb 12 23:12:13 2013 **@author:** Cindy Lopes

This module creates an instance of class Database and connects to a database to
create, update and query tables.

Tables of note include:

**table_to_query** - a table that contains metadata that is gleaned by a directory scan

**roi_tbl** - a table with a region of interest (could be named something else)

**trel_roiinst_con** or _int - a relational table that results from a spatial query

**tblArchive** - a copy of the metadata from the CIS image archive

**Modified on** 23 May 14:43:40 2018 **@reason:** Added logging functionality **@author:** Cameron Fitzpatrick

"""
#WIRL SRID's: lcc = 96718  aea = 999  wgs84 = 4326

import psycopg2
import psycopg2.extras
from psycopg2.extensions import AsIs
import os
import datetime
import numpy
import scipy.stats as stats
import glob
import getpass
import logging
import sys
import pandas as pd

class Database:
    """
    This is the Database class for each database connection.
    
    Creates a connection to the specified database.  You can connect as a specific user or 
        default to your own username, which assumes you have privileges and a password stored in your  ~/.pgpass file
        
        Note: if you have any issues with a bad query, send a rollback to the database to reset the connection.
        >>>>db.connection.rollback()
        
        **Parameters**
        
            *dbname*   : database name 
               
            *user*     : user with read or write (as required) access on the specified databse (eg. postgres user has read/write access to all databses)
            
            *password* : password to go with user  
              
            *port*     : server port (i.e. 5432) 
               
            *host*     : hostname of postgres server
    """
    
    def __init__(self, table_to_query, dbname, loghandler=None, user=None, password=None, port='5432', host='localhost'):
        
        if loghandler != None:
            self.loghandler = loghandler             #Logging setup if loghandler sent, otherwise, set up a console only logging system
            self.logger = logging.getLogger(__name__)
            self.logger.addHandler(loghandler)
            self.logger.propagate = False
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger = logging.getLogger(__name__)                        
            self.logger.setLevel(logging.DEBUG)
            self.logger.addHandler(logging.StreamHandler())
        
        self.dbName = dbname
        self.port = port
        self.host = host
        self.user = user
        self.password = "dbpass"
        self.table_to_query = table_to_query   #Use this to update all scripts to unhardcode table being queried
        
        if user == None:
            self.logger.info("Connecting to " + dbname + " with user " + getpass.getuser())
            connectionSetUp = "dbname=" + dbname + " port=" + port + " host=" + host
        else:
            self.logger.info("Connecting to " + dbname + " with user " + user)
            connectionSetUp = "dbname=" + dbname + " user=" + user  + " password="+ password +" port=" + port + " host=" + host   
            
        self.connection = psycopg2.connect(connectionSetUp)
        self.logger.info("Connection successful")
    
    #DATABASE UTILITY FUNCTION 
    def meta2db(self, metaDict, overwrite=False):
        """
        Uploads image metadata to the database as discovered by the meta module.
        *meta* is a dictionary - no need to upload all the fields (some are not
        included in the table structure)
        
        Note that granule and dimgname are unique - as a precaution - a first query
        deletes records that would otherwise be duplicated. 
        This assumes that they should be overwritten!
        
        
        **Parameters**
        
            *metaDict* : dictionnary containing the metadata
        """      
        #First, look to see if granule or dimgname exists in tblmetadata
        # This may happen since the archive periodically contains the same file twice
            # in this case the previous file is overwritten (a bit of a time waster)
        ###sqlDel = '''DELETE FROM tblbanddata WHERE granule = %(granule)s
            ####OR dimgname = %(dimgname)s'''
        
        sqlSel = '''SELECT FROM ''' + self.table_to_query + ''' WHERE dimgname = %(dimgname)s'''
        sqlDel = '''DELETE FROM ''' + self.table_to_query + ''' WHERE dimgname = %(dimgname)s'''

        #upload the data
        if metaDict['sattype'] == 'RSAT1' or metaDict['sattype'] == 'RSAT2'or metaDict['sattype'] == 'RS2':
            sqlIns = '''INSERT INTO ''' + self.table_to_query + ''' 
            (acDOY, geom, acDateTime, antennaPointing, beam, beams, bitsPerSample, 
            notes, copyright, dimgname, freqSAR, granule, lineSpacing, looks_Az, looks_Rg, 
            lutApplied, n_bands, n_beams, n_cols, n_geopts, n_rows, orbit, order_Az, order_Rg, 
            passDirection, pixelSpacing, polarization, processingFacility, productType,
            satellite, sattype, theta_far, theta_near, sat_heading, location) 
            VALUES ( 
            %(acDOY)s, ST_GeomFromText(%(geom)s, %(geoptsSRID)s), %(acDateTime)s, %(antennaPointing)s, %(beam)s, %(beams)s, %(bitsPerSample)s,
            %(notes)s, %(copyright)s, %(dimgname)s, %(freqSAR)s, %(granule)s, %(lineSpacing)s, %(looks_Az)s, %(looks_Rg)s, 
            %(lutApplied)s, %(n_bands)s, %(n_beams)s, %(n_cols)s, %(n_geopts)s, %(n_rows)s, %(orbit)s, %(order_Az)s, %(order_Rg)s, 
            %(passDirection)s, %(pixelSpacing)s, %(polarization)s, %(processingFacility)s, %(productType)s,
            %(satellite)s, %(sattype)s, %(theta_far)s, %(theta_near)s, %(sat_heading)s, %(location)s
            )'''
        
        elif metaDict['sattype'] == 'SEN-1':
            sqlIns = '''INSERT INTO ''' + "tblmetadata_ai_s1" + ''' 
            (acDOY, geom, acDateTime, beam, bitsPerSample, 
            notes, copyright, dimgname, granule, lineSpacing, n_bands, n_cols, n_geopts, n_rows, 
            passDirection, pixelSpacing, polarization,
            satellite, sattype, location) 
            VALUES ( 
            %(acDOY)s, ST_GeomFromText(%(geom)s, %(geoptsSRID)s), %(acDateTime)s, %(beam)s, %(bitsPerSample)s,
            %(notes)s, %(copyright)s, %(dimgname)s, %(granule)s, %(lineSpacing)s, %(n_bands)s, %(n_cols)s, %(n_geopts)s, %(n_rows)s, 
            %(passDirection)s, %(pixelSpacing)s, %(polarization)s,
            %(satellite)s, %(sattype)s, %(location)s
            )'''
            
        else:
            print("Warning! Satellite type {} not supported in the mode.".format(self.metaDict.sattype))
            print("Terminating program...")
            sys.exit()
                    
        curs = self.connection.cursor()
        if not overwrite:
            curs.execute(sqlSel, metaDict) #TODO
            result = curs.fetchall()
            if len(result) > 0:
                self.logger.debug("This zip is already in the database!")
                
        curs.execute(sqlDel, metaDict)
        curs.execute(sqlIns, metaDict)
        
        self.connection.commit()
        
        self.logger.info("dimgname:    " + metaDict['dimgname'] )
        self.logger.info("[Succesfuly added "+ metaDict['dimgname'] + " metadata into " + self.table_to_query + ".]" )

    #DATABASE UTILITY FUNCTION    
    def createTblMetadata(self):
        """
        Creates a metadata table of name specified in the cfg file under Table. It overwrites if that table name already exist, be careful!
        """

        #TODO - make SRID an option (default to 4326)
        
        name = self.table_to_query            
        curs = self.connection.cursor()  
        
        curs.execute('DROP TABLE IF EXISTS ' + name) # overwrites !!!  

        sql1 = 'CREATE TABLE ' + name
        sql2 = '(dimgname varchar(100), granule varchar(100), acDateTime timestamp, acDOY double precision, polarization varchar(20),'
        sql3 = 'beam varchar(5), n_bands int, n_cols int, n_rows int, lineSpacing double precision, pixelSpacing double precision, '
        sql4 = 'satellite varchar(20), productType varchar(20), processingFacility varchar(20), sattype varchar(20), notes varchar(256), '
        sql5 = 'n_geopts int, n_beams int, beams varchar(15), bitsPerSample int, freqSAR double precision, copyright varchar(256), '
        sql6 = 'looks_Az int, looks_Rg int, lutApplied varchar(50), antennaPointing varchar(10), orbit int, order_Az varchar(20), '
        sql7 = 'order_Rg varchar(20), passDirection varchar(20), sat_heading double precision, theta_far double precision, '
        sql8 = 'theta_near double precision, airtemp double precision, location character varying(300));'   
                     
        
        createSql = sql1 + sql2 + sql3 + sql4 + sql5 + sql6 + sql7 + sql8
        
        curs.execute(createSql) #Create the table
        
        # Add geometry - SRID of 4326 is WGS84 and must exist in Spatial refs - so check and add if needed
       
        sridQuery = 'SELECT * from spatial_ref_sys where srid = 4326';
        curs.execute(sridQuery)
        srid4326Result = curs.fetchall()
        if len(srid4326Result) == 0:
            #TODO throw exception and exit at this point?  
            # hard to imagine needing this, to be honest.

            self.logger.debug("The database does not have SRID 4326, adding this now")
            srid4326Sql1 = '''INSERT into spatial_ref_sys (srid, auth_name, auth_srid, proj4text, srtext) values '''
            srid4326Sql2 = '''( 4326, 'sr-org', 14, '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs ', 'GEOGCS["GCS_WGS_1984",DATUM'''
            srid4326Sql3 ='''["WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]');'''     
        
            srid4326Insert = srid4326Sql1 + srid4326Sql2 + srid4326Sql3
                      
            curs.execute(srid4326Insert)        
            self.connection.commit()
            
        geoColumnSql = """SELECT AddGeometryColumn('""" + self.table_to_query + """', 'geom', 4326, 'POLYGON', 2)"""
        
        #Now setup the primary key and index on dimgname
        alterSql = 'ALTER TABLE ' + self.table_to_query + ' OWNER TO postgres, ADD PRIMARY KEY (granule), ADD UNIQUE (dimgname);'

        indexSql = 'CREATE INDEX ' + self.table_to_query + '_geeom_gist ON ' + self.table_to_query + ' USING gist (geom);'

        curs.execute(geoColumnSql)
        curs.execute(alterSql)
        curs.execute(indexSql)
        self.connection.commit()

        self.logger.info(self.table_to_query + " created")

    #CALL FROM QUERY    
    def updateFromArchive(self, archDir):
        """
        Goes to CIS Archive metadata shapefiles and (re)creates and updates tblArchive in the connected database
        tblArchive then represents all the image files that CIS has (in theory)
        The first thing this script does is define the table - this is done from an sql file and contains the required SRID
        Then it uses ogr2ogr to upload each shp in the archDir 
        The script looks for the *.last files to know which files are the most current (these need to be updated)

        Can be extended to import from other archives (In the long term - PDC?)
        
        **Parameters**
        
            *archDir* : archive directory           
        """
        
        # go to shapefile directory        
        os.chdir(os.path.join(archDir))
        sats = glob.glob('*.last')
        srid = 96718

        self.qryFromText("DROP TABLE IF EXISTS public.tblarchive") # clean out old first..
        create ="YES" # on first time through, make new table, after that don't.
        # upload each shapefile in turn                        
        for sat in sats: 
            fp = open(sat)
            lastdate = fp.readline()
            fp.close()
            shpname = sat[:-5] +'_' +lastdate.strip() + '.shp'
            
            cmd = "ogr2ogr --config PG_USE_COPY YES -f PGDump "\
                +"-lco CREATE_TABLE="+create \
                +" -lco GEOMETRY_NAME=geom -lco DROP_TABLE=NO -lco SRID="\
                + str(srid) + " /vsistdout/ -nln tblArchive " + shpname +" "+ shpname[:-4] +  \
                " | psql -h "+self.host+" -d "+self.dbName+" -f -"
                                       
            try:
                ok = os.system(cmd) 
                if ok == 0: 
                    self.logger.info("added shapefile %s." % shpname)
                else:  
                    self.logger.error("Problem with shapefile %s." % shpname)
                create="NO"
            except:
                self.logger.error("Problem with shapefile %s." % shpname)           
            
        qryAlt = """ALTER TABLE tblArchive ALTER COLUMN \"valid time\"
                TYPE TIMESTAMP USING CAST (\"valid time\" AS timestamp);"""
        qryKey1 = """ALTER TABLE tblArchive DROP CONSTRAINT tblarchive_pk;"""
        qryKey2 = """ALTER TABLE tblArchive ADD CONSTRAINT tblArchive_pk PRIMARY KEY ("file name");"""
        
        qryCountAllImgs = """SELECT COUNT(*) FROM tblArchive;"""
        
        curs = self.connection.cursor()
        curs.execute(qryKey1)
        self.connection.commit()
        curs.execute(qryKey2)
        self.connection.commit()
        curs.execute(qryAlt)
        self.connection.commit()
        curs.execute(qryCountAllImgs)
        n_imgs = curs.fetchall()
        self.logger.info("tblArchive updated with " + str(n_imgs[0][0]) + " SAR images")     

    #CALL FROM QUERY    
    def qryGetInstances(self, granule, roi, metaTable):   
        """
        Writes a query to fetch the instance names that are
        associated spatially in the relational table.
        
        **Parameters**
        
            *granule*    : granule name 
            
            *roi*        : region of interest file 
            
            *metaTable*  : metadata table containing data of images being worked on
                
        **Returns**
        
            *instances*  : instances id (unique for entire project, i.e. 5-digit string)
        """
    
        # retrieve the dimgname/imgref1 by querying granule that maps to the wanted imgref1
        curs = self.connection.cursor()
        param = {'granule' : granule}
        
        sql = "SELECT dimgname FROM " + self.table_to_query + " WHERE granule LIKE %(granule)s"

        curs.execute(sql,param)
        try:
            dimgname = str(curs.fetchone()[0])
            
        except:
            instances = -1
            return instances
            
        # retrieve all the instances of polygons that relate to image       
        param = {'dimgname' : dimgname}
        sql = "SELECT inst FROM "+roi+" WHERE %(dimgname)s LIKE imgref"  
        curs.execute(sql,param)
        result = curs.fetchall()
        instances = []
        for i in range(len(result)):
            instances.append(result[i][0])
        return instances

    #DATABASE UTILITY FUNCTION   
    def updateROI(self, inFile, srid, wdir, ogr=False): 
        """
        This function will update an ROI (Region Of Interest) table in the database. It has a prescribed format
        It will take the shapefile named inFile and update the database with the info
        
        Note that this will overwrite any table named *inFile* in the database

        The generated table will include a column *inst* - a unique identifier created by 
        concatenating obj and instid

        **Parameters**
        
            *inFile*   : Basename of a shapefile (becomes the ROI table name too)
                
            *srid*     : srid of desired projection
            
            *wdir*     : Full path to directory containing ROI (NOT path to ROI itself, just the directory containing it)
           
        **Required in ROI (In shp attribute table)**
        
            *obj*      - The id or name of an object/polygon that defines a region of interest. Very systematic, no spaces. 
            
            *instid*   - A number to distinguish repetitions of each obj in time or space.  For example an ROI that occurs several summers would have several instids.            
            
            *fromdate* - A valid iso time denoting the start of the ROI - can be blank if imgref is used            
            
            *todate*   - A valid iso time denoting the start of the ROI - can be blank if imgref is used
   
        **Optional in ROI (in shp attribute table)**
            
            *imgref*   - A reference image dimage name (for a given ROI) - this can be provided in place of datefrom and dateto                                 
            
            *name*     - A name for each obj (Area51_1950s, Target7, Ayles)            
            
            *comment*  - A comment field
            
            Any other field can be added...            
        """
    
        outTable = inFile
        
        cmd = 'ogr2ogr --config PG_USE_COPY YES -f PGDump -lco GEOMETRY_NAME=geom -lco DROP_TABLE=IF_EXISTS -lco SRID='+ str(srid) + ' -nlt PROMOTE_TO_MULTI /vsistdout/ ' + inFile +'.shp '+ inFile + ' | psql -h '+self.host+' -d '+self.dbName+' -f -'
     
        os.chdir(wdir)
        ok = os.system(cmd)
        
        if ok == 0:
            self.logger.info("added shapefile %s." % inFile)
        else: 
            self.logger.error("Problem with shapefile %s." % inFile)
            return
    
        # can't be done in a transaction  qryClean = """VACUUM ANALYZE;"""
        qryCountAllPoly = 'SELECT COUNT(*) FROM ' + outTable + ';'
        qryCheckFieldExists = 'SELECT column_name from information_schema.columns where column_name=\'imgref\' and table_name=\'' + outTable + '\';'
        
        #make new column inst = obj+instid
        qryNewColInst = 'ALTER TABLE ' + outTable + ' ADD COLUMN inst varchar;'
        qryUpdateInstField = 'UPDATE '+outTable+' SET inst = obj||instid;'

        qryKey1 = 'ALTER TABLE ' + outTable + ' DROP CONSTRAINT  ' + outTable + '_pk;'
        qryKey2 = 'ALTER TABLE  ' + outTable + '  ADD CONSTRAINT  ' + outTable + '_pk PRIMARY KEY ("inst");'
       
        # make sure you have enough space here...
        qryLongerDate = 'ALTER TABLE  ' + outTable + '  ALTER COLUMN todate TYPE varchar(20), ALTER COLUMN fromdate TYPE varchar(20)'
     
        qryImgRefFromDate = ''' SET fromdate = 
                    substring(imgref from 1 for 4)||'-'||substring(imgref from 5 for 2)||'-'||substring(imgref from 7 for 2)||' '||
                    substring(imgref from 10 for 2)||':'||substring(imgref from 12 for 2)||':'||substring(imgref from 14 for 2)
                    WHERE fromdate ISNULL AND imgref NOTNULL;'''

        qryImgRefToDate = ''' SET todate = 
                    substring(imgref from 1 for 4)||'-'||substring(imgref from 5 for 2)||'-'||substring(imgref from 7 for 2)||' '||
                    substring(imgref from 10 for 2)||':'||substring(imgref from 12 for 2)||':'||substring(imgref from 14 for 2)
                    WHERE todate ISNULL AND imgref NOTNULL;'''
 
        qryCastDate = 'ALTER TABLE ' + outTable + ' ALTER COLUMN FromDate TYPE '+ \
                    'TIMESTAMP USING CAST (FromDate AS timestamp), ' + \
                    'ALTER COLUMN ToDate TYPE TIMESTAMP USING CAST (ToDate AS timestamp);'                    
                    
        try:
            curs = self.connection.cursor()
            curs.execute(qryCountAllPoly)
            n_rows = curs.fetchall()
            curs.execute(qryCheckFieldExists)
            field = curs.fetchall()
            
        except:
            self.logger.error("Can't query the database")
                
        curs.execute(qryNewColInst)
        curs.execute(qryUpdateInstField)      
        curs.execute(qryLongerDate)

        if len(field) != 0: 
            curs.execute('UPDATE ' + outTable+ qryImgRefFromDate)  #TODO test this
            curs.execute('UPDATE ' + outTable+ qryImgRefToDate)
        curs.execute(qryCastDate)
        self.connection.commit()
    
        curs.execute(qryKey1)
        self.connection.commit()
        curs.execute(qryKey2)
        self.connection.commit()       
        
        self.logger.info("Database updated with " + str(n_rows[0][0]) + " ROI polygons")
        
    #POSSIBLY OBSOLETE    
    def qryFromText(self, sql, output=False):
        """
        Runs a query in the current databse by sending an sql string  
           
        **Note:** do not use '%' in the query b/c it interfers with the pyformat protocol
        used by psycopg2. Also be sure to triple quote your string to avoid escaping single quotes.
        
        IF EVER THE Transaction block fails, just conn.rollback(). Try to use pyformat for queries - see dbapi2 (PEP).
        You can format the SQL nicely with an online tool - like SQLinForm
        
        **Parameters**
            
            *sql*    : the sql text that you want to send
            
            *output* : make true if you expect/want the query to return results
            
        **Returns**
           
           The result of the query as a tupple containing a numpy array and the column names as a list (if requested and available)
        """
        
        curs = self.connection.cursor()
        try:
            curs.execute(sql)
            if output:
                rows = curs.fetchall()
                colnames = [desc[0] for desc in curs.description]
                
        except ValueError as e:
            self.logger.error('ERROR(value): Confirm that the values sent or retrieved are as expected--> ' +str(e))
            self.connection.rollback() #from Metadata import Metadatalback()
            
        except psycopg2.ProgrammingError as e:
            self.logger.error('ERROR(programming): Confirm the SQL statement is valid--> ' +str(e))
            self.connection.rollback()
            
        except StandardError as e:
            self.logger.error('ERROR(standard): ' +str(e))   
            self.connection.rollback()
            
        else:
            self.connection.commit()
            self.logger.debug('Query sent successfully')
            if output:
                return(numpy.asarray(rows), colnames)       

    #OBSOLETE    
    def qryFromFile(self, fname, path, output=False):
        """
        Runs a query in the current databse by opening a file - adds the path and 
        .sql extension - reading contents to a string and running the query
                
        **Note:** do not use '%' in the query b/c it interfers with the pyformat protocol
        used by psycopg2
        
        **Parameters**
            
            *fname*  : file name (don't put the sql extension, it's assumed)

            *path*   : full path to fname

            *output* : make true if you expect/want the query to return results
        """
        
        fname = os.path.join(path, fname)
    
        fp = open(fname+'.sql', mode='r')
        sql = fp.read()
        data = self.qryFromText(sql, output)
        if output:
            return(data)


    def qrySelectFromAvailable(self, roi, selectFrom, spatialrel, srid):
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

        curs = self.connection.cursor()

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

            fromdate = instances[i][1] + datetime.timedelta(seconds=1)
            fromdate = fromdate.strftime('%Y-%m-%d')

            #allow for truncation errors
            todate = instances[i][2] + datetime.timedelta(seconds=1)
            todate = todate.strftime('%Y-%m-%d')   #%H:%M:%S'

            param = {'inst': inst, 'fromdate' : fromdate, 'todate' : todate, 'srid' : int(srid)}

            if selectFrom == 'tblArchive':
                sql1 = """SELECT DISTINCT ON (substring("file name", 1, 27)) 
                "file name", "file path", "subtype", "beam mode", "valid time", "catalog id" """
                sql4 = 'AND "valid time" >= %(fromdate)s '
                sql5 = 'AND "valid time" <= %(todate)s '
                sql9 = """ORDER BY substring("file name", 1, 27), "file name" DESC"""

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
            self.connection.commit()
            rows = curs.fetchall()

            for i in range(len(rows)):

                if selectFrom == 'tblArchive':

                    granule= rows[i][0]
                    catid= rows[i][5]
                    acTime= rows[i][4]

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

    #OBSOLETE
    def relations2db(self, roi, spatialrel, instimg, fname=None, mode='refresh', export=False):
        """
        A function to add or update relational tables that contain the name of an image
        and the features that they are spatially related to:
        For example:  a table that shows what images intersect with general areas or
        a table that lists images that contain ROI polygons
            
        This function runs in create mode or refresh mode
        Create - Drops and re-creates the table
        
        Refresh - Adds new data (leaves the old stuff intact)
        
        **Parameters**
            
            *roi*        : region of interest table                

            *spatialrel* : spatial relationship (i.e. ST_Contains or ST_Intersect)                

            *instimg*    : a list of images that are spatially related to each inst 

            *fname*      : csv filename if exporting                       

            *mode*       : create or refresh mode
            
            *export*     : If relations should be exported to a csv or not
        """
        
        name = self.nameRelationalTable(roi, spatialrel)
        
        assert mode == 'refresh' or mode == 'create', 'Wrong mode!'
        
        curs = self.connection.cursor()
        
        if mode == 'create':
            sql1 = 'CREATE TABLE ' + name
            sql2 = ' (inst character varying NOT NULL, granule character varying NOT NULL, '
            sql3 = 'CONSTRAINT ' + name +'_pkey PRIMARY KEY (inst, granule)) WITH (OIDS=FALSE);'
                
            curs.execute('DROP TABLE IF EXISTS ' + name) # overwrites !!!
            curs.execute(sql1+sql2+sql3)
            self.connection.commit()    
        
        if mode == 'refresh': 
            # Look in the trel to see if instimg exists there - if so, remove...
            sql = 'SELECT COUNT(granule) FROM '+name+' WHERE inst = %(inst)s AND granule = %(granule)s'
            instimg
            for row in instimg[:]:  #NB: iter a copy of list, or values get skipped
                curs.execute(sql, row)
                if curs.fetchall()[0][0] > 0:
                    instimg.remove(row)
                    
        # now put the new data into the database
        curs.executemany("""INSERT INTO """+name+"""(inst,granule) VALUES
          (%(inst)s, %(granule)s)""", instimg)
        self.connection.commit()
        
        self.logger.info("Uploaded new data to " + name)
        
        if export:
            tmp = pd.DataFrame.from_dict(instimg)
            for col in tmp.columns:
                if tmp[col].dtype == 'O' or tmp[col].dtype == 'S':
                    tmp[col] = tmp[col].str.rstrip()  # somehow there are plenty of spaces in some cols
            tmp.to_csv(fname,index=False)    

    def qryCropZone(self, granule, roi, spatialrel, inst, metaTable, srid=4326):
        """
        Writes a query to fetch the bounding box of the area that the inst polygon and
        image in question intersect.
        returns a crop ullr tupple pair in the projection given
        
        **Parameters**
            
            *granule*    : granule name                

            *roi*        : region of interest file                

            *spatialrel* : spatial relationship (i.e. ST_Contains or ST_Intersect)                           

            *inst*       : instance id (i.e. a 5-digit string)
            
            *metaTable*  : metadata table containing data of images being worked on
            
            *srid*       : srid of desired projection (default WGS84, as this comes out of TC from snap, crop can be done before projection) 
                
        **Returns**
            
            *ullr*       : upper left, lower right tupple pair in the projection given
        """

        param = {"srid" : int(srid), "granule" : granule, "inst" : str(inst)}  

        #TODO: Investigate error:
        #"could not form projection from 'srid=96718' to 'srid=4326"
        sql1 = "SELECT ST_AsText(ST_Envelope(ST_Intersection((SELECT ST_Transform(" + self.table_to_query + ".geom, %(srid)s) FROM " + self.table_to_query + " WHERE granule = %(granule)s), (SELECT ST_Transform("
        sql2 = ".geom, %(srid)s) FROM "
        sql3 = " WHERE inst = %(inst)s))))"
    
        sql = sql1+ roi +sql2+ roi +sql3

        curs = self.connection.cursor()
        curs.execute(sql, param)
        bbtext = curs.fetchall()

        #parse the text to get the pair of tupples
        bbtext = bbtext[0][0]  #slice the piece you need

        if bbtext == 'GEOMETRYCOLLECTION EMPTY' or bbtext == 'POLYGON EMPTY' or bbtext == None:
            ullr = 0
         
        else:#clockwise
            ul = bbtext.split(',')[0].split('((')[1]
            ulx = ul.split()[0]
            uly = ul.split()[1]
            
            ur = bbtext.split(',')[1]   #ul
            urx = ur.split()[0]
            ury = ur.split()[1]
            
            lr = bbtext.split(',')[2]
            lrx = lr.split()[0]
            lry = lr.split()[1]
            
            ll = bbtext.split(',')[3].split('))')[0]    #lr
            llx = ll.split()[0]
            lly = ll.split()[1]

            #ullr = (ulx, uly), (lrx, lry)
            ullr  = (urx, ury), (llx, lly)
        
        return ullr

    def qryMaskZone(self, granule, roi, srid, inst, metaTable):
        """
       Writes a query to fetch the area the inst polygon in the roi and the image in question intersect. 
       This polygon will be used to mask the image. 
        
        **Parameters**
            
            *granule*    : granule name               

            *roi*        : region of interest file                               

            *srid*       : srid of desired projection               

            *inst*       : instance id (i.e. a 5-digit string)
            
            *metaTable*  : metadata table containing data of images being worked on
            
            *idField*    : name of field containing instance id's
                
        **Returns**
            
            *polytext*  :   gml text
        """
    
        curs = self.connection.cursor()        
        
        param = {'granule': granule}
        
        sql = "SELECT dimgname FROM "+metaTable+" WHERE granule LIKE %(granule)s"
        curs.execute(sql,param)
        dimgname = str(curs.fetchone()[0])
        
        #line below removes a dash due to lack of in earlier naming convention
        param = {'srid': int(srid), 'granule': granule, 'inst': inst, 'dimgname' : dimgname } #+ '%'}
        
        #make table selected from unhardcoded
        sql = '''SELECT ST_AsText(ST_Transform('''+roi+'''.geom, %(srid)s))
        FROM '''+roi+''' WHERE inst = %(inst)s AND %(dimgname)s LIKE imgref'''
        
        polytext = curs.fetchall()
        if len(polytext) > 0:
            return polytext[0][0]
        elif polytext == 'GEOMETRYCOLLECTION EMPTY':
            polytext = 0
        else: #this should not be the case
            self.logger.debug("Query did not execute, no masking performed")
            polytext = 0
        curs.close()
        return polytext

    #UTIL FUNCTION
    def imgData2db(self, imgData, bandName, inst, dimgname, granule):
        """
        Upload image data as an array to a new database table
        
        What is needed by function: imgData, the imgType, the bandName, the inst, dimgname and granule        
        What is computed to add to upload: count, mean, std, min, max for non-zero elements
        
        **Parameters**
            
            *imgData*   : Pixel values                        

            *bandName*  : Name of the band being uploaded                  

            *inst*      : instance id                 

            *dimgname*  : Image name                

            *granule*   : granule name 
        """
        noDataVal = 0

        polyData = imgData[numpy.where( imgData != noDataVal )]
        
        upload = {
            'granule' : granule,
            'bandname' : bandName,
            'inst' : inst,
            'dimgname' : dimgname,
            'mean' : str(polyData.mean()),  # convert real or get can't adapt error
            'var' :  str(polyData.var()),
            'maxdata' : str(polyData.max()), 
            'mindata' : str(polyData.min()),
            'median' : str(numpy.median(polyData)),
            'quart1' : str(stats.scoreatpercentile(polyData, 25)),
            'quart3' : str(stats.scoreatpercentile(polyData, 75)),
            'skew' : str(stats.skew(polyData, None)),
            'kurtosis' : str(stats.kurtosis(polyData, None))
            }
            
            
        #If table isn't created
        '''
        create table tblbanddata (granule varchar(100), bandname varchar(100), inst int, dimgname varchar(100), mean varchar(25), var varchar(25),
        maxdata varchar(25), mindata varchar(25), median varchar(25), quart1 varchar (25), quart3 varchar(25), skew varchar (25), kurtosis varchar (25))
        '''        
        
        #First, look to see if primary key exists, if so, overwrite record
        sqlDel = '''DELETE FROM tblbanddata WHERE bandname = %(bandname)s 
            AND granule = %(granule)s AND inst = %(inst)s'''
        
        delVals = {'granule' : granule,
            'bandname' : bandName,
            'inst' : inst,}
                
        sqlIns = '''INSERT INTO tblbanddata 
            (granule, bandname, inst, dimgname, mean, var, 
            maxdata, mindata, median, quart1, quart3, skew, kurtosis) 
            VALUES 
            (%(granule)s, %(bandname)s, %(inst)s, %(dimgname)s, 
            %(mean)s, %(var)s, %(maxdata)s, %(mindata)s, 
            %(median)s, %(quart1)s, %(quart3)s, %(skew)s, %(kurtosis)s)'''
             
        curs = self.connection.cursor()
        
        try:
            curs.execute(sqlDel, delVals)
            self.connection.commit()
        except:
            self.connection.rollback()
            pass
        
        try:
            curs.execute(sqlIns, upload)
        except Exception as e:
            self.logger.error(e)
            return
        self.connection.commit()
        self.logger.info('Image ' + granule + ' data uploaded to database')        

    #OBSOLETE    
    def numpy2sql(self, numpyArray, dims):
        """
        Converts a 1- or 2-D numpy array to an sql friendly array
        Do not use with a string array! 
        
        **Parameters**
            
            *numpyArray* : numpy array to convert                

            *dims*       : dimension (1 or 2)

        **Returns**
            
            *array_sql*  : an sql friendly array
        """
        
        assert dims == 1 or dims == 2, 'Specify dimension of array - 1 or 2'
        
        array_sql = "{" 
    
        if dims == 1:
            for row in numpyArray:  # dim = 1
                array_sql += str(row) + ','
            array_sql = array_sql[:-1] # Trim comma
        
        elif dims == 2:
            for row in numpyArray:  # dim = 1
                row_sql = "{"
                for value in row:
                    row_sql += str(value) + ','
                row_sql = row_sql[:-1] # Trim comma
                row_sql += "}"
                array_sql += str(row_sql) + ','
            array_sql = array_sql[:-1] # Trim comma
    
        array_sql += "}"
        return array_sql        
    
    #OBSOLETE
    def sql2numpy(self, sqlArray, dtype='float32'):
        """
        Coming from SQL queries, arrays are stored as a list (or list of lists)
        Defaults to float32
        
        **Parameters**
            
            *sqlArray* : an sql friendly array               

            *dtype*    : default type (float32)
                
        **Returns**
            
            *list*     : list containing the arrays
        """
        
        list = numpy.asarray(sqlArray, dtype=dtype) 
        return list

    #OBSOLETE
    def exportToCSV(self, qryOutput, outputName):
        """
        Given a dictionary of results from the database and a filename puts all the results
        into a csv with the filename outputName
        
        **Parameters**
            
            *qryOutput*  : output from a query - needs to be a tupple - numpy data and list of column names                

            *outputName* : the file name             
        """
        
        tmp = pd.DataFrame(qryOutput[0], columns=qryOutput[1])
        for col in tmp.columns:
            if tmp[col].dtype == 'O' or tmp[col].dtype == 'S':
                stripped = tmp[col].str.rstrip()  # somehow there are plenty of spaces in some cols
                if not stripped.isnull().all():  #sometimes this goes horribly wrong (datetimes)
                    tmp[col] =stripped
        tmp.to_csv(outputName, index=False)

    #DATABASE?    
    def findInstances(self, roi):
        """
        Scans metadata table to determine if any images
        intersect the given roi
        
        **Parameters**
            
            *roi*  : roi table name           
        """
        
        meta = self.table_to_query
        
        qry = """SELECT """+ meta+""".dimgname, """+roi+""".inst""" +\
        """ FROM """+meta+ """, """+roi+ \
        """ WHERE ST_Intersects(ST_Transform("""+self.table_to_query+""".geom, 96718), 
        ST_Transform("""+roi+""".geom, 96718)) """ +\
        """AND """+meta+""".acdatetime BETWEEN """+roi+""".fromdate AND """+roi+""".todate"""
        
        curs = self.connection.cursor()
        
        curs.execute(qry)
        self.connection.commit()
        instances = curs.fetchall()
        for instance in instances:
            img = instance[0]
            inst = instance[1]
            qry2 = """SELECT imgref FROM """+roi+""" WHERE inst = '"""+inst+"""'"""
            curs.execute(qry2)
            self.connection.commit()
            imgref = curs.fetchall()[0]

            if imgref[0] is None:
                sqlAdd = """UPDATE """+roi+""" SET imgref = '"""+img+\
                         """' WHERE inst = '"""+inst+"""'"""
                curs.execute(sqlAdd)
                self.connection.commit()
            elif imgref[0] == img:
                pass
            else:
                n = len(inst)
                qry_inst = """SELECT inst FROM """+roi+""" WHERE inst LIKE '"""+inst[:n-3]+"""%' """ +\
                           """AND LENGTH(inst) = """ + str(n)
                curs.execute(qry_inst)
                self.connection.commit()
                last_inst = numpy.max([int(x[0]) for x in curs.fetchall()])
                
                sqlCOL = """SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS """+\
                         """WHERE TABLE_NAME = '"""+roi+"""'"""
                
                curs.execute(sqlCOL)
                self.connection.commit()
                cols = [col[0] for col in curs.fetchall()]
                
                keep_col = [col for col in cols if col != "imgref" and col != "inst" and col != "ogc_fid"]
                keep_col = ", ".join(keep_col)
                new_inst = str(last_inst + 1)
                sqlINS = """INSERT INTO """+roi+""" ("""+", ".join(cols)+ \
                         """) SELECT '"""+new_inst+"""', """+keep_col+""", '"""+img+"""', '"""+new_inst+ \
                         """' FROM """+roi+""" WHERE inst = '"""+inst+"""'"""
                         
                curs.execute(sqlINS)
                self.connection.commit()
                
    def stats2db(stats, inst, granule, roi):
        #function to upload stats generated in Quantitative mode to a database table
        pass            
        
        
    #POLARIMETRIC SPECIFIC
    def beaconShapefilesToTables(self, dirName):
        """
        Takes a directory containing beacon shape files and converts them to tables and 
        inserts them into the database appending *beacon_* before the name
        
        **Parameters**
           
           *dirName* : Directory containing the beacon shapefiles
        """
        
        os.chdir(dirName)        
        
        #Go through and get the names of all the shapefiles in the directory
        shapefiles=[]
        
        for sFile in glob.glob("*.shp"):
            shapefiles.append(sFile)
        self.logger.info("shape files are", shapefiles)
        
        for shpname in shapefiles:

            try:
                ok = os.system("ogr2ogr -f PostgreSQL PG:'host="+ self.host +" dbname=" + self.dbName + \
                "' -a_srs EPSG:4326 -nln beacon_" + shpname[:-4] + " " + shpname +" "+ shpname[:-4]) 
                qryAlt = """ALTER TABLE beacon_""" + shpname[:-4] +\
                    """ ALTER COLUMN gps_time TYPE TIMESTAMP USING CAST (gps_time AS timestamp);"""
                self.qryFromText(qryAlt)
                if ok == 0: 
                    self.logger.debug("added shapefile %s." % shpname)
                else:
                    self.logger.error("Problem with shapefile %s." % shpname)                  
            except:
                self.logger.error("Problem with shapefile %s." % shpname)           

    #OBSOLETE    
    def convertGPSTime(self, shpTable):
        """
        Takes a shape file and converts the gps_time from character type to timestamp time.
        
        **Parameters**
            
            *shpTable* : Shapefile table in the database
        """
      
        sql1 = "ALTER TABLE " + shpTable + ' '
        sql2 = "ALTER COLUMN gps_time TYPE timestamp USING gps_time::timestamp"
        query = sql1 + sql2
        self.logger.debug("query is" + query)
        curs = self.connection.cursor()
        curs.execute(query) 
        self.connection.commit()

    #KEEP    
    def removeHandler(self):
        self.logger.handlers = []



    def exportToCSV_Tmp(self, qryOutput, outputName):
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
        