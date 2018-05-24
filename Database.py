# -*- coding: utf-8 -*-
"""
**Database.py**

**Created on** Tue Feb 12 23:12:13 2013 **@author:** Cindy Lopes

This module creates an instance of class Database and connects to a database to
create, update and query tables.

Tables of note include:

**tblmetadata** - a table that contains metadata that is gleaned by a directory scan
**roi_tbl** - a table with a region of interest (could be named something else)
**trel_roiinst_con** or _int - a relational table that results from a spatial query
**tblArchive** - a copy of the metadata from the CIS image archive

Other tables could contain data from drifting beacons or other data

**Modified on** 23 May 14:43:40 2018 **@reason:** Added logging functionality **@author:** Cameron Fitzpatrick
"""

import psycopg2
import psycopg2.extras
from psycopg2.extensions import AsIs
import os
#import sys
#from datetime import datetime
import datetime
import shutil
import numpy
import Util
import scipy.stats as stats
import copy
import csv
import glob
import getpass
import pandas as pd
import pdb
import logging

class Database:
    """
    This is the Database class for each database connection.
    
    Creates a connection to the specified database.  You can connect as a specific user or 
        default to your own username, which assumes you have privileges and a password stored in your  ~/.pgpass file
        
        Note: if you have any issues with a bad query, send a rollback to the database to reset the connection.
        >>>>db.connection.rollback()
        
        **Parameters**
        
            *dbname*   : database name 
               
            *user*     : user with sudo access on the specified databse (i.e. postgres user has sudo access to all databses)
            
            *password* : password to go with user  
              
            *port*     : server port (i.e. 5432) 
               
            *host*     : hostname (i.e. localhost)
    """
    
    def __init__(self, dbname, user=None, password=None, port='5432', host='localhost', loghandler = None):
        
        if loghandler != None:
            self.loghandler = loghandler             #Logging setup if loghandler sent, otherwise, all errors are printed
            self.logger = logging.getLogger('Database')
            self.logger.addHandler(loghandler)
            
            self.logger.setLevel(logging.DEBUG)
        
        self.dbName = dbname
        self.port = port
        self.host = host
        self.user = user
        self.password = "dbpass"
        
        if user == None:
            try:
                self.logger.info("Connecting to ", dbname, " with user ", getpass.getuser())
            except:
                print "Connecting to ", dbname, " with user ", getpass.getuser()
            connectionSetUp = "dbname=" + dbname + " port=" + port + " host=" + host
        else:
            try:
                self.logger.info("Connecting to ", dbname, " with user ", user)
            except:
                print "Connecting to ", dbname, " with user ", user
            connectionSetUp = "dbname=" + dbname + " user=" + user  + " password="+ password +" port=" + port + " host=" + host        
        self.connection = psycopg2.connect(connectionSetUp)
        try:
            self.logger.info("Connection successful")
        except:
            print "Connection successful"
        

    
    def meta2db(self, metaDict, overwrite=True):
        """
        Uploads image metadata to the database as discovered by the meta module.
        *meta* is a dictionary - no need to upload all the fields (some are not
        included in the table structure)
        
        Note that granule and dimgname are unique - as a precaution - a first query
        deletes records that would otherwise be duplicated 
        This assumes that they should be overwritten!
        
        
        **Parameters**
        
            *metaDict* : dictionnary containing the metadata
        """
        
        #First, look to see if granule or dimgname exists in tblmetadata
        # This may happen since the archive periodically contains the same file twice
            # in this case the previous file is overwritten (a bit of a time waster)
        ###sqlDel = '''DELETE FROM tblbanddata WHERE granule = %(granule)s
            ####OR dimgname = %(dimgname)s'''
        
        sqlSel = '''SELECT FROM tblmetadata WHERE dimgname = %(dimgname)s'''
        sqlDel = '''DELETE FROM tblmetadata WHERE dimgname = %(dimgname)s'''
        
        #print "The dictionary is: ", metaDict        
        
        #upload the data
        sqlIns = '''INSERT INTO tblmetadata 
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
                    
        
        curs = self.connection.cursor()
        if not overwrite:
            curs.execute(sqlSel, metaDict) #TODO
            
        curs.execute(sqlDel, metaDict)
        curs.execute(sqlIns, metaDict)
        self.connection.commit()
        try:
            self.logger.info("dimgname:    ", metaDict['dimgname'])
            self.logger.info("[Succesfuly added "+ metaDict['dimgname'] + " metadata into tblmetadata.]")
        except:
            print "dimgname:    ", metaDict['dimgname'] ###
            print "[Succesfuly added "+ metaDict['dimgname'] + " metadata into tblmetadata.]"   ###
    
    def createTblMetadata(self):
        """
        Creates a metadata table called *tblmetadata*. It overwrites if *tblmetadata* already exist.
        """

        #TODO - make SRID an option (default to 4326)
        
        name = 'tblmetadata'    
        
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
            try:
                self.logger.debug("The database does not have SRID 4326, adding this now")
            except:
                print "The database does not have SRID 4326, adding this now" 
            srid4326Sql1 = '''INSERT into spatial_ref_sys (srid, auth_name, auth_srid, proj4text, srtext) values '''
            srid4326Sql2 = '''( 4326, 'sr-org', 14, '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs ', 'GEOGCS["GCS_WGS_1984",DATUM'''
            srid4326Sql3 ='''["WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]');'''     
        
            srid4326Insert = srid4326Sql1 + srid4326Sql2 + srid4326Sql3
                      
            curs.execute(srid4326Insert)        
            self.connection.commit()
            
        geoColumnSql = '''SELECT AddGeometryColumn('tblmetadata', 'geom', 4326, 'POLYGON', 2);'''
        
        #Now setup the primary key and index on dimgname
        alterSql = 'ALTER TABLE tblmetadata OWNER TO postgres, ADD PRIMARY KEY (granule), ADD UNIQUE (dimgname);'
        ###alterSql = 'ALTER TABLE tblmetadata OWNER TO postgres, ADD PRIMARY KEY (dimgname);'
        indexSql = 'CREATE INDEX tblmetadata_geom_gist ON tblmetadata USING gist (geom);'

        curs.execute(geoColumnSql)
        curs.execute(alterSql)
        curs.execute(indexSql)
        self.connection.commit()
        try:
            self.logger.info("tblmetadata created")
        except:
            print "tblmetadata created"
        
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
        
        #pdb.set_trace()
        # go to shapefile directory        
        os.chdir(os.path.join(archDir))
        sats = glob.glob('*.last')
        srid = self.dbProj('lcc')  # assuming lcc the proj can be a parameter if required 

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

            '''Note there is another way to do this step using psql utilities.   
                cmdpth1 = 'shp2pgsql.bin -D -c -I -S -s 96718 ' + 
                cmdpth2 = ' tblArchive complete | psql -d complete -U postgres -w'
                cmd1 = cmdpth1 + os.path.join(archdir, rsat2_shp) + cmdpth2'''
                         
              
            try:
                ok = os.system(cmd) 
                if ok == 0: 
                    try:
                        self.logger.info("added shapefile %s." % shpname)
                    except:
                        print "added shapefile %s." % shpname
                else: 
                    try:   
                        self.logger.error("Problem with shapefile %s." % shpname)
                    except:
                        print "Problem with shapefile %s." % shpname
                create="NO"
            except:
                try:
                    self.logger.error("Problem with shapefile %s." % shpname)
                except:
                    print "Problem with shapefile %s." % shpname           
            
        qryAlt = """ALTER TABLE tblArchive ALTER COLUMN \"valid time\"
                TYPE TIMESTAMP USING CAST (\"valid time\" AS timestamp);"""
        qryKey1 = """ALTER TABLE tblArchive DROP CONSTRAINT tblarchive_pk;"""
        qryKey2 = """ALTER TABLE tblArchive ADD CONSTRAINT tblArchive_pk PRIMARY KEY ("file name");"""
           
        #TODO: Rename columns with spaces in them (ex.):   --- Not a pressing issue
            # ALTER TABLE tblarchive RENAME COLUMN "obj name" TO obj_name
        
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
        try:
            self.logger.info("tblArchive updated with " + str(n_imgs[0][0]) + " SAR images")
        except:
            print "tblArchive updated with " + str(n_imgs[0][0]) + " SAR images"     
        
    def qryGetInstances(self, granule, roi, spatialrel, proj):
        """
        Writes a query to fetch the instance names that are
        associated spatially in the relational table.
        
        **Parameters**
        
            *granule*    : granule name 
            
            *roi*        : region of interest file 
               
            *spatialrel* : spatial relationhip (i.e. ST_Contains or ST_Intersect)
                
            *proj*       : projection name
                
        **Returns**
        
            *instances*  : instances id (unique for entire project, i.e. 5-digit string)
        """
    
        #trelname = self.nameTable(roi, spatialrel)

        # retrieve the dimgname/imgref1 by querying granule that maps to the wanted imgref1
        curs = self.connection.cursor()
        curs.execute("SELECT dimgname FROM tblmetadata WHERE granule = '%s'", (AsIs(granule),))
        dimgname = curs.fetchone()[0]
        dimgname = "%" + dimgname + "%"

        # retrieve all the instances of polygons that relate to image
        #param = {'granule': granule}
        #sql_inst = 'SELECT inst FROM '+trelname+' WHERE granule=%(granule)s'
        
        #curs.execute(sql_inst, param)
        curs.execute("SELECT inst FROM ROI_NTAI_Flux WHERE imgref1 LIKE '%s'", (AsIs(dimgname),))
        result = curs.fetchall()

        instances = []
        for i in range(len(result)):
            instances.append(result[i][0])
        
        return instances
        
    def nameTable(self, roi, spatialrel):
        """
        Automatically gives a name to a relational table
        
        **Parameters**
        
            *roi*        : region of interest 
            
            *spatialrel* : spatial relationship (i.e. ST_Contains or ST_Intersect)
                            
        **Returns**
        
            *name*       : name of the table
        """
        
        if spatialrel == 'ST_Intersects':
            spat = '_int'
        if spatialrel == 'ST_Contains':
            spat = '_con'
        
        name = 'trel'+roi+'img'+spat
        
        return name
        
    def updateROI(self, inFile, path, proj, ogr=False):   #Where are these required and optional fields?
        """
        This function will update an ROI (Region Of Interest) table in the database. It has a prescribed format
        It will take the shapefile named inFile and update the database with the info
        
        Note that this will overwrite any table named *inFile* in the database

        The generated table will include a column *inst* - a unique identifier created by 
        concatenating obj and instid

        **Parameters**
        
            *inFile*   : basename of a shapefile (becomes an roi table name too)
            
            *path*     : full path to inFile
                
            *proj*     : projection name
           
        **Required**
        
            *obj*      - the id or name of an object/polygon that defines a region of interest. Very systematic, no spaces. 
            
            *instid*   - a number to distinguish repetitions of each obj in time or space.  For example an ROI that occurs several summers would have several instids.            
            
            *fromdate* - a valid iso time denoting the start of the ROI - can be blank if imgref is used            
            
            *todate*   - a valid iso time denoting the start of the ROI - can be blank if imgref is used
   
        **Optional**
            
            *imgref*   - a reference image dimage name (for a given ROI) - this can be provided in place of datefrom and dateto                                 
            
            *name*     - a name for each obj (Area51_1950s, Target7, Ayles)            
            
            *comment*  - a comment field
            
            Any other field can be added...            
        """
    
        srid = self.dbProj(proj) 
        outTable = inFile
        
        #pdb.set_trace()
        #Create / recreate the table in the database
        if ogr:
            
            cmd = "ogr2ogr --config PG_USE_COPY YES -f PGDump "\
                +"-lco GEOMETRY_NAME=geom -lco DROP_TABLE=IF_EXISTS -lco SRID="\
                + str(srid) + " /vsistdout/ " + inFile +".shp "+ inFile +  \
                " | psql -h "+self.host+" -d "+self.dbName+" -f -"
        else:
            #Note there is another way to do this step using psql utilities.
            # pay attention to mulitpolygon vs polygon.  -S may be helpful 
            cmd = 'shp2pgsql -D -d -I -s '+ str(srid) +" "+ inFile +".shp "\
            + " | psql -h "+self.host+" -d "+self.dbName+" -f -"
        

        ok = os.system(cmd)

        
        if ok == 0:
            try:
                self.logger.info("added shapefile %s." % inFile)
            except:
                print "added shapefile %s." % inFile
        else: 
            try:
                self.logger.error("Problem with shapefile %s." % inFile)
            except:
                print "Problem with shapefile %s." % inFile
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
            try:
                self.logger.error("Can't query the database")
            except:
                print "Can't query the database"
               
        #curs.execute(qryNewColInst)
            
        curs.execute(qryUpdateInstField)      
        curs.execute(qryLongerDate)
        #pdb.set_trace()
        if len(field) != 0: 
            curs.execute('UPDATE ' + outTable+ qryImgRefFromDate)  #TODO test this
            curs.execute('UPDATE ' + outTable+ qryImgRefToDate)
        curs.execute(qryCastDate)
        self.connection.commit()
        
        curs.execute(qryKey1)
        self.connection.commit()
        curs.execute(qryKey2)
        self.connection.commit()
        
        try:
            self.logger.info("Database updated with " + str(n_rows[0][0]) + " ROI polygons")
        except:
            print "Database updated with " + str(n_rows[0][0]) + " ROI polygons"
        
    def dbProj(self, proj):
        """
        Relates *proj* the name (ie. proj.wkt) to *proj* the number (i.e. srid #).
        from Metadata import Metadata
        **Parameters**
            
            *proj* : projection name
                
        **Returns**
            
            *srid* : spatial reference id number of that projection
        """
    
        if proj == 'wgs84' or proj == 'nil':
            srid = 4326
        elif proj == 'aea' or proj == 'aeaIS':
            srid = 999
        elif proj == 'lcc' or proj == 'cis_lcc':
            srid = 96718
        else:
            try:
                self.logger.error('srid not yet defined... update database table')
            except:
                print 'srid not yet defined... update database table'
            srid = None
        return srid        

    def qryFromText(self, sql, output=False):
        """
        Runs a query in the current databse by sending an sql string  
           
        **Note:** do not use % in the query b/c it interfers with the pyformat protocol
        used by psycopg2; also be sure to triple quote your string to avoid escaping single quotes;
        IF EVER THE Transaction block fails, just conn.rollback();try to use pyformat for queries - see dbapi2 (PEP);
        you can format the SQL nicely with an online tool - like SQLinForm
        
        **Parameters**
            
            *sql*    : the sql text that you want to send
            
            *output* : make true if you expect/want the query to return results
            
        **Returns**
           
           The result of the query as a tupple containing a numpy array and the column names as a list (if requested and available)
        """
        
        #pdb.set_trace()
        curs = self.connection.cursor()
        try:
            curs.execute(sql)
            if output:
                rows = curs.fetchall()
                colnames = [desc[0] for desc in curs.description]
                
        except ValueError, e:
            try:
                self.logger.error('ERROR(value): Confirm that the values sent or retrieved are as expected--> ' +str(e))
            except:
                print 'ERROR(value): Confirm that the values sent or retrieved are as expected--> ' +str(e)
            self.connection.rollback() #from Metadata import Metadatalback()
            
        except psycopg2.ProgrammingError, e:
            try:
                self.logger.error('ERROR(programming): Confirm the SQL statement is valid--> ' +str(e))
            except:
                print 'ERROR(programming): Confirm the SQL statement is valid--> ' +str(e)
            self.connection.rollback()
            
        except StandardError, e:
            try:
                self.logger.error('ERROR(standard): ' +str(e))
            except:
                print 'ERROR(standard): ' +str(e)    
            self.connection.rollback()
            
        else:
            self.connection.commit()
            try:
                self.logger.debug('Query sent successfully')
            except:
                print 'Query sent successfully'
            if output:
                return(numpy.asarray(rows), colnames)       
        
    def qryFromFile(self, fname, path, output=False):
        """
        Runs a query in the current databse by opening a file - adds the path and 
        .sql extension - reading contents to a string and running the query
                
        **Note:** do not use % in the query b/c it interfers with the pyformat protocol
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
            
    def qrySelectFromArchive(self, roi, spatialrel, proj):
        """
        Given a table name (with polygons, from/todates), determine the scenes that cover the area
        from start (str that looks like iso date) to end (same format).
        
        Eventually include criteria:
            subtype - a single satellite name: ALOS_AR, RADAR_AR, RSAT2_AR (or ANY)        
            beam - a beam mode
    
        comes back with - a list of images+inst - the bounding box
        
        **Parameters**
            
            *roi*        : region of interest table in the database                

            *spatialrel* : spatial relationship (i.e. ST_Contains or ST_Intersect)                

            *proj*       : projection name
                
        **Returns**

            *copylist* : a list of image catalog ids                

            *instimg*  : a list of each instance and the images that correspond
        """
        
        #pdb.set_trace()
        tblROI = roi
    
        assert spatialrel.lower() == 'st_contains' or spatialrel.lower() == 'st_intersects'
       
        curs = self.connection.cursor()       

        curs.execute('SELECT inst, fromdate, todate FROM ' + tblROI + ';')
        
        instances = curs.fetchall()
          
        
        n_poly = len(instances)
    
        instimg = [] # make a list of dictionaries to create the instimg table
        copyfiles = [] # make a list to contain all the files to copy
        
        # The query is set up to take the most recent addition to the archive of 
            # the SAME file - ie the date/time and satellite match fully, however, it 
            # is desirable to match on more than one image as the time span allows.
        #Note DO NOT do this comparison in WGS84-lat.lon  - use a projection)
        sql1 = """SELECT DISTINCT ON (substring("file name", 1, 27)) 
            "file name", "file path", "subtype", "beam mode", "valid time", "catalog id" """
        sql2 = 'FROM tblArchive, ' +tblROI+ ' '
        sql3 = 'WHERE ' + tblROI + '.inst = %(inst)s '
        sql4 = 'AND "valid time" >= %(datefrom)s '
        sql5 = 'AND "valid time" <= %(dateto)s '
        sql6 = 'AND ' + spatialrel + ' '
        sql7 = '(ST_Transform(tblArchive.geom, %(srid)s), ST_Transform('
        sql8 =  tblROI+'.geom, %(srid)s)) '
        sql9 = """ORDER BY substring("file name", 1, 27), "file name" DESC"""
        qry = sql1 + sql2 + sql3 + sql4 + sql5 + sql6 + sql7 + sql8+ sql9
    
        for i in range(n_poly): # for each polygon in tblROI, get images that
          
            inst = instances[i][0]
                         
            datefrom = instances[i][1].strftime('%Y-%m-%d %H:%M:%S')
            
            
            #allow for truncation errors
            dateto = instances[i][2] + datetime.timedelta(seconds=1) 
            dateto = dateto.strftime('%Y-%m-%d %H:%M:%S')
            srid = self.dbProj(proj) 

            param = {'inst': inst, 'datefrom' : datefrom, 
                     'dateto' : dateto, 'srid': srid}
                       
            
            curs.execute(qry, param)
            rows = curs.fetchall()
    
            #Changed to instid
            try:
                self.logger.debug("Found ", len(rows), " images associated with instance number " + inst)
            except:
                print "Found ", len(rows), " images associated with instance number " + inst
        
            for i in range(len(rows)):
    
                granule= rows[i][0]
                catid= rows[i][5]
                acTime= rows[i][4]
                #path = rows[i][1]
    
                copyfiles.append(catid)
                
                instimg.append({"inst": inst, "granule": granule, "catid": catid, "time": acTime})
    
        # make sure there are no repeat occurrances of the files to copy
        copylist = dict.fromkeys(copyfiles).keys()
        
        copylist.sort()
        copylist.reverse()
        
        return copylist, instimg

    def instimg2db(self, roi, spatialrel, instimg, mode='refresh'):
        """
        There can be several relational tables that contain the name of an image
        and the feature that it relates to:
        For example:  a table that shows what images intersect with general areas or
        a table that lists images that contain ROI polygons...
            
        This function runs in create mode or refresh mode
        Create - Drops and re-creates the table
        
        Refresh - Adds new data (leaves the old stuff intact)
        
        **Parameters**
            
            *roi*        : region of interest table                

            *spatialrel* : spatial relationship (i.e. ST_Contains or ST_Intersect)                

            *instimg*    : a list of only images of that instance id                

            *copylist*   : a list of images + inst                

            *mode*       : create or refresh mode
        """
        
        name = self.nameTable(roi, spatialrel)
        
        assert mode == 'refresh' or mode == 'create', 'Wrong mode!'
        
        curs = self.connection.cursor()
        
        if mode == 'create':
            sql1 = 'CREATE TABLE ' + name
            sql2 = ' (inst character varying NOT NULL, granule character varying NOT NULL, '
            sql3 = 'CONSTRAINT ' + name +'_pkey PRIMARY KEY (inst, granule)) WITH (OIDS=FALSE);'
            #sql4 = 'ALTER TABLE ' + name + ' OWNER TO postgres;'
                
            curs.execute('DROP TABLE IF EXISTS ' + name) # overwrites !!!
            curs.execute(sql1+sql2+sql3)
            #curs.execute(sql4)
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
        
        try:
            self.logger.info("Uploaded new data to " + name)
        except:
            print "Uploaded new data to " + name

    def instimgExport(self, instimg, fname):
        """
        Saves the instimg listing as a csv file named fname.csv in the current dir.

        **Parameters**
           
           *instimg* : a list of images and where they cover                 

            *fname*   : filename to write to
        """  
        
        tmp = pd.DataFrame.from_dict(instimg)
        for col in tmp.columns:
            if tmp[col].dtype == 'O' or tmp[col].dtype == 'S':
                tmp[col] = tmp[col].str.rstrip()  # somehow there are plenty of spaces in some cols
        tmp.to_csv(fname,index=False)
        
    
    def copylistExport(self, copylist, fname):
        """
        Saves the copylist as a text file named fname.txt in the current dir.

        **Parameters**
           
           *copylist* : a list of images - catalog ids or files               

            *fname*    : filename to write copylist to
        """
        
        fout = open(fname+".txt", 'w')
    
        for line in copylist:
            fout.write(line+'\n')
        
        fout.close()

    def copylistImport(self, fname):
        """
        Reads the copylist text file named fname.txt in the current dir.
        
        **Parameters**
           
           *fname*        : filename to read copylist from
                
        **Returns**
            
            *new_copylist* : a list of images + inst
        """
        
        new_copylist = []
        fp = open(fname+".txt", 'r')
    
        for line in fp:
            new_copylist.append(line.rstrip('\n'))
        
        fp.close()
        return new_copylist  
    
    def copyfiles(self, copylist, wrkdir):
        """
        Copies files from cisarchive.  If file could not be found, check that the 
        drive mapping is correct (above).
        
        **Parameters**
            
            *copylist* : a list of images + inst             

            *wrkdir*   : working directory
        """
        
        for fname in copylist:
            destination = os.path.join(wrkdir,os.path.split(fname)[1])
            if not os.path.isfile(fname):
                try:
                    self.logger.error(fname + ' does not exist')
                except:
                    print fname + ' does not exist'
                continue        
            if os.path.isfile(destination):
                try:
                    self.logger.debug(destination + " exists already and will not be copied")
                except:
                    print destination + " exists already and will not be copied"
            else:
                shutil.copy(fname,wrkdir)
                try:
                    self.logger.debug("Copied File " + fname)
                except:
                    print "Copied File " + fname
        try:
            self.logger.info("Finished File Copy")
        except:
            print "Finished File Copy"
        
    def qrySelectFromLocal(self, roi, spatialrel, proj):
        """
        Determines the scenes that cover the area (spatialrel = contains or intersects)
        from start (str that looks like iso date) to end (same format).
        
        Eventually include criteria:
        
            subtype - a single satellite name: ALOS_AR, RADAR_AR, RSAT2_AR (or ANY)           
           
           beam - a beam mode
    
        comes back with - a list of images+inst - the bounding box
        
        **Parameters**
            
            *roi*        : region of interest                

            *spatialrel* : spatial relationship (i.e. ST_Contains or ST_Intersect)                

            *proj*       : projection
                
        **Returns**
           
            *copylist*   : a list of image names including full paths                

            *instimg*    : a list of the images and their corresponding instance ids
        """
        
        #TODO make this tblmetadata
        
        #tblROI = 'tbl'+roi
        tblROI = 'tblroi'
    
        assert spatialrel.lower() == 'st_contains' or spatialrel.lower() == 'st_intersects'
       
        curs = self.connection.cursor()

        curs.execute('SELECT inst, fromdate, todate FROM ' + tblROI + ';')
        
        instances = curs.fetchall()
          
        n_poly = len(instances)
        instimg = [] # make a list of dictionaries to create the instimg table
        copyfiles = [] # make a list to contain all the files to copy
    
        for i in range(n_poly): # for each polygon in tblROI, get images that
            #Note DO NOT do this comparison in WGS84-lat.lon  - use a projection)
            
            # The query is set up to take the most recent addition to the archive of 
            # the SAME file - ie the date/time and satellite match fully, however, it 
            # is desirable to match on more than one image as the time span allows.

            sql1 = """SELECT DISTINCT ON (substring("granule", 1, 27)) 
                "granule", "location", "sattype", "beam", "acdatetime" """
            sql2 = 'FROM tblMetadata, ' +tblROI+ ' '
            sql3 = 'WHERE ' + tblROI + '.inst = %(inst)s '
            sql4 = 'AND "acdatetime" >= %(datefrom)s '
            sql5 = 'AND "acdatetime" <= %(dateto)s '
            sql6 = 'AND ' + spatialrel + ' '
            sql7 = '(ST_Transform(tblMetadata.geom, %(srid)s), ST_Transform('
            sql8 =  tblROI+'.geom, %(srid)s)) '
            sql9 = """ORDER BY substring("granule", 1, 27), "granule" DESC"""
            qry = sql1 + sql2 + sql3 + sql4 + sql5 + sql6 + sql7 + sql8+ sql9
            
            inst = instances[i][0]
                         
            datefrom = instances[i][1].strftime('%Y-%m-%d %H:%M:%S')        
            
            #allow for truncation errors
            ###dateto = instances[i][2] + datetime.timedelta(seconds=1) 
            dateto = instances[i][2].strftime('%Y-%m-%d %H:%M:%S')    ###
            ###dateto = dateto.strftime('%Y-%m-%d %H:%M:%S')
            srid = self.dbProj(proj) 
            
            param = {'inst': inst, 'datefrom' : datefrom, 
                     'dateto' : dateto, 'srid': srid}
                       
            curs.execute(qry, param)
            rows = curs.fetchall()
           
            try:
                self.logger.debug("Found ", len(rows), " images associated with instance number " + inst)
            except:
                print "Found ", len(rows), " images associated with instance number " + inst
    
            for i in range(len(rows)):
    
                catalogid = rows[i][0]
                #Get the location:
                #location= rows[i][1]

        #Get the obj name, file name and file size
        filename = rows[i][2]
        filesize = rows[i][3]
        obj = rows[i][4]
        instid = rows[i][5]
        notes = rows[i][6]
        area = rows[i][7]
                
                #error handler
                #path = os.path.join(newdrive,dir,granule)
            
        copyfiles.append({"catalogid": catalogid, "filename": filename, "filesize": filesize, "obj": obj, "instid": instid, "notes": notes, "area": area})
                
                #instimg.append({"inst": inst, "catalogid": catalogid})
        instimg.append({"catalogid": catalogid})
    
    
        # make sure there are no repeat occurrances of the files to copy
        #copylist = dict.fromkeys(copyfiles).keys()
        
        #copylist.sort()
        #copylist.reverse()
        #print copylist
        #print instimg

        return copyfiles, instimg

    def qryCropZone(self, granule, roi, spatialrel, proj, inst):
        """
        Writes a query to fetch the bounding box of the area that the inst polygon and
        image in question intersect.
        returns a crop ullr tupple pair in the projection given
        
        **Parameters**
            
            *granule*    : granule name                

            *roi*        : region of interest file                

            *spatialrel* : spatial relationship (i.e. ST_Contains or ST_Intersect)                

            *proj*       : projection name                

            *inst*       : instance id (i.e. a 5-digit string)
                
        **Returns**
            
            *ullr*       : upper left, lower right tupple pair in the projection given
        """
        
        #tblroi = 'tbl'+roi
        tblroi = roi
        #trelname = self.nameTable(roi, spatialrel)
        srid = self.dbProj(proj)
        #param = {"srid": AsIs(srid), "granule": AsIs(granule), "inst": AsIs(inst)}
    
        #Changed geom to geom    
    
#==============================================================================
#         sql1 = 'SELECT ST_AsText(ST_Envelope(ST_Intersection((SELECT ST_Transform' +\
#             '(tblArchive.geom, %(srid)s) FROM tblArchive '+\
#             'WHERE "file name"= %(granule)s),'+\
#             '(SELECT ST_Transform('
#         sql2 = """.geom, %(srid)s) FROM """
#         sql3 = """ INNER JOIN """
#         sql4 = """ ON """
#         sql5 = """.inst = """
#         sql6 = """.inst WHERE """
#         sql7 = """.granule = %(granule)s AND """
#         sql8 = """.inst = %(inst)s))))"""
#         sql = sql1+ tblroi +sql2+ trelname +sql3+ tblroi +sql4+ trelname +sql5+ \
#             tblroi +sql6+ trelname +sql7+ trelname +sql8
#==============================================================================
    

        sql1 = "SELECT ST_AsText(ST_Envelope(ST_Intersection((SELECT ST_Transform (tblmetadata.geom, '%s') FROM tblmetadata WHERE granule = '%s'), (SELECT ST_Transform("
        sql2 = ".geom, '%s') FROM "
        sql3 = " WHERE inst = '%s'))))"
    
        sql = sql1+ tblroi +sql2+ tblroi +sql3
    
        curs = self.connection.cursor()
        curs.execute(sql, (AsIs(srid), AsIs(granule), AsIs(srid), AsIs(inst)))
        #bbtext = curs.fetchone()[0]
        #print "ST_AsText: " + bbtext
        
        bbtext = curs.fetchall()
        
        #parse the text to get the pair of tupples
        bbtext = bbtext[0][0]  #slice the piece you need
        #print bbtext
        
        if bbtext == 'GEOMETRYCOLLECTION EMPTY' or bbtext == None:
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
            #print ullr[0:2]
#==============================================================================
#             ul = bbtext.split(',')[1]            
#             lr = bbtext.split(',')[3]            
#             ullr = (ul.split()[0], ul.split()[1]), (lr.split()[0], lr.split()[1])
#==============================================================================
            
        return ullr

    def qryMaskZone(self, granule, roi, spatialrel, proj, inst):
        """
        Writes a query to fetch the gml polygon of the area that the inst polygon and
        image in question intersect.
        returns gml text but also saves a file... mask.gml in the current dir
        
        **Parameters**
            
            *granule*    : granule name               

            *roi*        : region of interest file                

            *spatialrel* : spatial relationship (i.e. ST_Contains or ST_Intersect)                

            *proj*       : projection name                

            *inst*       : instance id (i.e. a 5-digit string)
                
        **Returns**
            
            *polytext*  :   gml text
        """
        
        tblroi = 'tbl'+roi
        trelname = self.nameTable(roi, spatialrel)
        srid = self.dbProj(proj)
        param = {'srid': srid, 'granule': granule, 'inst': inst}
    
            
        sql1 = 'SELECT ST_AsText(ST_Intersection((SELECT ST_Transform' +\
            '(tblArchive.geom, %(srid)s) FROM tblArchive '+\
            'WHERE "file name"= %(granule)s),'+\
            '(SELECT ST_Transform('
        sql2 = """.geom, %(srid)s) FROM """
        sql3 = """ INNER JOIN """
        sql4 = """ ON """
        sql5 = """.inst = """
        sql6 = """.inst WHERE """
        sql7 = """.granule= %(granule)s AND """
        sql8 = """.inst = %(inst)s)))"""
    
        sql = sql1+ tblroi +sql2+ trelname +sql3+ tblroi +sql4+ trelname +sql5+ \
            tblroi +sql6+ trelname +sql7+ trelname +sql8
        #print sql
    

        curs = self.connection.cursor()
        curs.execute(sql, param)
        polytext = curs.fetchall()[0][0]

    
        if polytext == 'GEOMETRYCOLLECTION EMPTY':
            polytext = 0
        return polytext

    def imgData2db(self, imgData, xSpacing, ySpacing, bandName, inst, dimgname, granule):
        """
        Here are the data in an array... upload to database
        need the imgData, the imgType, the bandName, the inst, dimgname and granule
        
        will compute the count, mean, std, min, max for non-zero elements
        and send them to db as well

        **Parameters**
            
            *imgData*   :               

            *xSpacing*  :                

            *ySpacing*  :                

            *bandName*  :                   

            *inst*      : instance id (i.e. a 5-digit string)                

            *dimgname*  : Derek's image name                

            *granule*   : granule name
        """
        
        noDataVal = 0
        #polyData = numpy.ma.masked_equal(imgData, noDataVal) #doesn't work all the time
        polyData = imgData[numpy.where( imgData != noDataVal )]
        #make histogram; note: hist is not masked array aware!
        # this histogram is in log scale - dB units
        bins = numpy.arange(-50, 10, 1) # every 1 dB... 
        histData, bins =numpy.histogram(Util.getdBScale(polyData), 
                                           bins, normed=True)
        
        sql_array = self.numpyhql(imgData, 2)
        sql_histData = self.numpy2sql(histData, 1)
        sql_binData = self.numpy2sql(bins, 1)
        
        upload = {
            'granule' : granule,
            'bandname' : bandName,
            'inst' : inst,
            'dimgname' : dimgname,
            'n_cols' : imgData.shape[1],
            'n_rows' : imgData.shape[0],
            'mean' : str(polyData.mean()),  # convert real or get can't adapt error
            'var' :  str(polyData.var()),
            'n_pixels' : len(polyData), #count the pixels within the polygon
            'maxdata' : str(polyData.max()), 
            'mindata' : str(polyData.min()),
            'median' : str(numpy.median(polyData)),
            'quart1' : str(stats.scoreatpercentile(polyData, 25)),
            'quart3' : str(stats.scoreatpercentile(polyData, 75)),
            'skew' : str(stats.skew(polyData, None)),
            'kurtosis' : str(stats.kurtosis(polyData, None)),
            'hist' : sql_histData,
            'bin' : sql_binData,
            'xSpacing' : str(xSpacing), 
            'ySpacing' : str(ySpacing),
            'banddata' : sql_array
            }
        
        #First, look to see if primary key exists, if so, overwrite record
        sqlDel = '''DELETE FROM tblbanddata WHERE bandname = %(bandname)s 
            AND granule = %(granule)s AND inst = %(inst)s'''
                
        sqlIns = '''INSERT INTO tblbanddata 
            (granule, bandname, inst, dimgname, n_cols, n_rows, mean, var, n_pixels, 
            maxdata, mindata, median, quart1, quart3, skew, kurtosis, hist, bin, 
            xSpacing, ySpacing, banddata) 
            VALUES 
            (%(granule)s, %(bandname)s, %(inst)s, %(dimgname)s, %(n_cols)s, 
            %(n_rows)s, %(mean)s, %(var)s, %(n_pixels)s, %(maxdata)s, %(mindata)s, 
            %(median)s, %(quart1)s, %(quart3)s, %(skew)s, %(kurtosis)s, %(hist)s, 
            %(bin)s, %(xSpacing)s, %(ySpacing)s, %(banddata)s)'''
             

        curs = self.connection.cursor()
        try:
            curs.execute(sqlDel, upload)
        except:
            try:
                self.logger.error(curs.query())
            except:
                print curs.query()
        try:
            curs.execute(sqlIns, upload)
        except: 
            try:
                self.logger.error(curs.query())
            except:
                print curs.query()
        self.connection.commit()
        try:
            self.logger.info('Image ' + bandName + ' data uploaded to database')
        except:
            print 'Image ' + bandName + ' data uploaded to database'
        
        
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
    
    
    def sql2numpy(self, sqlArray, dtype='float32'):
        """
        Comming from SQL queries, arrays are stored as a list (or list of lists)
        Defaults to float32
        
        **Parameters**
            
            *sqlArray* : an sql friendly array               

            *dtype*    : default type (float32)
                
        **Returns**
            
            *list*     : list containing the arrays
        """
        
        list = numpy.asarray(sqlArray, dtype=dtype) 
        return list
        
        
    def update_NTAI_FLUX_ROI(self, inFile, path, proj):
        """
        Goes to a shapefile named inFile and updates the postGIS database
        Assumes dbase postgis exists and that outTable does as well - this overwrites!
    
        **Parameters**
            
            *inFile* : basename of a shapefile           

            *path*   : full path to inFile               

            *proj*   : projection name
            
        **Required**
            
            name, inst, obj, type, fromdate, todate(optional)            

            fromdate - a valid time_start (ie it is here, when was it here?)            

            todate   - a valid time_end (ie it is here, when was it here?)            

            inst     - an instance id (unique for entire project) - Nominally a 5-digit string
    
        **Optional**
            
            refimg   - a reference image name (ie how do you know it was here)           

            type     - an ice type (ie ice island, ice shelf, mlsi, fyi, myi, epishelf, open water)            

            subtype  - an ice subtype (ie ice island could be iced firn, basement; open water could be calm, windy)            

            comment  - a comment field            

            name     - a name (Target7, Ayles)            

            obj      - an object id tag (to go with name but very systematic: 2342, and if it splits 2342_11 & 2342_12)
        """
    
        srid = self.dbProj(proj)
        outTable = 'public.tbl'+inFile
        
    
        #issue with shp2pgsql use shp2pgsql.bin instead
        cmdpth1 = 'shp2pgsql.bin'
        cmdpth2 = ' -D -I -s '           
        ###cmdpth3 = ' complete |'
        cmdpth3 = 'testROI |'
            ###cmdpth4 = ' psql -d complete -U postgres -w'
        cmdpth4 = ' psql -d testROI -U postgres -w'
        '''
        cmdpth3 = ' postgis |'
        cmdpth4 = ' psql -d postgis'
        '''
        
        cmd = cmdpth1 + ' -d' + cmdpth2 + str(srid)+' '+ os.path.join(path, inFile+'.shp ') + outTable +\
                cmdpth3 + cmdpth4
        os.system(cmd)
    
        #print "Done."
        #print "Database updated with " + str(n_rows[0][0]) + " ROI polygons"


    def customizedQuery(self, attributeList, roi, spatialrel, proj):
        """
        Customizable query that takes an list of attributes to search for, a roi, a spatialrel, and a proj
        and returns a dictionary with all the requested attributes for the results that matched the query
        
        **Parameters**
            
            *attributeList* :                

            *roi*           :                

            *spatialrel*    :                

            *proj*          :
                
        **Returns**

            *copylist*      :                

            *instimg*       :
        """
        
        tblROI = 'tbl'+roi
    
        assert spatialrel.lower() == 'st_contains' or spatialrel.lower() == 'st_intersects'
       
        curs = self.connection.cursor()
        

        curs.execute('SELECT inst, fromdate, todate FROM ' + tblROI + ';')
        
        instances = curs.fetchall()
          
        
        n_poly = len(instances)
    
        instimg = [] # make a list of dictionaries to create the instimg table
        copyfiles = [] # make a list to contain all the files to copy
          
        unmodifiedAttibutes = copy.deepcopy(attributeList)
        
        #Format for query
        for i in range (len(attributeList)):
            attributeList[i] = "\"" + attributeList[i] + "\""
        formattedAttibutes =  ', '.join(attributeList)
        
        #Add inst for joining in dict
        dictAttributes = ['inst'] + unmodifiedAttibutes
                

    
        for i in range(n_poly): # for each polygon in tblROI, get images that
    
            #Note DO NOT do this comparison in WGS84-lat.lon  - use a projection)
            
            # The query is set up to take the most recent addition to the archive of 
            # the SAME file - ie the date/time and satellite match fully, however, it 
            # is desirable to match on more than one image as the time span allows.
        
            sql1 = """SELECT DISTINCT ON (substring("granule", 1, 27)) """ + formattedAttibutes
            
            
            sql2 = 'FROM tblMetadata, ' +tblROI+ ' '
            sql3 = 'WHERE ' + tblROI + '.inst = %(inst)s '
            sql4 = 'AND "acdatetime" >= %(datefrom)s '
            sql5 = 'AND "acdatetime" <= %(dateto)s '
            sql6 = 'AND ' + spatialrel + ' '
            sql7 = '(ST_Transform(tblMetadata.geom, %(srid)s), ST_Transform('
            sql8 =  tblROI+'.geom, %(srid)s)) '
            sql9 = """ORDER BY substring("granule", 1, 27), "granule" DESC"""
            qry = sql1 + sql2 + sql3 + sql4 + sql5 + sql6 + sql7 + sql8+ sql9
            
            
            inst = instances[i][0]
                         
            datefrom = instances[i][1].strftime('%Y-%m-%d %H:%M:%S')
            
            
            #allow for truncation errors
            dateto = instances[i][2] + datetime.timedelta(seconds=1) 
            dateto = dateto.strftime('%Y-%m-%d %H:%M:%S')
            srid = self.dbProj(proj) 

            param = {'inst': inst, 'datefrom' : datefrom, 
                     'dateto' : dateto, 'srid': srid}
    
            curs.execute(qry, param)
            rows = curs.fetchall()
            
            try:
                self.logger.debug("Found ", len(rows), " images associated with instance number " + inst)
            except:
                print "Found ", len(rows), " images associated with instance number " + inst
        
        
            for i in range(len(rows)):
    
                #Add inst to the start of row [i]
                rowList = [inst] + list(rows[i])          
                
                #granule = rows[i][1]
                #Get the location:
                location= rows[i][2]               
                
                instimg.append(dict(zip(dictAttributes, rowList)))
                
                #error handler
                #path = os.path.join(newdrive,dir,granule)
    
                copyfiles.append(location)
                       
        # make sure there are no repeat occurrances of the files to copy
        copylist = dict.fromkeys(copyfiles).keys()
        
        copylist.sort()
        copylist.reverse()
        
        return copylist, instimg

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

    def beaconShapefilesToTables(self, dirName):
        """
        Takes a directory containing beacon shape files and converts them to tables and 
        inserts them into the database appending *beacon_* before the name
        
        **Parameters**
           
           *dirName* :
        """
        
        os.chdir(dirName)        
        
        #Go through and get the names of all the shapefiles in the directory
        shapefiles=[]
        
        for sFile in glob.glob("*.shp"):
            shapefiles.append(sFile)
        try:
            self.logger.info("shape files are", shapefiles)
        except:
            print "shape files are", shapefiles
        
        #issue with shp2pgsql use shp2pgsql.bin instead
        #cmdpth1 = 'shp2pgsql.bin'
        #cmdpth2 = ' -D -I -s 4326'           
        #cmdpth3 = ' |'
        #cmdpth4 = ' psql -d complete -U postgres -w'
        #cmd = cmdpth1 + ' -d' + cmdpth2 +' '+ os.path.join(dirName, shape) + " beacon_" + shape[:-4] +\
        #        cmdpth3 + cmdpth4
        
        for shpname in shapefiles:

            try:
                ok = os.system("ogr2ogr -f PostgreSQL PG:'host="+ self.host +" dbname=" + self.dbName + \
                "' -a_srs EPSG:4326 -nln beacon_" + shpname[:-4] + " " + shpname +" "+ shpname[:-4]) 
                qryAlt = """ALTER TABLE beacon_""" + shpname[:-4] +\
                    """ ALTER COLUMN gps_time TYPE TIMESTAMP USING CAST (gps_time AS timestamp);"""
                self.qryFromText(qryAlt)
                if ok == 0: 
                    try:
                        self.logger.debug("added shapefile %s." % shpname)
                    except:
                        print "added shapefile %s." % shpname
                else:
                    try:
                        self.logger.error("Problem with shapefile %s." % shpname)
                    except:
                        print "Problem with shapefile %s." % shpname                  
            except:
                try:
                    self.logger.error("Problem with shapefile %s." % shpname)
                except:
                    print "Problem with shapefile %s." % shpname           
 
    def bothArchiveandMetadata(self):
        """
        Finds all the results in both the archive and tblmetadata.
        """
        
        sql1 = """SELECT granule, "file name", "beam mode", beam, satellite  """
        sql2 = 'FROM tblmetadata, tblArchive '
        sql3 = 'WHERE '
        sql4 = """tblArchive."valid time" <= tblmetadata.acdatetime + interval '1 second'"""
        sql5 = """AND tblArchive."valid time" >= tblmetadata.acdatetime - interval '1 second'"""
        qry = sql1 + sql2 + sql3 + sql4 + sql5
        
        curs = self.connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
        curs.execute(qry)        
        results = curs.fetchall()
                
        curs = self.connection.cursor() 
        
        sql1 = 'CREATE TABLE ' + 'tblOverlap'
        sql2 = ' ( granule character varying NOT NULL, '
        sql3 = ' "file name" character varying, "beam mode" character varying(5), beam character varying(5), satellite character varying(20), '
        sql4 = 'CONSTRAINT ' + 'tblOverlap' +'_pkey PRIMARY KEY (granule, "file name")) WITH (OIDS=FALSE);'
        sql5 = 'ALTER TABLE ' + 'tblOverlap' + ' OWNER TO postgres;'
            
        curs.execute('DROP TABLE IF EXISTS ' + 'tblOverlap') # overwrites !!!
        curs.execute(sql1+sql2+sql3+sql4)
        curs.execute(sql5)
        self.connection.commit()    
   
        for entry in results:
            #print entry 
            #print entry['beam mode']
                        
            # now put the new data into the database
            curs.execute("""INSERT INTO """+'tblOverlap'+"""(granule, "file name", "beam mode", beam, satellite) VALUES
              (%s, %s, %s, %s, %s) """, (entry['granule'], entry['file name'], entry['beam mode'], entry['beam'], entry['satellite'])) 
            self.connection.commit()
            
        try:
            self.logger.info("Uploaded new data to " + 'tblOverlap')
        except:
            print "Uploaded new data to " + 'tblOverlap'
        
        #print "the results are: ", results
                
    def checkTblArchiveOverLapsTblMetadata(self, filename):
        """
        Check if a file name from tblArchive is in the overlap table.
        
        **Parameters**
            
            *filename*   :
                
        **Returns**
            
            *dictionary* :
        """
        
        curs = self.connection.cursor() 
        
        query = """SELECT granule FROM tblOverlap WHERE "file name" LIKE '""" + filename + "'"     
        
        curs.execute(query)
        result = curs.fetchall()
        
        dictionary = {'dimgname':"", 
                      'polarization':"",
                      'granule':"",
                      'linespacing':"",
                      'acdatetime':"",
                      'location':""}
        
        
        if(len(result) > 0):
            
            for entry in result:            
                
                query = """SELECT dimgname, granule, polarization, linespacing, acdatetime, location  FROM tblMetadata WHERE granule LIKE '""" + entry[0] + "'"
                curs.execute(query)
                e = curs.fetchall()
                
                for i in range(len(e)):

                    #Add inst to the start of row [i]
                    rowList = list(e[i])
                    dictionary = dict(zip(['dimgname', 'granule', 'polarization', 'linespacing', 'acdatetime', 'location'], rowList))
                  
        return dictionary
        
    def alterTimestamp(self, shpTable):
        """
        Takes a shape file and converts the gps_time from character type to timestamp time.
        
        **Parameters**
            
            *shpTable* :
        """
        
        sql1 = "ALTER TABLE " + shpTable + ' '
        sql2 = "ALTER COLUMN gps_time TYPE timestamp USING gps_time::timestamp"
        query = sql1 + sql2
        try:
            self.logger.debug("query is" + query)
        except:
            print "query is" + query
        curs = self.connection.cursor()
        curs.execute(query) 
        self.connection.commit()
        
    def removeHandler(self):
        self.logger.handlers = []
