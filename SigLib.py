# -*- coding: utf-8 -*-
"""
**SigLib.py**

This script will bring together all the SigLib modules with a config script to


**Created on** Mon Oct  7 20:27:19 2013 **@author:** Sougal Bouh Ali
"""

import os
import sys
import signal
import commands
import ConfigParser
import logging
import shutil
from time import localtime, strftime
from glob import glob
import pdb
import argparse #TODO - make this work (see pdfshrink.py)

from Database import Database
from Metadata import Metadata
from Image import Image
import Util

#1afijoptupdb.set_trace()


def syntax():
    sys.stderr.write("""
    SYNTAX: siglib configfile.cfg [zipfile]

    Options:
        zipfile     The name of a single zipfile to work on.
    \n""")
    sys.exit(1)

if len(sys.argv) == 1:
    sys.stderr.write("No input given\n")
    syntax()

class SigLib:
    def __init__(self):
        
        self.cfg = os.path.expanduser((sys.argv[1]))

        config = ConfigParser.RawConfigParser()
        config.read(self.cfg)
        self.cfg = os.path.basename(self.cfg)[:-4]
        self.tmpDir = os.path.abspath(os.path.expanduser(config.get("Directories","tmpDir")))
        self.imgDir = os.path.abspath(os.path.expanduser(config.get("Directories", "imgDir")))
        self.projDir = os.path.abspath(os.path.expanduser(config.get("Directories", "projDir")))
        self.scanDir = os.path.abspath(os.path.expanduser(config.get("Directories", "scanDir")))
        self.vectDir = os.path.abspath(os.path.expanduser(config.get("Directories","vectDir")))
        self.dataDir = os.path.abspath(os.path.expanduser(config.get("Directories","dataDir")))
        self.logDir = os.path.abspath(os.path.expanduser(config.get("Directories","logDir")))
        self.archDir = os.path.abspath(os.path.expanduser(config.get("Directories", "archDir")))
        
        self.scanQuery = config.get("Input", "query")
        self.scanPath = config.get("Input", "path")
        self.scanFile = config.get("Input", "file")
        self.scanFor = config.get("Input", "scanFor")
        self.query = config.get("Input", "sql")
        
        self.dbName = config.get("Database", "db")
        self.dbHost = config.get("Database", "host")
        self.create_tblmetadata = config.get("Database", "create_tblmetadata")

        self.processData2db = config.get("Process", "data2db")
        self.processData2img = config.get("Process", "data2img")
        self.saveMeta = True  #TODO put in cfg
        
        self.mode = "AMPMode"

        self.proj = config.get(self.mode,"proj")
        self.crop = config.get(self.mode,"crop")
        self.mask = config.get(self.mode,"mask")
        self.roi = config.get(self.mode,"roi")
        self.roiProj = config.get(self.mode,"roiProj")
        self.spatialrel = config.get(self.mode,"spatialrel")
        self.imgTypes = ['sigma', 'theta']
        self.imgType = config.get(self.mode,"imgTypes")
        self.imgFormat = config.get(self.mode,"imgFormat")

        self.timeout = 10 # set to 0 for 'off' or a number from 7 to 10 [needs testing]

        self.issueString = ""
        self.count_img = 0            # Number of images processed
        self.bad_img = 0             # Number of bad images processed
        self.starttime = strftime('%Y%m%d_%H%M%S', localtime())
        shutil.copy(os.path.abspath(os.path.expanduser(sys.argv[1])), os.path.join(self.logDir,self.cfg + "_" +\
            self.starttime +'.cfg')) # make a copy of the cfg file

    def createLog(self,zipfile=None):
        """
        Creates log file that will be used to report progress and errors
        **Parameters**
            *zipfile* : a valid zipfile name with full path (optional) for file input
        """        
        if zipfile is not None: 
            self.loggerFileName = os.path.basename(zipfile) + "_" + self.cfg \
                + "_" + strftime('%Y%m%d_%H%M%S', localtime())+".log"
        else:
            self.loggerFileName = self.cfg + "_" + self.starttime+".log"
        #logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger(__name__)
        loghandler = logging.FileHandler(os.path.join(self.logDir,self.loggerFileName))
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')        
        loghandler.setFormatter(formatter)        
        logger.addHandler(loghandler)
        
        logger.setLevel(logging.DEBUG)  # TODO - set a parameter in config file/self.loglevel to change this. 
        
        logger.info("SigLib Run w/ config: %s", self.cfg)
        logger.info("User: %s",os.getenv('USER'))
        

    def logConcatenate(self):
        """
        Looks through log directory for log files associated with this cfg and starttime
        Concatenates them into one master log file
        Removes all the individual log files? 
        Lists images that had an error? (grep through?)
        """
        #TODO

    def handler(self, signum, frame):
        """
        Handles exceptions - most notably time out errors
        """        
        logger = logging.getLogger(__name__)
        logger.error("Image processing is taking longer than usual, stopping/moving to next image")
        self.bad_img += 1
        raise Exception

    def proc_File(self, zipfile):
        """
        Locates a single satellite image zip file and processes it according 
        to the config file.  Note this cannot be nested in proc_dir since the 
        logging structure and other elements must parallelizable
        
        **Parameters**
            *zipfile* : a valid zipfile name with full path
        
        """
        self.createLog(zipfile)
        logger = logging.getLogger(__name__)
        self.count_img += 1
      
        # Setting the handler
        signal.signal(signal.SIGALRM, self.handler)
        # A Zipfile that is ~100 MB takes about 15 min, but it gets much longer as file size increases
        if self.timeout !=0:
            #TODO - investigate the relationship between size and time (look at log and tif timestamps)
            timeout = int(450*2.7**((self.timeout/1e9)*os.path.getsize(zipfile)))  
            signal.alarm(timeout) 
            
        logger.info('Started processing image %s', zipfile)    

        try:
            self.retrieve(zipfile)
            logger.debug('image retrieved')
            # Do clean-up
                        
        except Exception, e:
            logger.error('Image failed %s, due to: %s', zipfile, e, exc_info=True)
            logger.error("Image processing exception, moving to next image")
            self.issueString += "\n\nERROR (exception): " + zipfile
            self.bad_img += 1
        
        os.chdir(self.tmpDir)
        os.system("rm -r " +os.path.splitext(os.path.basename(zipfile))[0])
        logger.debug('cleaned zip dir')

        good_img = self.count_img - self.bad_img
        logger.info("%i images where successfully processed out of %i", good_img, self.count_img)

        if self.bad_img > 0:
            # Write the issue file
            logger.error(self.issueString)


    def proc_Dir(self, path, pattern):
        """
        Locates satelite image raw data files (zipfiles) using a
        *pattern* in *path* search method, and then calls createImg()
        to process the data into image.

        **Parameters**
            *path*  :   directory tree to scan

            *pattern*   :   file pattern to discover
        """

        self.createLog()
        logger = logging.getLogger(__name__)
        ziproots = []           # List of the zip files (dirpath + *.zip: '/xx/yy/zz/*.zip')

        # Returns a list 'ziproots' of the zip files with the specified path and pattern
        for dirpath, dirnames, filenames in os.walk(path):
            ziproots.extend(glob(os.path.join(dirpath,pattern)))

        logger.info('Found %i files in %s matching pattern %s', len(ziproots), path, pattern)
        ziproots.sort() # Nice to have this in some kind of order
        # Process every zipfile in ziproots 1 by 1
        for zipfile in ziproots:
            self.count_img += 1

            # Setting the handler
            signal.signal(signal.SIGALRM, self.handler)
            # A Zipfile that is ~100 MB takes about 15 min, but it gets much longer as file size increases
            if self.timeout !=0:
                #TODO - investigate the relationship between size and time (look at log and tif timestamps)
                timeout = int(450*2.7**((self.timeout/1e9)*os.path.getsize(zipfile)))  
                signal.alarm(timeout) 

            try:
                self.retrieve(zipfile)
                logger.debug('image retrieved') #TODO move to retrieve and return meaningful info. 
            except Exception:
                logger.error('Image failed %s', zipfile)
                logger.error("Image processing exception, moving to next image")
                self.issueString += "\n\nERROR (exception): " + zipfile
                self.bad_img += 1

            # Do clean-up
            os.chdir(self.tmpDir)
            os.system("rm -r " +os.path.splitext(os.path.basename(zipfile))[0])
            logger.debug('cleaned zip dir')

        good_img = self.count_img - self.bad_img
        logger.info("%i images where successfully processed out of %i", good_img, self.count_img)

        if self.bad_img > 0:
            # Write the issue file
            logger.error(self.issueString)

    def retrieve(self, zipfile):
        """
        given a zip file name this function will..... find out what satellite it is, unzip it, ...
        """
        logger = logging.getLogger(__name__)    
        
        # Verify if zipfile has its own subdirectory before unzipping
        unzipdir, granule, nested = Util.getZipRoot(os.path.join(self.scanDir,zipfile), self.tmpDir)
        logger.debug("Zipfile %s will unzip to %s. Granule is %s and Nested is %s", zipfile, unzipdir, granule, nested)        
        # Unzip the zip file into the unzip directory
        Util.unZip(zipfile, unzipdir)
        logger.debug("Unzip ok")

        zipname, ext = os.path.splitext(os.path.basename(zipfile))
        logger.info('Zipname: %s', zipname)
        
        if unzipdir == self.tmpDir:      # If files have been unzipped in their own subdirectory
            unzipdir = os.path.join(self.tmpDir, granule)    # Then correct the name of unzipdir
            if nested == 1:   # If zipfile has nested directories
                unzipdir = os.path.join(unzipdir, granule)    # Then correct the name of unzipdir

        # Parse zipfile
        fname, imgname, sattype = Util.getFilename(granule, unzipdir)

        logger.info("granule:  %s", granule)
        logger.info("sat type:  %s", sattype)

        if fname == "error":
            logger.error("File not valid or available, moving to next file")
            self.issueString += "\n\nERROR (retrieve): " + zipfile    # Take note
            self.bad_img += 1

        else:#begin processing data ...
            sar_meta = Metadata(granule, imgname, unzipdir, zipfile, sattype)   # Retrieve metadata
        
            if sar_meta.status != "ok":       # Meta class unsuccessful
                logger.error("Creating an instance of the meta class failed, moving to next image")
                self.bad_img += 1
                self.issueString += "\n\nERROR (meta class): " + zipfile

            if self.saveMeta:
                sar_meta.saveMetaFile(self.imgDir)
                        
            if self.processData2db == "1":
                db = Database(self.dbName, host=self.dbHost)  # Connect to the database
                self.data2db(sar_meta, db)

            if self.processData2img == "1":
                logger.debug("processing data to image")
                self.data2img(fname, imgname, zipname, sattype, granule, zipfile, unzipdir)


    def data2db(self, meta, db):
        """
        Adds the image file metadata to tblmetadata table in the specified database.
        Will create/overwrite the table tblmetadata if prompted (be carefull)

        **Parameters**
            *meta* :   A metadata instance from Metadata.py
        
            *db*    :   database connection
        """

        if meta.status == "ok":
            meta_dict = meta.createMetaDict()  # Create dictionary of all the metadata fields
            db.meta2db(meta_dict)       # Upload metadata to database
          
        else:
            print "Creating an instance of the meta class failed, moving to next file"
            self.bad_img += 1
            self.issueString += "\n\nERROR (Metadata): " + zipfile

        return


    def data2img(self, fname, imgname, zipname, sattype, granule, zipfile, unzipdir):
        """
        Opens an image file and converts it to the format given in the config file

        **Parameters**
            *fname* :   image filename (i.e. R1_980705_114117.img OR product.xml)
            *imgname*   :   image name (i.e. R1_980705_114117)
            *zipname*   :  zipname
            *sattype*   :   satelite platform
            *granule*   :   granule name
            *zipfile*   :   zipfile
            *unzipdir*  :   unzip directory
            
        """
        logger = logging.getLogger(__name__)

        #TODO remove from here and pass a metadata instance to this function. 
        sar_meta = Metadata(granule, imgname, unzipdir, zipfile, sattype)   # Retrieve metadata
        
        if sar_meta.status != "ok":       # Meta class unsuccessful
            logger.error("Creating an instance of the meta class failed, moving to next image")
            self.bad_img += 1
            self.issueString += "\n\nERROR (meta class): " + zipfile

        else:
            logger.debug('Metadata ok')
            # Change working directories so that processed image can be stored in imgDir
            os.chdir(self.imgDir)
            
            # Process the image
            sar_img = Image(fname, unzipdir, sar_meta, self.imgType, self.imgFormat, zipname)

            if sar_img.status == "error":
                logger.error("Image could not be opened or manipulated, moving to next image")
                os.remove(sar_img.tifname)
                self.issueString += "\n\nERROR (image processing): " + zipfile
                self.bad_img += 1
            else:
                logger.debug('Image read ok')
                if self.imgType == 'amp':
                    ok = sar_img.projectImg(self.proj, self.projDir, resample='bilinear')
                else:  # no smoothing for quantitative images
                    ok = sar_img.projectImg(self.proj, self.projDir, resample='near')
                #pdb.set_trace()
                if ok != 0: # trap errors here
                    logger.error('ERROR: Issue with projection... will stop projecting this img')
                    self.issueString += "\n\nWARNING (image projection): " + zipfile
                    #sar_img.cleanFiles(['proj'])
                else:
                    logger.debug('Image projected ok')
                    if self.crop is not "":
                        logger.debug('Image crop')
                        sar_img.cropImg([tuple(map(float, self.crop.split(" "))[:2]), \
                            tuple(map(float, self.crop.split(" "))[2:])], 'crop')
                        logger.debug('Image crop done')    
                    try: 
                        sar_img.vrt2RealImg()
                        logger.debug('Image convert vrt to real ok')
                    except:
                        logger.error("Issue converting from vrt to real image")
                        self.issueString += "\n\nWARNING (vrt2real): " + zipfile
                        self.bad_img += 1
                    #TODO  #mask here...
                    # For removing data: sar_img.maskImg(mask, vectdir, 'inside', imgType) #mask the coastline
                    # For retaining data: 
                    #              maskwkt = dbImg.qryMaskZone(zipname, roi, spatialrel, proj, inst)
                    #                ingestutil.wkt2shp('instmask', vectdir, proj, projdir, maskwkt)
                    #                sar_img.maskImg('instmask', vectdir, 'outside', imgType)
                    #sar_img.reduceImg(2,2)                    
                    stats = sar_img.getImgStats()
                    #logger.debug('Image statistics retreived: %s', stats)  #TODO - hook this up, need to mind the data type
                    sar_img.applyStretch(stats, procedure='std', sd=3, sep='sep')
                    logger.debug('Image stretch ok')
                    sar_img.makePyramids()
                    logger.debug('Image pyramid ok')

                sar_img.cleanFiles(levels=['nil','proj','crop']) 
                logger.debug('Intermediate file cleanup done')


    def run(self):
        if self.create_tblmetadata == "1":
            ans = raw_input("Confirm you want to create/overwrite tblMetadata? Y/N")
            if ans.lower == 'y':
                self.dbName.createTblMetadata()
        if self.scanPath == "1":
            self.proc_Dir(self.scanDir, self.scanFor)      # Scan by path pattern
        elif self.scanFile == "1":
            self.proc_File(os.path.abspath(os.path.expanduser(str(sys.argv[-1]))))  #assume this is the last arg (after 'jobid')
        elif self.proc_Query == "1":
            print "\nScan Query not implemented yet.\n"
        else:
            print "\nPlease specify one method to scan the data in the config file.\n"


if __name__ == "__main__":
    
    SigLib().run()
