# -*- coding: utf-8 -*-
"""
**SigLib.py**

This script is thr margin that brings together all the SigLib modules with a config script to query, maniputlate and process remote sensing imagery


**Created on** Mon Oct  7 20:27:19 2013 **@author:** Sougal Bouh Ali
**Modified on** Wed May  23 11:37:40 2018 **@reason:** Sent instance of Metadata to data2img instead of calling Metadata again **@author:** Cameron Fitzpatrick


Common Parameters of this Module:

*zipfile* : a valid zipfile name with full path and extension

*zipname* : zipfile name without path or extension

*fname* : image filename with extention but no path

*imgname* : image filename without extention

*granule* : unique name of an image in string format

"""

import os
import sys
import ConfigParser
import logging
import shutil
import time
from time import localtime, strftime
from glob import glob

from Database import Database
from Metadata import Metadata
from Image import Image
import Util

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
        self.logDir = os.path.abspath(os.path.expanduser(config.get("Directories","logDir")))
        self.archDir = os.path.abspath(os.path.expanduser(config.get("Directories", "archDir")))
        
        self.dbName = config.get("Database", "db")
        self.dbHost = config.get("Database", "host")
        self.create_tblmetadata = config.get("Database", "create_tblmetadata") 
        self.uploadROI = config.get("Database", "uploadROI")
        self.table_to_query = config.get("Database", "table")
        
        self.scanQuery = config.get("Input", "query")
        self.scanPath = config.get("Input", "path")
        self.scanFile = config.get("Input", "file")
        self.scanFor = config.get("Input", "scanFor")
        self.uploadData = config.get("Input", "uploadData")

        self.processData2db = config.get("Process", "data2db")
        self.processData2img = config.get("Process", "data2img")
        self.scientificProcess = config.get("Process", "scientific")
        self.polarimetricProcess = config.get("Process", "polarimetric")
        
        self.proj = config.get('MISC',"proj")
        self.projSRID = config.get('MISC', "projSRID")
        self.crop = config.get('MISC',"crop")
        self.mask = config.get('MISC',"mask")
        self.roi = config.get('MISC',"roi")
        self.roiProjSRID = config.get('MISC',"roiProjSRID")
        self.spatialrel = config.get('MISC',"spatialrel")
        self.imgType = config.get('MISC',"imgTypes")
        self.imgFormat = config.get('MISC',"imgFormat")

        self.issueString = ""
        self.count_img = 0            # Number of images processed
        self.bad_img = 0             # Number of bad images processed
        self.starttime = strftime('%Y%m%d_%H%M%S', localtime())
        shutil.copy(os.path.abspath(os.path.expanduser(sys.argv[1])), os.path.join(self.logDir,self.cfg + "_" +\
            self.starttime +'.cfg')) # make a copy of the cfg file
        self.length_time = 0
        self.loghandler = 0
        self.logger = 0        
    
    def createLog(self,zipfile=None):   
        """
        Creates log file that will be used to report progress and errors
        **Parameters**
            
            *zipfile* 
        """
        
        if zipfile is not None: 
            self.loggerFileName = os.path.basename(zipfile) + "_" + self.cfg \
                + "_" + strftime('%Y%m%d_%H%M%S', localtime())+".log"
        else:
            self.loggerFileName = self.cfg + "_" + self.starttime+".log"

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.loghandler = logging.FileHandler(os.path.join(self.logDir,self.loggerFileName))
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')        
        self.loghandler.setFormatter(formatter)        
        self.logger.addHandler(self.loghandler)
        self.logger.propagate = False
                
        self.logger.info("SigLib Run w/ config: %s", self.cfg)
        self.logger.info("User: %s",os.getenv('USER'))        


    def proc_File(self, zipfile):
        """
        Locates a single satellite image zip file and processes it according 
        to the config file.  Note this cannot be nested in proc_dir since the 
        logging structure and other elements must parallelizable
        
        **Parameters**
            
            *zipfile* 
        """
        
        self.logger = self.createLog(zipfile)
        self.logger = logging.getLogger(__name__)
        self.count_img += 1      
                           
        self.logger.info('Started processing image %s', zipfile) 
            
        try:
            start_time = time.time()
            self.retrieve(zipfile)
            self.logger.debug('image retrieved')
            # Do clean-up
                            
        except Exception, e: #Normally Exception, e
            self.logger.error('Image failed %s, due to: %s', zipfile, e, exc_info=True)
            self.logger.error("Image processing exception, moving to next image")
            self.issueString += "\n\nERROR (exception): " + zipfile
            self.bad_img += 1
            
            
        end_time = time.time()
        self.logger.info("Image Processing Time: " + str(int((end_time-start_time)/60)) + " Minutes " + str(int((end_time-start_time)%60)) + " Seconds")
        os.chdir(self.tmpDir)
        os.system("rm -r " +os.path.splitext(os.path.basename(zipfile))[0])
        self.logger.debug('cleaned zip dir')

        good_img = self.count_img - self.bad_img
        self.logger.info("%i images where successfully processed out of %i", good_img, self.count_img)

        if self.bad_img > 0:
            # Write the issue file
            self.logger.error(self.issueString)
          
    def proc_Dir(self, path, pattern):
        """
        Locates satelite image raw data files (zipfiles) using a
        *pattern* in *path* search method, and then calls createImg()
        to process the data into image.

        **Parameters**
            
            *path*    : directory tree to scan

            *pattern* : file pattern to discover
        """
            
        self.logger = self.createLog()
        self.logger = logging.getLogger(__name__)
                
        ziproots = []           # List of the zip files (dirpath + *.zip: '/xx/yy/zz/*.zip')

        # Returns a list 'ziproots' of the zip files with the specified path and pattern
        for dirpath, dirnames, filenames in os.walk(path):
            ziproots.extend(glob(os.path.join(dirpath,pattern)))

        self.logger.info('Found %i files in %s matching pattern %s', len(ziproots), path, pattern)
        ziproots.sort() # Nice to have this in some kind of order
        # Process every zipfile in ziproots 1 by 1
        
        for zipfile in ziproots:
            formatter = logging.Formatter('')        
            self.loghandler.setFormatter(formatter)
            self.logger.info('')
            self.logger.info('')        
            self.logger.info('')
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')         
            self.loghandler.setFormatter(formatter)
            
            self.count_img += 1
                
            try:
                start_time = time.time()
                self.retrieve(zipfile)
                self.logger.debug('image retrieved') #TODO move to retrieve and return meaningful info.
            except Exception: #Normally Exception
                self.logger.error('Image failed %s', zipfile)
                self.logger.error("Image processing exception, moving to next image")
                self.issueString += "\n\nERROR (exception): " + zipfile
                self.bad_img += 1
                        

            # Do clean-up
            end_time = time.time()
            self.logger.info("Image Processing Time: " + str(int((end_time-start_time)/60)) + " Minutes " + str(int((end_time-start_time)%60)) + " Seconds")
            os.chdir(self.tmpDir)
            os.system("rm -r " +os.path.splitext(os.path.basename(zipfile))[0])
            self.logger.debug('cleaned zip dir')

        good_img = self.count_img - self.bad_img
        self.logger.info("%i images where successfully processed out of %i", good_img, self.count_img)

        if self.bad_img > 0:
            # Write the issue file
            self.logger.error(self.issueString)
        
        self.logger.handlers = []
        logging.shutdown()
        del sys.modules['Image']
        del sys.modules['Metadata']
        del sys.modules['Database']
        del sys.modules['Util']

    def retrieve(self, zipfile):
        """
        Given a zip file name this function will: find out what satellite it is, unzip it, get instance of metadata, then 
        dependant on the config, save metadata in a file and/or one of the following: Process to image or process to database.
        
        **Parameters**
            
            *zipfile* 
        """
        
        # Verify if zipfile has its own subdirectory before unzipping
        unzipdir, granule, nested = Util.getZipRoot(os.path.join(self.scanDir,zipfile), self.tmpDir)            
        self.logger.debug("Zipfile %s will unzip to %s. Granule is %s and Nested is %s", zipfile, unzipdir, granule, nested)        
        # Unzip the zip file into the unzip directory
        Util.unZip(zipfile, unzipdir)
        self.logger.debug("Unzip ok")

        zipname, ext = os.path.splitext(os.path.basename(zipfile))
        
        if unzipdir == self.tmpDir:      # If files have been unzipped in their own subdirectory
            unzipdir = os.path.join(self.tmpDir, granule)    # Then correct the name of unzipdir
            if nested == 1:   # If zipfile has nested directories
                unzipdir = os.path.join(unzipdir, granule)    # Then correct the name of unzipdir

        # Parse zipfile
        fname, imgname, sattype = Util.getFilename(granule, unzipdir, self.loghandler)

        formatter = logging.Formatter('')        
        self.loghandler.setFormatter(formatter)
        self.logger.info('\n' + 'Zipname: %s', zipname)
        self.logger.info('Imgname: %s', imgname)
        self.logger.info("granule:  %s", granule)
        self.logger.info("sat type:  %s", sattype + '\n')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')         
        self.loghandler.setFormatter(formatter)
        
            
        if fname == "error":
            self.logger.error("File not valid or available, moving to next file")
            self.issueString += "\n\nERROR (retrieve): " + zipfile    # Take note
            self.bad_img += 1

        else:#begin processing data ...
            sar_meta = Metadata(granule, imgname, unzipdir, zipfile, sattype, self.loghandler)   # Retrieve metadata          
        
            if sar_meta.status != "ok":       # Meta class unsuccessful
                self.logger.error("Creating an instance of the meta class failed, moving to next image")
                self.bad_img += 1
                self.issueString += "\n\nERROR (meta class): " + zipfile
            
            else:
                formatter = logging.Formatter('')        
                self.loghandler.setFormatter(formatter)
                self.logger.info('\n' + 'Dimgname: %s', sar_meta.dimgname + '\n')
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')         
                self.loghandler.setFormatter(formatter)

            sar_meta.saveMetaFile(self.imgDir)
                        
            if self.processData2db == "1":
                db = Database(self.table_to_query, self.dbName, loghandler=self.loghandler, host=self.dbHost)  # Connect to the database
                self.data2db(sar_meta, db, zipfile)
                db.removeHandler()

            if self.processData2img == "1":
                self.logger.debug("processing data to image")
                self.data2img(fname, imgname, zipname, sattype, granule, zipfile, sar_meta, unzipdir)
                
            if self.scientificProcess == "1":
                db = Database(self.table_to_query, self.dbName, loghandler=self.loghandler, host=self.dbHost)
                self.scientific(db, fname, imgname, zipname, sattype, granule, zipfile, sar_meta, unzipdir)
                db.removeHandler()
                
            if self.polarimetricProcess == '1':
                if 'Q' not in sar_meta.beam:
                    self.logger.debug("This is not a quad-pol scene, skipping")
                    return Exception
                db = Database(self.table_to_query, self.dbName, loghandler=self.loghandler, host=self.dbHost)
                self.polarimetric(db, fname, imgname, zipname, sattype, granule, zipfile, sar_meta, unzipdir)
            
    def data2db(self, meta, db, zipfile):
        """
        Adds the image file metadata to tblmetadata table in the specified database.
        Will create/overwrite the table tblmetadata if prompted (be carefull)

        **Parameters**
            
            *meta* :   A metadata instance from Metadata.py

            *db*   :   database connection            
        """

        if meta.status == "ok":
            meta_dict = meta.createMetaDict()  # Create dictionary of all the metadata fields
            db.meta2db(meta_dict)       # Upload metadata to database
          
        else:
            self.logger.error("Creating an instance of the meta class failed, moving to next file")
            self.bad_img += 1
            self.issueString += "\n\nERROR (Metadata): " + zipfile
            

    def data2img(self, fname, imgname, zipname, sattype, granule, zipfile, sar_meta, unzipdir):
        """
        Opens an image file and converts it to the format given in the config file

        **Parameters**
            
            *fname*    

            *imgname* 

            *zipname* 

            *sattype*  : satelite platform

            *granule*  

            *zipfile* 
            
            *sar_meta* : instance of the Metadata class

            *unzipdir* : directory zipfiles were unzipped into     
        """
        
        # Change working directories so that processed image can be stored in imgDir
        os.chdir(self.imgDir)
        
        newTmp = os.path.join(self.tmpDir,zipname)
        if os.path.isdir(newTmp):
            pass
        else:
            os.makedirs(newTmp)

        # Process the image
        sar_img = Image(fname, unzipdir, sar_meta, self.imgType, self.imgFormat, zipname, self.imgDir, newTmp, self.loghandler)
        
        if sar_img.status == "error":
            self.logger.error("Image could not be opened or manipulated, moving to next image")
            sar_img.cleanFiles(levels=['nil']) 
            self.issueString += "\n\nError (image processing): " + zipfile
            self.bad_img += 1
            
        else:
            """
            self.logger.debug('Image read ok')
            if self.imgType == 'amp' and 'Q' not in sar_meta.beam and sar_meta.productType == 'SLC':    #single or dual band SLC
                sar_img.snapTC(self.proj, self.projDir)
                sar_img.snapDataTypeConv()
            elif self.imgType == 'amp' and 'Q' not in sar_meta.beam:           #non SLC product
                sar_img.snapTC(self.proj, self.projDir, existingInput=False)
                sar_img.snapDataTypeConv()
            elif self.imgType == 'amp' and 'Q' in sar_meta.beam:   #quad-pol SLC product
                sar_img.snapTC(self.proj, self.projDir)
                sar_img.snapDataTypeConv()
            else:             #any product, not amplitude
                sar_img.snapTC(self.proj, self.projDir, smooth = False, outFormat = 'GeoTiff-BigTiff')    
            
            os.chdir(self.imgDir)
            """
            try:    
                if self.imgType == 'amp':
                    ok = sar_img.projectImg(self.proj, self.projDir, resample='bilinear')
                else:  # no smoothing for quantitative images
                    ok = sar_img.projectImg(self.proj, self.projDir, resample='near')
            except:
                self.logger.error('ERROR: Issue with projection... will stop projecting this img')
                self.issueString += "\n\nWARNING (image projection): " + zipfile
                return Exception
            
            if ok != 0: # trap errors here
                self.logger.error('ERROR: Issue with projection... will stop projecting this img')
                self.issueString += "\n\nWARNING (image projection): " + zipfile
                       
            self.logger.debug('Image projected ok')             
                
            try: 
                sar_img.vrt2RealImg()
                self.logger.debug('Image convert vrt to real ok')
            except:
                self.logger.error("Issue converting from vrt to real image")
                self.issueString += "\n\nWARNING (vrt2real): " + zipfile
                self.bad_img += 1
                
            #if self.imgType == 'amp':                                              
            stats = sar_img.getImgStats()
            sar_img.applyStretch(stats, procedure='std', sd=3, sep=True)
            self.logger.debug('Image stretch ok')
             
            sar_img.compress()
            sar_img.makePyramids()
            self.logger.debug('Image pyramid ok')
            
            sar_img.cleanFiles(levels=['nil','proj']) 
            self.logger.debug('Intermediate file cleanup done')
            sar_img.removeHandler()
            sar_meta.removeHandler()
    
                 
    def scientific(self, db, fname, imgname, zipname, sattype, granule, zipfile, sar_meta, unzipdir):            #Needs final testing!!!!!!!
        """
        Process images 'Scientifically', based on an ROI in the database, and per zipfile:
            -Qry to find what polygons in the ROI overlap this image
            -Process one polygon at a time (Project, crop, and mask), saving each as its own img file OR uploading img data to database
            
        **Parameters**
        
            *db*       : instance of the Database class
            
            *fname*    
            
            *zipname* 
            
            *sattype*  : satellite platform
            
            *granule*  
            
            *zipfile*  
            
            *sar_meta* : instance of the Metadata class
            
            *unzipdir* : directory zipfile was unzipped into                            
        """
        
        os.chdir(self.imgDir)
        newTmp = os.path.join(self.tmpDir,zipname)
        
        # Process the image
        sar_img = Image(fname, unzipdir, sar_meta, self.imgType, self.imgFormat, zipname, self.imgDir, newTmp, self.loghandler)
        
        if sar_img.status == "error":
            self.logger.error("Image could not be opened or manipulated, moving to next image")
            os.remove(sar_img.tifname)
            self.issueString += "\n\nError (image processing): " + zipfile
            self.bad_img += 1
            return
        else:
            self.logger.debug('Image read ok')        
        
        instances = db.qryGetInstances(granule, self.roi, self.table_to_query)   
        if instances == -1:
            self.logger.error(fname + ' has no associated polygons, exiting')
            return
            
        sar_img.tmpFiles = sar_img.FileNames
        for i, inst in enumerate(instances):
            sar_img.FileNames = sar_img.tmpFiles   #reset list of filenames within Image.py each loop
                      
            sar_img.cleanFileNames()
            #Crop!
            self.logger.debug('Processing '+ str(inst) + ' : ' + str(i+1) + ' of ' + str(len(instances)) + ' subsets')
              
            #PROJECT
            if self.imgType == 'amp':
                ok = sar_img.projectImg(self.proj, self.projDir, resample='bilinear')
            else:  # no smoothing for quantitative images
                ok = sar_img.projectImg(self.proj, self.projDir, resample='near')
            if ok != 0: # trap errors here 
                self.logger.error('ERROR: Issue with projection... will stop processing this img')
                
                sar_img.cleanFiles(levels=['nil','proj']) 
                return
              
            crop = db.qryCropZone(granule, self.roi, self.spatialrel, inst, self.table_to_query, srid=self.projSRID) 
            #ok = sar_img.snapSubset(inst, ullr=crop) 
            ok = sar_img.cropImg(crop, inst)   #error due to sending last inst?
            
            if ok != 0: # trap errors here 
                self.logger.error('ERROR: Issue with cropping... will stop processing this subset')
                sar_img.cleanFiles(['nil', 'proj', 'crop'])
                continue
            sar_img.vrt2RealImg(inst)
            
                   ### MASK
            if self.mask != '':     #If providing a mask, use that one
                #os.chdir(self.imgDir)
                sar_img.maskImg(self.mask, self.vectDir, 'outside') #mask the coastline
                sep = 'tog'
                
            else:            #If no mask provided, make one based on ROI and inst
                #os.chdir(self.imgDir)
                maskwkt = db.qryMaskZone(granule, self.roi, self.roiProjSRID, inst, self.table_to_query)
                Util.wkt2shp('instmask'+str(inst), self.tmpDir, self.proj, self.projDir, maskwkt)
                sar_img.maskImg('instmask'+str(inst), self.tmpDir, 'outside')
                sep = 'sep'
                    
            if self.uploadData == '1':  
                for i, bandName in enumerate(sar_img.bandNames):
                    band = i+1
                    imgData = sar_img.getBandData(band)                    
                    db.imgData2db(imgData, bandName, inst, sar_img.meta.dimgname, zipname)
            else:
                stats = sar_img.getImgStats()
                sar_img.applyStretch(stats, procedure='std', sd=3, sep=sep)
                
            sar_img.cleanFiles(levels=['proj', 'crop'])
        sar_img.cleanFiles(levels=['nil']) 
        self.logger.debug('Intermediate file cleanup done')
        sar_img.removeHandler()
  
      
    def polarimetric(self, db, fname, imgname, zipname, sattype, granule, zipfile, sar_meta, unzipdir):
        """
        This function will take full FQ images and perform desired polarimetric matrix generation(s), speckle filtering, and decomposition(s). 
        Combinations can be set in the config file. BE CAREFULL, if no combination is specified, then all possible combinations will be performed, 
        so be prepared for a long run-time and large amounts of data!
        
        **Parameters**
        
            *db* : an instance of the Database class
            
            *fname* 
            
            *imgname*  
            
            *zipname* 
            
            *sattype* : Satellite platform
            
            *granule* 
            
            *zipfile* 
            
            *sar_meta* : an instance of the Metadata class
            
            *unzipdir* : location zipfiles were unzipped into
        """
        
        os.chdir(self.imgDir)
        newTmp = os.path.join(self.tmpDir,zipname)
        
        beaconTable = 'beacon_tracks'              
        beacons = db.beaconIntersections(beaconTable, granule)  #Get beacon instances that overlap this image
        
        if len(beacons) == 0:
            self.logger.debug(granule + ' has no beacons associated with it!')
            return
        
        self.logger.debug("Found " + str(len(beacons)) + " beacons in this image!")
        
        # Process the image
        sar_img = Image(fname, unzipdir, sar_meta, self.imgType, self.imgFormat, zipname, self.imgDir, newTmp, self.loghandler, pol=True)
        
        seenBeacons = []
        #filenames = []
        sar_img.tmpFiles = sar_img.FileNames
        for i in range(len(beacons)):
            sar_img.FileNames = sar_img.tmpFiles
            beaconid = beacons[i][0]
            latitude = float(beacons[i][1])
            longitude = float(beacons[i][2])
            
            if beaconid in seenBeacons:
                self.logger.debug("Seen best beacon for " + str(beaconid) + ", moving on")
                continue
            
            #sar_img.cleanFileNames()           
            self.logger.debug("Found beacon " + str(beaconid) + " in image " + granule + ". Processing...")
            seenBeacons.append(beaconid)
            
            finalsDir = os.path.join(newTmp,'finals')  #directory in tmp for images to be masked  
            if os.path.isdir(finalsDir):
                pass
            else:
                os.makedirs(finalsDir)
 
            output = sar_img.snapSubset(beaconid, latitude, longitude, finalsDir) #subset
            
            if output == -1:
                return
            
            sar_img.makeAmp(newFile = False, save = False)
                
            matrices = ['C3', 'T3']           
            
            tmpArr = sar_img.FileNames
            for matrix in matrices:
                sar_img.FileNames = tmpArr
                matrixFN = sar_img.matrix_generation(matrix)  #Generate each type of matrix, one at a time
                if matrix == 'C3':
                    sar_img.polarFilter(save=False)  #only want to save the filename of the latter, so T3 isn't generated upon C3
                else:
                    sar_img.polarFilter(save=True)
                
                rm = os.path.join(finalsDir, matrixFN+'.data')
                
                for dirpath, dirs, files in os.walk(rm):
                    for file in files:
                        try:
                            os.chdir(dirpath)
                            os.remove(file)
                        except:
                            pass
                os.remove(matrixFN+'.dim')               
            
            self.logger.debug("All matrices generated!")
                      
            decompositions = ['Sinclair Decomposition', 'Pauli Decomposition', 'Freeman-Durden Decomposition', 'Yamaguchi Decomposition', 'van Zyl Decomposition', 'H-A-Alpha Quad Pol Decomposition', 'Cloude Decomposition']
                        
            for decomposition in decompositions:
                try:
                    if decomposition == "H-A-Alpha Quad Pol Decomposition":
                        i = 4
                        while i <= 4:
                            sar_img.decomposition_generation(decomposition, outputType=i)
                            i+=1
                        
                    elif decomposition == "Touzi Decomposition":
                        i = 5
                        while i <= 8:
                            sar_img.decomposition_generation(decomposition, outputType=i)
                            i+=1
                    
                    else:
                        sar_img.decomposition_generation(decomposition)
                    
                except:
                    self.logger.error("Error with " + decomposition + ", moving on")
                        
            self.logger.debug('All matrix-decomposition combinations generated!')
          
        #for filename in filenames:
            for dirpath, dirnames, filenames in os.walk(finalsDir):
                for filename in filenames:
                    if os.path.splitext(filename)[1] == '.img':                 
                        os.chdir(dirpath)
                        name = os.path.splitext(filename)[0]
                        
                        masks = db.polarimetricDonuts(granule, beaconid)
                        counter = 0
                        type = ''
                        for mask in masks:
                            if counter == 0: 
                                type = 'ii'
                            else: 
                                type = 'donut'
                                
                            shutil.copy(name+'.img', name+type+'.img')  #make local copies for this maskign operation 
                            shutil.copy(name+'.hdr', name+type+'.hdr')
                            
                            name = name+type
                              
                            convMask = sar_img.slantRangeMask(mask, os.path.splitext(dirpath)[0], name)     #add back masking
                            
                        
                            shpname = 'instmask_'+zipname+'_'+str(beaconid)+ '_' + type
                            Util.wkt2shp(shpname, finalsDir, self.proj, self.projDir, convMask)
                            
                            sar_img.maskImg(shpname, finalsDir, side = 'outside', inname = name+'.img')                            
                            
                            imgData = sar_img.getBandData(1, name+'.img')                    
                            db.imgData2db(imgData, name, str(beaconid), sar_img.meta.dimgname, granule)  
                            counter+=1
                            
                            os.remove(name+'.img')  #remove our tmp masking files
                            os.remove(name+'.hdr')
         
    def run(self):      
        if self.create_tblmetadata == "1":
            ans = raw_input("Confirm you want to create/overwrite tblMetadata? Y/N")
            if ans.lower == 'y':
                db = Database(self.table_to_query, self.dbName, self.loghandler, host=self.dbHost)
                db.createTblMetadata()
        if self.uploadROI == "1":
            db = Database(self.table_to_query, self.dbName, self.loghandler, host=self.dbHost)
            db.updateROI(self.roi, self.roiProjSRID, self.vectDir)  #Refer to this function in documentation before running to confirm convension
        if self.scanPath == "1":
            self.proc_Dir(self.scanDir, self.scanFor)      # Scan by path pattern
        elif self.scanFile == "1":
            self.proc_File(os.path.abspath(os.path.expanduser(str(sys.argv[-1]))))  #assume this is the last arg (after 'jobid')
        else:
            print "\nPlease specify one method to scan the data in the config file.\n"

if __name__ == "__main__":   
    SigLib().run()
    