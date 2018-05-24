''' I made this script to faciliate database operations and query in siglib since
this functionality has been cut off in the post CIS era.  

To Do - update the cfg file to handle this 'mode'
        - look at CIS extraction scripts - how do files get named? 
        - incorporate the metadata search stuff. 
        -Current error: trying to search database with table name of roi, but non existant?

'''
############################################# PARAMETERS
#homedir = "/tank/HOME/cfitzpatrick/SigLib"
datadir =  "/tank/SCRATCH/cfitzpatrick/test_zips"
homedir = "/tank/SCRATCH/cfitzpatrick/"
#datadir =  "/tank/SCRATCH/cfitzpatrick/"
projdir = "/tank/ice/data/proj"
shapedir = "tank/SCRATCH/cfitzpatrick/"
tmpDir = "/tank/SCRATCH/cfitzpatrick/tmp"
zipfile = "RS2_OK16346_PK175171_DK165494_SCWA_20101205_123058_HH_HV_SGF.zip"
proj = 'lcc'
roi =  'Sample_DiscoveryROI' 
roiProj = 'lcc'  # roi can have a different proj.
spatialrel = 'ST_Contains'

############################################# HOUSEKEEPING
import os
import sys


sys.path.append(os.path.join(homedir))
from Metadata import Metadata
import Database
import Util


os.chdir(os.path.join(datadir))
############################################ Select files and copy them
db = Database.Database('cameron')
db.createTblMetadata()

unzipdir, granule, nested = Util.getZipRoot(os.path.join(datadir,zipfile), tmpDir)
Util.unZip(zipfile, unzipdir)
zipname, ext = os.path.splitext(os.path.basename(zipfile))      
if unzipdir == tmpDir:      # If files have been unzipped in their own subdirectory
    unzipdir = os.path.join(tmpDir, granule)    # Then correct the name of unzipdir
    if nested == 1:   # If zipfile has nested directories
       unzipdir = os.path.join(unzipdir, granule)    # Then correct the name of unzipdir
fname, imgname, sattype = Util.getFilename(granule, unzipdir)
sar_meta = Metadata(granule, "product.xml", unzipdir, zipfile, "RS2")   # Retrieve metadata
meta_dict = sar_meta.createMetaDict()  # Create dictionary of all the metadata fields
db.meta2db(meta_dict)

db.updateFromArchive('/tank/ice/data/vector/CIS_Archive')   # this should be done
db.updateROI(roi, datadir, roiProj) #loads (overwrites) ROI file into dbase
copylist, instimg = db.qrySelectFromArchive(roi, spatialrel, roiProj) #spatial query
db.instimg2db(roi, spatialrel, instimg, mode='create')  #this uploads the instimg to a relational table in the database

#Here is a sample query that might be used to pull out data and then save as csv
'''
q = "SELECT "+ roi+".inst, "+roi+".name, "+db.nameTable(roi,spatialrel)+".granule, "+\
    "tblArchive.\"catalog id\", tblArchive.\"file size\", tblArchive.\"valid time\" "+\
    "FROM "+db.nameTable(roi,spatialrel)+" "+\
    "INNER JOIN "+roi+" on "+db.nameTable(roi,spatialrel)+".inst = "+roi+".inst " +\
    "INNER JOIN tblArchive on "+db.nameTable(roi,spatialrel)+".granule = tblArchive.\"file name\" "+\
    "ORDER BY "+roi+".inst;"

db.exportToCSV(db.qryFromText(q,output=True),'ResoluteSummer2014.csv')
'''
#db.copylistExport(copylist, 'ResoluteCatIDContains')

#db.copyfiles(copylist, datadir) #copies files to working directory
#copylistx =  # make reltbl of inst-images
