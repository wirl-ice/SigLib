''' I made this script to faciliate database operations and query in siglib since
this functionality has been cut off in the post CIS era.  

To Do - update the cfg file to handle this 'mode'
        - look at CIS extraction scripts - how do files get named? 
        - incorporate the metadata search stuff. 
        -Current error: trying to search database with table name of roi, but non existant?

'''
############################################# PARAMETERS
datadir =  "/tank/SCRATCH/cfitzpatrick/Siglib/test_zips"
homedir = "/tank/SCRATCH/cfitzpatrick/Siglib/"
#datadir =  "/tank/SCRATCH/cfitzpatrick/Siglib/"
projdir = "/tank/ice/data/proj"
shapedir = "/tank/SCRATCH/cfitzpatrick/Siglib/"
tmpDir = "/tank/SCRATCH/cfitzpatrick/Siglib/tmp"
zipfile = "RS2_OK34723_PK340098_DK302201_FQ17W_20121029_214643_HH_VV_HV_VH_SLC.zip"
vectdir = "/tank/SCRATCH/cfitzpatrick/Siglib/"
imgdir = "/tank/SCRATCH/cfitzpatrick/Siglib/output_linux"
gdal_merge = "/usr/bin/"


proj = 'lcc'
imgType = 'amp'
roi =  'IceIslands_lcc' 
roiProj = 'lcc'  # roi can have a different proj.
spatialrel = 'ST_Contains'
mask = ''
imgFormat = 'gtiff'
selectFrom = 'tblmetadata_r1_r2'
combine = False

############################################# HOUSEKEEPING
import os
import sys
import shutil

sys.path.append(os.path.join(homedir))
from Metadata import Metadata
import Database
import Util
from Image import Image


############################################ Select files and copy them
db = Database.Database('wirlsar')
#db.createTblMetadata()

unzipdir, granule, nested = Util.getZipRoot(os.path.join(datadir,zipfile), tmpDir)

Util.unZip(zipfile, unzipdir)
zipname, ext = os.path.splitext(os.path.basename(zipfile))      
if unzipdir == tmpDir:      # If files have been unzipped in their own subdirectory
    unzipdir = os.path.join(tmpDir, granule)    # Then correct the name of unzipdir
    if nested == 1:   # If zipfile has nested directories
        unzipdir = os.path.join(unzipdir, granule)    # Then correct the name of unzipdir
        
fname, imgname, sattype = Util.getFilename(granule, unzipdir)
sar_meta = Metadata(granule, "product.xml", unzipdir, zipfile, "RS2")   # Retrieve metadata

#meta_dict = sar_meta.createMetaDict()  # Create dictionary of all the metadata fields
#db.meta2db(meta_dict)
'''

#db.updateFromArchive('/tank/ice/data/vector/CIS_Archive')   # this should be done
db.updateROI(roi, roiProj, homedir, True) #loads (overwrites) ROI file into dbase
#copylist, instimg = db.qrySelectFromAvailable(roi, selectFrom, spatialrel, roiProj) #spatial query
#if selectFrom == 'tblArchive':
#db.instimg2db(roi, spatialrel, instimg, mode='create')  #this uploads the instimg to a relational table in the database

#Here is a sample query that might be used to pull out data and then save as csv
"""
q = "SELECT "+ roi+".inst, "+roi+".name, "+db.nameTable(roi,spatialrel)+".granule, "+\
    "tblArchiveogr2ogr -t_srs /tank/ice/data/proj/lcc.wkt Sample_ScientificROI_lcc.shp Sample_ScientificROI.shp Sample_ScientificROI.\"catalog id\", tblArchive.\"file size\", tblArchive.\"valid time\" "+\
    "FROM "+db.nameTable(roi,spatialrel)+" "+\
    "INNER JOIN "+roi+" on "+db.nameTable(roi,spatialrel)+".inst = "+roi+".inst " +\
    "INNER JOIN tblArchive on "+db.nameTable(roi,spatialrel)+".granule = tblArchive.\"file name\" "+\
    "ORDER BY "+roi+".inst;"
"""
#db.exportToCSV(db.qryFromText(q,output=True),'ResoluteSummer2014.csv')
#db.copylistExport(copylist, 'ResoluteCatIDContains')

#db.copyfiles(copylist, datadir) #copies files to working directory
#copylistx =  # make reltbl of inst-images
'''
os.chdir(imgdir)
        
        # Process the image
sar_img = Image(fname, unzipdir, sar_meta, imgType, imgFormat, zipname, imgdir)
          
         
### SUBSET
# Query dbase to see what polygons overlap with this img
instances = db.qryGetInstances(granule, roi, proj, selectFrom)   
if instances == -1:
    print 'This image has no associated polygons, exiting'
    sys.exit()
         
for i, inst in enumerate(instances):
    sar_img.cleanFileNames()
    
    #PROJECT
    if imgType == 'amp':
        ok = sar_img.projectImg(proj, projdir, resample='bilinear')
    else:  # no smoothing for quantitative images
        ok = sar_img.projectImg(proj, projdir, resample='near')
    if ok != 0: # trap errors here 
        print 'ERROR: Issue with projection... will stop processing this img'
            #sar_img.cleanFiles(['proj'])    
    
    print 'Processing '+ str(inst) + ' : ' + str(i+1) + ' of ' + str(len(instances)) + ' subsets'
   
    crop = db.qryCropZone(granule, roi, spatialrel, proj, inst, selectFrom)
  
    ok = sar_img.cropImg(crop, str(inst))   #error due to sending last inst?
            
    if ok != 0: # trap errors here 
        print 'ERROR: Issue with cropping... will stop processing this subset'
        sar_img.cleanFiles(['crop'])
        sys.exit()
    sar_img.vrt2RealImg(str(inst))
            
            ### MASK
    if imgType == 'amp': #this is a qualitative image...
        if mask != '':     #If providing a mask, use that one
            os.chdir(imgdir)
            sar_img.maskImg(mask, vectdir, 'outside', imgType) #mask the coastline
            stats = sar_img.getImgStats()
            sar_img.applyStretch(stats, procedure='std', sd=3, sep='tog')
            
        else:            #If no mask provided, make one based on ROI and inst
            os.chdir(imgdir)
            maskwkt = db.qryMaskZone(granule, roi, proj, str(inst), selectFrom)
            Util.wkt2shp('instmask'+str(inst), tmpDir, proj, projdir, maskwkt)
            sar_img.maskImg('instmask'+str(inst), tmpDir, 'outside', imgType)
            stats = sar_img.getImgStats()
            sar_img.applyStretch(stats, procedure='std', sd=3, sep='sep')
    
    else: # this is a quantitative image...   
        maskwkt = db.qryMaskZone(granule, roi, spatialrel, proj, inst)
        Util.wkt2shp('instmask'+inst, vectdir, proj, projdir, maskwkt)
        sar_img.maskImg('instmask'+inst, vectdir, 'outside', imgType)
    
                #bands...
        for i, bandName in enumerate(sar_img.bandNames):
            band = i+1
            imgData, xSpacing, ySpacing = sar_img.getBandData(band)                    
            db.imgData2db(imgData, xSpacing, ySpacing, bandName, inst, sar_img.meta.dimgname, zipname)
    
            #Cleanup subset
    sar_img.cleanFiles(levels=['proj', 'crop'])
           
sar_img.cleanFileNames()

if combine == True:
    sar_img.combineTif(imgdir,zipname,gdal_merge)

for file in os.listdir(tmpDir):         #Clean tmpDir
    path = os.path.join(tmpDir, file)
    try:
        if os.path.isfile(path):
            os.unlink(path)
        elif os.path.isdir(path):
            shutil.rmtree(path)
    except Exception as e:
        print e
        
print "Temp Directory Clean!"