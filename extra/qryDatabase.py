''' I made this script to faciliate database operations and query in siglib since
this functionality has been cut off in the post CIS era.  

To Do - update the cfg file to handle this 'mode'
        - remove the 'old school/ogr' upload once you have udated pollux
        - look at CIS extraction scripts - how do files get named? 
        - incorporate the metadata search stuff. 

'''
############################################# PARAMETERS
homedir = "/tank/HOME/dmueller/Dropbox/Research/siglib"
datadir =  "/tank/HOME/dmueller/Dropbox/Research/Andrew/EL/AndrewRSatSearch"
wrkdir = datadir
projdir = "/tank/ice/data/proj"

proj = 'lcc'
roi =  'roi_milne' 
roiProj = 'lcc'  # roi can have a different proj.
spatialrel = 'ST_Contains'

############################################# HOUSEKEEPING
import os
import sys

sys.path.append(os.path.join(homedir))

import Database

os.chdir(os.path.join(datadir))
############################################ Select files and copy them
db = Database.Database('ci2d3', host='antares')
#db.updateFromArchive('/tank/ice/data/vector/CISArchive', ?,?)   # this should be done
db.updateROI(roi, datadir, roiProj, 'create_roi_milne') #loads (overwrites) ROI file into dbase
copylist, instimg = db.qrySelectFromArchive(roi, spatialrel, proj) #spatial query
db.instimg2db(roi, spatialrel, instimg, mode='create')  #this uploads the instimg to a relational table in the database

q = """
SELECT roi_milne.inst, trelroi_milneimg_con.granule, tblArchive."obj name", 
tblArchive."file size", tblArchive."valid time", tblArchive."catalog id"
FROM trelroi_milneimg_con 
INNER JOIN roi_milne on trelroi_milneimg_con.inst = roi_milne.inst
INNER JOIN tblArchive on trelroi_milneimg_con.granule = tblArchive."file name"
ORDER BY roi_milne.inst
"""

db.exportToCSV(db.qryFromText(q,output=True),'MilneContains.csv')
db.copylistExport(copylist, 'AOI_CatIDIntersects')

#db.copyfiles(copylist, datadir) #copies files to working directory
