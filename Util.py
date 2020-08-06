# -*- coding: utf-8 -*-
"""
**util.py**

This module contains miscellaneous code that helps siglib work with directories, 
zip files, clean up intermediate files and so on. 


**Created on** Tue Feb 12 20:04:11 2013 **@author:** Cindy Lopes
**Modified on** Sat Nov 23 14:49:18 2013 **@reason:** Added writeIssueFile and compareIssueFiles **@author:** Sougal Bouh Ali
**Modified on** Sat Nov 30 15:37:22 2013 **@reason:** Redesigned getFilname, getZipRoot and unZip **@author:** Sougal Bouh Ali
**Modified on** Wed May 23 14:41:40 2018 **@reason:** Added logging functionality **@author:** Cameron Fitzpatrick
"""

import os
import zipfile
import subprocess
import math
import logging
import shutil

from osgeo import gdal
from osgeo import osr
from osgeo import ogr
import shlex
import numpy      
    
def getFilename(zipname, unzipdir, loghandler=None):
    """
    Given the name of a zipfile, return the name of the image,
    the file name, and the corresponding sensor/platform (satellite).

    **Parameters**
        
        *zipname*  : The basename of the zip file you are working with

        *unzipdir* : Where the zipfile will unzip to (find out with getZipRoot)

    **Returns**
        
        *fname*    :  The file name that corresponds to the image

        *imgname*  : The name of the image (the basename, sans extension)

        *sattype*  : The type of satellite/image format this file represents
    """
    
    if loghandler != None:
        loghandler = loghandler             #Logging setup if loghandler sent, otherwise, set up a console only logging system
        logger = logging.getLogger(__name__)
        logger.addHandler(loghandler)
        logger.propagate = False    
        logger.setLevel(logging.DEBUG)
    else:
        logger = logging.getLogger(__name__)                        
        logger.setLevel(logging.DEBUG)
        logger.addHandler(logging.StreamHandler())
    
    dirlist = os.listdir(unzipdir)      # List of all the files in zip_file to be used as the loop iterative element
    countdown = len(dirlist)            # Number of files in zip_file to be used as a counter inside the loop

    for file in dirlist:
        imgname, imgext = os.path.splitext(file)

        # Here is a RSAT-2
        if imgext == ".xml":
            fname = "product.xml"
            sattype = "RS2"
            imgname, imgext = os.path.splitext(fname)

            break

        # Here is a RSAT-1 CDPF
        elif imgext == ".sarl" or imgext == ".sart" or imgext == ".nvol" or imgext == ".vol":
            fname = imgname+".img"
            sattype = "CDPF"

            # Look for leader file, then deal with *.trl and *.img
            if os.path.isfile(os.path.join(unzipdir,imgname+".sarl")):
                os.rename(os.path.join(unzipdir,imgname+".sarl"), os.path.join(unzipdir,imgname+".led"))

            else:
                logger.error("No file named *.sarl found")
                return "error", "error", "error"

            if os.path.isfile(os.path.join(unzipdir,imgname+".sart")):
                os.rename(os.path.join(unzipdir,imgname+".sart"), os.path.join(unzipdir,imgname+".trl"))

            else:
                logger.error("could not find a trailer file")
                return "error", "error", "error"

            if os.path.isfile(os.path.join(unzipdir,imgname+".sard")):
                os.rename(os.path.join(unzipdir,imgname+".sard"), os.path.join(unzipdir,imgname+".img"))

            # Special case for im_radar*.zip files that have *01f.sard name
            elif os.path.isfile(os.path.join(unzipdir,imgname+"01f.sard")):
                os.rename(os.path.join(unzipdir,imgname+"01f.sard"), os.path.join(unzipdir,imgname+".img"))

            else:
                logger.error("could not find a CIS CEOS data file named *.sard")
                return "error", "error", "error"

            break

        # Here is a RSAT-1 ASF_CEOS
        elif imgext == ".D" or imgext == ".L":
            fname = imgname+".D"
            sattype = "ASF_CEOS"

            break
        
        elif imgext == ".safe":
            fname = 'manifest.safe'
            sattype = "SEN-1"
            imgname, imgext = os.path.splitext(fname)
            
            break

        else:
            countdown-1

    if countdown == 0:
        logger.error("This zipfile contents are not recognised")
        return "error", "error", "error"

    else:
        try:
            logger.handlers = []
        except:
            pass
        return fname, imgname, sattype


def getZipRoot(zip_file, tmpDir):
    """
    Looks into a zipfile and determines if the contents will unzip into a subdirectory
    (named for the zipfile); or a sub-subdirectory; or no directory at all (loose files)
    
    Run this function to determine where the files will be unzipped to. If the files are in
    the immediate subfolder, then that is what is required.
        
    Returns the unzipdir (where the files will -or should- go) and zipname (basename of the zipfile)

    **Parameters**
        
        *zip_file* : full path, name and ext of a zip file

        *tmpDir*   : this is the path to the directory where you are working with this file (the path of the zip_file - or wrkdir)

    **Returns**

        *unzipdir* : the directory where the zip file will/should unzip to

        *zipname*  : basename of the zip file AND/OR the name of the folder where the image files are  
    """

    path = os.path.dirname(zip_file)            # Directory of zip_file
    zipfname = os.path.basename(zip_file)       # File name of zip_file
    zipname, ext = os.path.splitext(zipfname)   # Seperate the extension and the name

    os.chdir(path)      # Change the working directory to path

    zip = zipfile.ZipFile(zip_file)     # Open the zip_file as an object
    dirlist = zip.namelist()            # List of all the files in zip_file to be used as the loop iterative element
    countdown = len(dirlist)            # Number of files in zip_file to be used as a counter inside the loop

    for f in dirlist:
        fsplit = f.split("/")       # Seperate the directories if any

        if len(fsplit) > 1 and fsplit[0] == zipname:        # Files are in a directory
            unzipdir = tmpDir       # Unzipdir will be their own subdirectory

            if fsplit[1] == zipname:    # Takes care of new CIS data for CI2D3 with nested subdirectories
                nesteddir = 1       # Determines if the unzipdir has sub sub dirs
            else:
                nesteddir = 0
            break

        elif len(fsplit) > 2 and fsplit[0] == zipname and fsplit[1] == zipname:
            unzipdir = tmpDir       # Unzipdir will be their own subdirectory
            nesteddir = 1
            break

        # Special case for zipfiles that have a subdirectory, but its name is different from zip_file
        elif len(fsplit) > 1 and fsplit[1] == 'schemas':    # If 'schemas' dir exist, then files are in a directory
            zipname = fsplit[0]     # Rename zipname with the name of the subdirectory in zip_file
            unzipdir = tmpDir       # Unzipdir will be their own subdirectory
            nesteddir = 0
            break

        elif fsplit[0].split('.')[1] == 'SAFE':
            zipname = fsplit[0]
            unzipdir = tmpDir
            nesteddir = 0
            break            
            
        else:
            countdown = countdown -1

    if countdown == 0:      # Files are not in a directory
        unzipdir = os.path.join(tmpDir, zipname)        # Unzipdir will be a new directory
        nesteddir = 0

    return unzipdir, zipname, nesteddir


def unZip(zip_file, unzipdir, ext='all'):
    """
    Unzips the zip_file to unzipdir with python's zipfile module.

    "ext" is a keyword that defaults to all files, but can be set
    to just extract a leader file L or xml for example.


    **Parameters**
       
       *zip_file* : Name of a zip file - with extension

        *unzipdir* : Directory to unzip to
        
    **Optional**

        *ext*      : 'all' or a specific ext as required
    """

    zip = zipfile.ZipFile(zip_file)     # Open the zip_file as an object

    if ext == 'all':        # Unzip all the files
        zip.extractall(path=unzipdir)       # Unzip/extract everything in zip_file to unzipdir

    else:       # Unzip only the files with the specified extension
        zippedfiles = zip.namelist()        # List of all the files in zip_file

        for filename in zippedfiles:
            if os.path.splitext(filename)[1] == '.'+ext:    # File matches the extension
                zip.extract(filename, path=unzipdir)        # Unzip/extract it to unzipdir

    zip.close()
    
def wktpoly2pts(wkt, bbox=False):
    """
    Converts a Well-known Text string for a polygon into a series of tuples that
    correspond to the upper left, upper right, lower right and lower left corners
    
    This works with lon/lat rectangles. 
    
    If you have a polygon that is not a rectangle, set bbox to True and the 
    bounding box corners will be returned
    
    Note that for rectangles in unprojected coordinates (lon/lat deg), this is 
    slightly different from ullr or llur (elsewhere in this project) which are 
    derived from bounding boxes of projected coordinates
    
    **Parameters**
        
        *wkt*         : a well-known text string for a polygon
    
    **Returns**
        
        *ul,ur,lr,ll* : a list of the four corners   
    """
    
    if bbox:
         poly = ogr.CreateGeometryFromWkt(wkt)
         bb = poly.GetEnvelope()
         ul = tuple(bb[0], bb[3])
         ur = tuple(bb[1], bb[3]) 
         lr = tuple(bb[1], bb[2])
         ll = tuple(bb[0], bb[2]) 
         
    else:
        wkt = wkt[9:-2] #strip out the wkt stuff
        assert len(wkt.split(',')) ==5, 'This is not a rectangle, use bbox=True'
        wkt = wkt.split(',')[0:4] # don't need the last one
        wkt = [g.strip() for g in wkt] # make sure no spaces except between
        ul = tuple([float(g) for g in wkt[0].split(' ')])
        ur = tuple([float(g) for g in wkt[1].split(' ')])
        lr = tuple([float(g) for g in wkt[2].split(' ')])
        ll = tuple([float(g) for g in wkt[3].split(' ')])
     
    return ul, ur, lr, ll

def llur2ullr(llur):
    """
    a function that returns:
    upperleft, lower right when given...
    lowerleft, upper right
    a list of tupples [(x,y),(x,y)]

    Note - this will disappoint if proj is transformed (before or after)
    
    **Parameters**
        
        *llur* : a list of tupples [(x,y),(x,y)] corresponding to lower left, upper right corners of a bounding box
    """
    
    ll = llur[0]
    ur = llur[1]

    ul = (ll[0], ur[1])
    lr = (ur[0], ll[1])

    return ul,lr

def ullr2llur(ullr):
    """
    a function that returns:
    lowerleft, upper right when given...
    upperleft, lower right
    a list of tupples [(x,y),(x,y)]

    Note - this will disappoint if proj is transformed (before or after)
     
    **Parameters**
        
        *ullr* : a list of tupples [(x,y),(x,y)] corresponding to upper right, lower left corners of a bounding box
    """
    
    ul = ullr[0]
    lr = ullr[1]

    ur = (lr[0], ul[1])
    ll = (ul[0], lr[1])

    return ll,ur    

def reprojSHP(in_shp, vectdir, proj, projdir):
    """
    Opens a shapefile, saves it as a new shapefile in the same directory
    that is reprojected to the projection wkt provided.

    **Note:** this could be expanded to get polyline data from polygon data
    for masking lines (not areas) ogr2ogr -nlt MULTILINESTRING

    **Parameters**
        
        *in_shp*  :

        *vectdir* :

        *proj*    :

        *projdir* :

    **Returns**
        
        *out_shp* : name of the proper shapefile
    """
    
    extlist = ['.shp', '.dbf','.prj','.shx']

    #load the srs from the reference wkt
    img_wkt = os.path.join(projdir, proj+'.wkt')
    img_srs = osr.SpatialReference()
    img_srs.ImportFromWkt(img_wkt)

    vect_srs = osr.SpatialReference()
    #driver = ogr.GetDriverByName('ESRI Shapefile')

    ds = ogr.Open(os.path.join(vectdir, in_shp+'.shp'))

    lyr = ds.GetLayer(0)
    vect_srs = lyr.GetSpatialRef()

    if vect_srs.IsSame(img_srs):
        # if the spatial reference of the vector mask matches the images, then
        # do nothing and return the name of the 'good shapefile'
        return in_shp

    else:
        #if the spatial reference doesn't match then reproject the shapefile
        out_shp = in_shp+'_'+proj

        #first delete any shapefiles that might be old so they can be overwriten
        if os.path.isfile(os.path.join(vectdir, out_shp+'.shp')):  # if shp, assume they are all around...
            try:
                for ext in extlist:
                    os.remove(os.path.join(vectdir, out_shp+ext))  #Must remove because it won't overwrite!
            except:
                pass # the files don't all exist (or they can't be deleted (problems later!)
        #tr = osr.CoordinateTransformation(vect_srs, img_srs)

        #Now reproject
        # go for it outside the API - command line ogr2ogr
        cmd = 'ogr2ogr -f \"ESRI Shapefile\" -t_srs ' + os.path.join(projdir, proj+'.wkt') +\
        ' ' + os.path.join(vectdir, out_shp + '.shp')+ ' ' +\
        os.path.join(vectdir, in_shp +'.shp')+' ' + in_shp

        command =  shlex.split(cmd)

        subprocess.Popen(command).wait() # must wait until finished

    return out_shp  # return the name of the proper shapefile

def getdBScale(power):
    """
    Convert a SAR backscatter value from the linear power scale to the log dB scale

    **Note:** power must be a scalar or an array of scalars,negative powers will throw back NaN.

    **Parameters**
       
       *power* : backscatter in power units

    **Returns**
        
        *dB*    : backscatter in dB units
    """
    
    dB = 10 * numpy.log10(power)
    return dB

def getPowerScale(dB):
    """
    Convert a SAR backscatter value from the log dB scale to the linear power scale

    **Note:** dB must be a scalar or an array of scalars

    **Parameters**
        
        *dB*    : backscatter in dB units

    **Returns**
        
        *power* : backscatter in power units
    """
    
    power = pow(10.0, dB/10.0)
    return power

def az(pt1,pt2):
    """
    Calculates the great circle initial azimuth between two points
    in dd.ddd format. 
    This formula assumes a spherical earth.  Use Vincenty's formulae
    for better precision 
    
    https://en.wikipedia.org/wiki/Azimuth
    https://en.wikipedia.org/wiki/Vincenty%27s_formulae
    
    **Parameters:**
        
        *pt1* : point from (tuple of lon and lat)

        *pt2* : point to (tuple of lon and lat)
    
    **Returns**

        *az*  : azimuth from North in degrees
    """

    if (type(pt1) != tuple) or (type(pt2) != tuple):
        raise TypeError("Only tuples are supported as arguments")

    #convert to radians
    pt1 = list(map(math.radians, pt1))
    pt2 = list(map(math.radians, pt2))
    
    deltaLon = pt2[0]-pt1[0]
    
    x = math.cos(pt1[1])*math.tan(pt2[1]) - math.sin(pt1[1])*math.cos(deltaLon)
    az =  math.degrees(math.atan2(math.sin(deltaLon),x))
    return (az+360) % 360


def wkt2shp(shpname, vectdir, proj, projdir, wkt):
    """
    Takes a polygon defined by well-known-text and a projection name and outputs
    a shapefile into the current directory

    **Parameters**
        
        *shpname* :

        *vectdir* :

        *proj*    :

        *projdir* :

        *wkt*     :
    """
    
    if wkt == 0:
        return -1

    spatialReference = osr.SpatialReference()
    
    fname = os.path.join(projdir, proj+'.wkt')
    fwkt = open(fname, 'r')
    projwkt = fwkt.read()
    
    spatialReference.ImportFromWkt(projwkt)
    
    #spatialReference.SetWellKnownGeogCS('WGS84')
    driver = ogr.GetDriverByName('ESRI Shapefile')

    extlist = ['.shp', '.dbf','.prj','.shx']
    #first delete any shapefiles that might be old so they can be overwriten
    if os.path.isfile(os.path.join(vectdir, shpname+'.shp')):  # if shp, assume they are all around...
        try:
            for ext in extlist:
                os.remove(os.path.join(vectdir, shpname+ext))  #Must remove because it won't overwrite!
        except:
            pass # the files don't all exist (or they can't be deleted (problems later!)

    fout = shpname+'.shp'

    datasource = driver.CreateDataSource(os.path.join(vectdir, fout))
    layer = datasource.CreateLayer('layer', spatialReference, geom_type=ogr.wkbPolygon)
    feature = ogr.Feature(layer.GetLayerDefn())
    poly  = ogr.CreateGeometryFromWkt(wkt)
    # Set geometry
    feature.SetGeometryDirectly(poly)
    layer.CreateFeature(feature)

    # Clean up
    feature.Destroy()
    datasource.Destroy()
    del fout

    
def copyfiles(self, copylist, wrkdir, fname=None, export=False, loghandler=None):   
    """
    Copies files from a local archive. If file could not be found, check that the 
    drive mapping is correct (above).   
    
    
    **Parameters**
        
        *copylist* : a list of images + inst             

        *wrkdir*   : working directory
        
        *fname*    : Filename if exporting copylist as txt
        
        *export*   : True if want to export copylist as txt
    """
    
    if loghandler != None:
        loghandler = loghandler             #Logging setup if loghandler sent, otherwise, set up a console only logging system
        logger = logging.getLogger(__name__)
        logger.addHandler(loghandler)
        logger.propagate = False   
        logger.setLevel(logging.DEBUG)
    else:
        logger = logging.getLogger(__name__)                        
        logger.setLevel(logging.DEBUG)
        logger.addHandler(logging.StreamHandler())    
    
    for fname in copylist:
        destination = os.path.join(wrkdir,os.path.split(fname)[1])
        if not os.path.isfile(fname):
            self.logger.error(fname + ' does not exist')
            continue        
        if os.path.isfile(destination):
            self.logger.debug(destination + " exists already and will not be copied")
        else:
            shutil.copy(fname,wrkdir)
            self.logger.debug("Copied File " + fname)
    self.logger.info("Finished File Copy")
    
    if export:
        fout = open(fname+".txt", 'w')

        for line in copylist:
            fout.write(line+'\n')
    
        fout.close() 

def interpolate_bilinear_matrix(Q, x, y):
    """
    Peforms a bilinear interpolation of matrix Q at the specified x (length M), y (length N) points and
    returns an M x N array of the interpolated values
    
    **Parameters**
        
        *Q*        : 2x2 array of known values located at the corners of x and y     

        *x*        : x points at which to interpolate Q-value
        
        *y*        : y points at which to interpolate Q-value
        
    """

    Q = numpy.matrix(Q)

    normalization_factor = 1/((x[-1]-x[0])*(y[-1]-y[0]))

    Y = numpy.asarray([[y[-1]-y],[y-y[0]]])
    X = numpy.asarray([x[-1]-x, x-x[0]])

    interp_y = Q*Y

    interp = normalization_factor*(X.transpose()*interp_y)
    
    return numpy.asarray(interp)

def interpolate_bilinear(P_corr, Pixels, Lines, x_matrix, y_matrix, z_matrix):
    x, y, z = numpy.empty(P_corr.shape), numpy.empty(P_corr.shape), numpy.empty(P_corr.shape)
    for l in range(len(P_corr)):
        for p in range(len(P_corr[l])):
            u = Pixels[l][int(P_corr[l][p])]
            v = Lines[l][p]
            L = Lines[l][p]
            if (p+1) < len(P_corr[l]) and (l+1) < len(P_corr):
                x_square = x_matrix[l:l+2,p:p+2]
                y_square = y_matrix[l:l+2,p:p+2]
                z_square = z_matrix[l:l+2,p:p+2]
                x[l][p] = (u+1-P_corr[l][p])*(v+1-L)*x_square[0][0] + (P_corr[l][p]-u)*(v+1-L)*x_square[0][1] + (u+1-P_corr[l][p])*(L-v)*x_square[1][0] + (P_corr[l][p]-u)*(L-v)*x_square[0][0]
                y[l][p] = (u+1-P_corr[l][p])*(v+1-L)*y_square[0][0] + (P_corr[l][p]-u)*(v+1-L)*y_square[0][1] + (u+1-P_corr[l][p])*(L-v)*y_square[1][0] + (P_corr[l][p]-u)*(L-v)*y_square[0][0]
                z[l][p] = (u+1-P_corr[l][p])*(v+1-L)*z_square[0][0] + (P_corr[l][p]-u)*(v+1-L)*z_square[0][1] + (u+1-P_corr[l][p])*(L-v)*z_square[1][0] + (P_corr[l][p]-u)*(L-v)*z_square[0][0]
            else:
                x[l][p] = 0
                y[l][p] = 0
                z[l][p] = 0

    return x,y,z

def geographic_to_cartesian(lat, lng, a, b):
    """
    transforms geographic latitude and longitude to cartesian space.
    
    **Parameters**
        
        *lat*        : latitude values to be transformed (can be single value, an array, or M x N matrix)

        *lng*        : longitude values to be transformed (can be single value, an array, or M x N matrix)
        
        *a*        : semi major axis of reference ellipse (float)
        
        *b*        : semi minor axis of reference ellipse (float)
        
    """

    normalization_factor = numpy.sqrt((a**2)*(numpy.cos(lat)**2)+ \
                                   (b**2)*(numpy.sin(lat)**2))
    x = (a**2)*numpy.cos(lat)*numpy.cos(lng)/normalization_factor
    y = (a**2)*numpy.cos(lat)*numpy.sin(lng)/normalization_factor
    z = (b**2)*numpy.sin(lat)/normalization_factor

    return x, y, z

def cartesian_to_geographic(x, y, z, a, b):
    """
    transforms cartesian space to geographic latitude and longitude
    
    **Parameters**
        
        *x*        : x values to be transformed (can be single value, an array, or M x N matrix)

        *y*        : y values to be transformed (can be single value, an array, or M x N matrix)

        *z*        : z values to be transformed (can be single value, an array, or M x N matrix)
        
        *a*        : semi major axis of reference ellipse (float)
        
        *b*        : semi minor axis of reference ellipse (float)
        
    """
    lat = numpy.arctan((a*z)/(b*numpy.sqrt(b**2-z**2)))
    lng = numpy.arctan2(y,x)
    return numpy.degrees(lat), numpy.degrees(lng)