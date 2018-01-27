# -*- coding: utf-8 -*-
"""
**util.py**

This module contains miscellaneous code that helps siglib work with directories, 
zip files, clean up intermediate files and so on. 



**Created on** Tue Feb 12 20:04:11 2013 **@author:** Cindy Lopes
**Modified on** Sat Nov 23 14:49:18 2013 **@reason:** Added writeIssueFile and compareIssueFiles **@author:** Sougal Bouh Ali
**Modified on** Sat Nov 30 15:37:22 2013 **@reason:** Redesigned getFilname, getZipRoot and unZip **@author:** Sougal Bouh Ali

"""

import os
import zipfile
import subprocess
import math

from osgeo import osr
from osgeo import ogr
import shlex
import numpy


def getFilename(zipname, unzipdir):
    '''
    Given the name of a zipfile, return the name of the image,
    the file name, and the corresponding sensor/platform (satellite).

    **Parameters**
        *zipname*   : The basename of the zip file you are working with
        *unzipdir*  : Where the zipfile will unzip to (find out with getZipRoot)

    **Returns**
        *fname* :  The file name that corresponds to the image
        *imgname*   : The name of the image (the basename, sans extension)
        *sattype*   : The type of satellite/image format this file represents
    '''

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
                 print "No file named *.sarl found"
                 return "error", "error", "error"

            if os.path.isfile(os.path.join(unzipdir,imgname+".sart")):
                os.rename(os.path.join(unzipdir,imgname+".sart"), os.path.join(unzipdir,imgname+".trl"))

            else:
                print "could not find a trailer file"
                return "error", "error", "error"

            if os.path.isfile(os.path.join(unzipdir,imgname+".sard")):
                os.rename(os.path.join(unzipdir,imgname+".sard"), os.path.join(unzipdir,imgname+".img"))

            # Special case for im_radar*.zip files that have *01f.sard name
            elif os.path.isfile(os.path.join(unzipdir,imgname+"01f.sard")):
                os.rename(os.path.join(unzipdir,imgname+"01f.sard"), os.path.join(unzipdir,imgname+".img"))

            else:
                print "could not find a CIS CEOS data file named *.sard"
                return "error", "error", "error"

            break

        # Here is a RSAT-1 ASF_CEOS
        elif imgext == ".D" or imgext == ".L":
            fname = imgname+".D"
            sattype = "ASF_CEOS"

            break

        else:
            countdown-1

    if countdown == 0:
        print "This zipfile contents are not recognised"
        return "error", "error", "error"

    else:
        return fname, imgname, sattype

#TODO: this deletes the entire directory...fix it
def deltree(dirname):
    '''
    Delete all the files and sub-directories in a certain path

    **Parameters**
        *dirname*   :
    '''

    if os.path.exists(dirname):
        for root,dirs,files in os.walk(dirname):
                for dir in dirs:
                    deltree(os.path.join(root,dir))
                for file in files:
                    os.remove(os.path.join(root,file))
        os.rmdir(dirname)

def cleartree(dirname):

    if os.path.exists(dirname):
        for root,dirs,files in os.walk(dirname):
                for dir in dirs:
                    deltree(os.path.join(root,dir))
                for file in files:
                    os.remove(os.path.join(root,file))


def getZipRoot(zip_file, tmpDir):
    '''
    Looks into a zipfile and determines if the contents will unzip into a subdirectory
    (named for the zipfile); or a sub-subdirectory; or no directory at all (loose files)
    
    Run this function to determine where the files will be unzipped to. If the files are in
    the immediate subfolder, then that is what is required.
        
    Returns the unzipdir (where the files will -or should- go) and zipname (basename of the zipfile)

    **Parameters**
        *zip_file*  : full path, name and ext of a zip file
        *tmpDir*    : this is the path to the directory where you are 
                        working with this file (the path of the zip_file - or wrkdir)

    **Returns**
        *unzipdir*  : the directory where the zip file will/should unzip to
        *zipname*   : basename of the zip file AND/OR the name of the folder where the image files are  
    '''

    path = os.path.dirname(zip_file)            # Directory of zip_file
    zipfname = os.path.basename(zip_file)       # File name of zip_file
    zipname, ext = os.path.splitext(zipfname)   # Seperate the extension and the name

    os.chdir(path)      # Change the working directory to path

    zip = zipfile.ZipFile(zip_file)     # Open the zip_file as an object
    dirlist = zip.namelist()            # List of all the files in zip_file to be used as the loop iterative element
    countdown = len(dirlist)            # Number of files in zip_file to be used as a counter inside the loop

    for f in dirlist:
        fsplit = f.split("/")       # Seperate the directories if any
        #print "fsplit: ", fsplit
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

        else:
            countdown = countdown -1

    if countdown == 0:      # Files are not in a directory
        unzipdir = os.path.join(tmpDir, zipname)        # Unzipdir will be a new directory
        nesteddir = 0

    return unzipdir, zipname, nesteddir


def unZip(zip_file, unzipdir, ext='all'):
    '''
    Unzips the zip_file to unzipdir with python's zipfile module.

    "ext" is a keyword that defaults to all files, but can be set
    to just extract a leader file L or xml for example.


    **Parameters**
        *zip_file*  : Name of a zip file - with extension
        *unzipdir*  : Directory to unzip to
        
    **Optional**
        *ext* : 'all' or a specific ext as required
    '''

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
    *wkt* : a well-known text string for a polygon
    
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
    '''a function that returns:
    upperleft, lower right when given...
    lowerleft, upper right
    a list of tupples [(x,y),(x,y)]

    Note - this will disappoint if proj is transformed (before or after)
    
    **Parameters**
    *llur* : a list of tupples [(x,y),(x,y)] corresponding to lower left, upper right corners of a bounding box
    '''
    ll = llur[0]
    ur = llur[1]

    ul = (ll[0], ur[1])
    lr = (ur[0], ll[1])

    return ul,lr

def ullr2llur(ullr):
    '''a function that returns:
    lowerleft, upper right when given...
    upperleft, lower right
    a list of tupples [(x,y),(x,y)]

    Note - this will disappoint if proj is transformed (before or after)
     
    **Parameters**
    *ullr* : a list of tupples [(x,y),(x,y)] corresponding to upper right, lower left corners of a bounding box
    '''
    ul = ullr[0]
    lr = ullr[1]

    ur = (lr[0], ul[1])
    ll = (ul[0], lr[1])

    return ll,ur
    

def reprojSHP(in_shp, vectdir, proj, projdir):
    '''
    Opens a shapefile, saves it as a new shapefile in the same directory
    that is reprojected to the projection wkt provided.

    **Note:** this could be expanded to get polyline data from polygon data
    for masking lines (not areas) ogr2ogr -nlt MULTILINESTRING

    **Parameters**
        *in_shp*    :
        *vectdir*   :
        *proj*      :
        *projdir*   :

    **Returns**
        *out_shp*   :   name of the proper shapefile
    '''
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
    """Convert a SAR backscatter value from the linear power scale to the log dB scale

    **Note:** power must be a scalar or an array of scalars,negative powers will throw back NaN.

    **Parameters**
        *power*     :  backscatter in power units

    **Returns**
        *dB*   :  backscatter in dB units
    """
    dB = 10 * numpy.log10(power)
    return dB

def getPowerScale(dB):
    """Convert a SAR backscatter value from the log dB scale to the linear power scale

    **Note:** dB must be a scalar or an array of scalars

    **Parameters**
        *dB*     :  backscatter in dB units

    **Returns**
        *power*   :  backscatter in power units
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
    pt1 : point from (tuple of lon and lat)
    pt2 : point to (tuple of lon and lat)
    
    **Returns**
    az : azimuth from North in degrees
  
    """

    if (type(pt1) != tuple) or (type(pt2) != tuple):
        raise TypeError("Only tuples are supported as arguments")

    #convert to radians
    pt1 = map(math.radians, pt1)
    pt2 = map(math.radians, pt2)
    
    deltaLon = pt2[0]-pt1[0]
    
    x = math.cos(pt1[1])*math.tan(pt2[1]) - math.sin(pt1[1])*math.cos(deltaLon)
    az =  math.degrees(math.atan2(math.sin(deltaLon),x))
    return (az+360) % 360





def wkt2shp(shpname, vectdir, proj, projdir, wkt):
    '''
    Takes a polygon defined by well-known-text and a projection name and outputs
    a shapefile into the current directory

    **Parameters**
        *shpname*   :

        *vectdir*   :

        *proj*      :

        *projdir*   :

        *wkt*       :
    '''
    if wkt == 0:
        return -1

    spatialReference = osr.SpatialReference()
    fname = os.path.join(projdir, proj+'.wkt')
    fwkt = open(fname, 'r')
    projwkt = fwkt.read()
    spatialReference.ImportFromWkt(projwkt)

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
    layer = datasource.CreateLayer(shpname, geom_type=ogr.wkbPolygon, srs=spatialReference)
    feature = ogr.Feature(layer.GetLayerDefn())
    poly  = ogr.CreateGeometryFromWkt(wkt)
    # Set geometry
    feature.SetGeometryDirectly(poly)
    layer.CreateFeature(feature)

    # Clean up
    feature.Destroy()
    datasource.Destroy()
    del fout


def writeIssueFile(fname, delimiter):
    """
    Generates a clean list of zipfiles, when given an Issue File written by scripts.

    **Parameters**
        *fname*     :   name of the text file with extension to be cleaned(i.e. textfile.txt)

        *delimiter* :   separator used to split zipfile from unwanted errors (i.e. use most common " / " before zipfile)
    """

    fr = open(fname, "r+")
    name, ext = os.path.splitext(fname)
    fw = open(name+"_cleaned.txt", "w+")

    print "File read: ", fr.name
    print "File writen: ", fw.name
    print "\n"

    # Read lines 1 by 1
    for line in fr.readlines():
        string = line.split(delimiter)
        line.split()
        # Remove undesirable items in string
        for x in xrange(len(string)-1):
            string.pop(0)

        # Write string to a file
        for x in string:
            fw.write(x.lstrip())

    fr.close()
    fw.close()


def compareIssueFiles(file1, file2):
    '''
    Compares 2 clean issue files generated by writeIssueFile() and
    generates a separate file containing the list of matched & unmatched files in 2 files.

    **Parameters**
        *file1*     :   name of the first text file with extension to be compared (i.e. textfile.txt)

        *file2*     :   name of the second text file with extension to be compared (i.e. textfile.txt)

    '''
    # File pointers
    f1r = open(file1, "r+")
    f2r = open(file2, "r+")
    fxw = open("matchedFiles.txt", "w+")
    fyw = open("unmatchedFiles.txt", "w+")

    print "First filename: ", f1r.name
    print "Second filename: ", f2r.name
    print "\n"

    f1_lines = f1r.readlines()  # List of lines
    f2_lines = f2r.readlines()
    f3_lines = f1_lines[:]      # Copy of list f1_lines, but they are not linked

    countX = 0      # Counter for matched lines
    countY = 0      # Counter for unmatched lines

    # Compare line by line
    for f1_line in f1_lines:
        for f2_line in f2_lines:

            if f1_line == f2_line:      # Lines match
                countX = countX + 1     # Increase the number of matches
                fxw.write(f1_line)      # Write the matched line to file

                # Sorting out the unmatched lines
                for x in xrange(len(f3_lines)):
                    if f3_lines[x-1] == f1_line:    # If lines match
                        f3_lines.pop(x-1)           # Then remove this line from

    for f3_line in f3_lines:
        countY = countY + 1
        fyw.write(f3_line)

    print "The first list has ", len(f1_lines) ," files, and the second list has ", len(f2_lines) ,"files."
    print "There's ", countX ," matched files, and they have stored in ", fxw.name
    print "Please refer to ", fyw.name ," for the list of the ", countY ," files that have not been matched."

    f1r.close()
    f2r.close()
    fxw.close()
    fyw.close()