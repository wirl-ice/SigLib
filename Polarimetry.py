from Database import Database
from SigLib import SigLib
from Image import Image
import Util
import logging
from glob import glob
import os
import shutil
import gc
import sys

try:
    sys.path.append('C:\\Users\\Cameron\\.snap\\snap-python')
    import snappy
    from snappy import ProductIO
    from snappy import GPF
    from snappy import GeoPos
except:
    print("Failure to import snappy!")
    sys.exit(-1)

try:
    GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()
    HashMap = snappy.jpy.get_type('java.util.HashMap')
    SubsetOp = snappy.jpy.get_type('org.esa.snap.core.gpf.common.SubsetOp')
    ReprojectOp = snappy.jpy.get_type('org.esa.snap.core.gpf.common.reproject.ReprojectionOp')
    BandMathsOp = snappy.jpy.get_type('org.esa.snap.core.gpf.common.BandMathsOp$BandDescriptor')
    gc.enable()
except:
    sys.exit(-1)

global FileNames
global logger

"""
Polarimetry.py is a standalone script that utilizes the functionality of SigLib. The purpose of the script is to perform
polarimetric calculations on quad-pol RADARSAT-2 scenes that contain ice islands (very large, often flat-topped icebergs).
Ice islands in the Canadian Arctic have been tracked for numerous studies using satellite tracking beacons that ping their 
location at a given interval. This data is used to find quad-pol images that contain ice islands. The script takes hand-drawn
outlines of the ice islands found in a given quad-pol scene, and masks the scene to both the outline of the ice island, and
to a 400 metre-wide donut around the ice island. Polarimetric variables available in SNAP-ESA are then computed onto each of the two
masks one at a time, the statistics from which are uploaded to a database table. 


"""


def snapCalibration(img, siglib, tmpdir, outDataType='sigma', saveInComplex=False):
    '''
    This fuction calibrates radarsat images into sigma, beta, or gamma

    **Parameters**

        *outDataType* : Type of calibration to perform (sigma, beta, or gamma), default is sigma

        *saveInComplex* : Output complex sigma data (for polarimetric mode)
    '''

    global FileNames

    os.chdir(tmpdir)
    img.openDataset(img.fname, img.path)
    outname = img.fnameGenerate()[2]

    img = os.path.join(img.path, img.fname)
    sat = ProductIO.readProduct(img)

    output = os.path.join(tmpdir, outname)

    parameters = HashMap()
    if saveInComplex:
        parameters.put('outputImageInComplex', 'true')
    else:
        parameters.put('outputImageInComplex', 'false')

        if outDataType == 'sigma':
            parameters.put('outputSigmaBand', True)
        elif outDataType == 'beta':
            parameters.put('createBetaBand', True)
            parameters.put('outputBetaBand', True)
            parameters.put('outputSigmaBand', False)
        elif outDataType == 'gamma':
            parameters.put('outputSigmaBand', False)
            parameters.put('createGammaBand', True)
            parameters.put('outputGammaBand', True)
        else:
            logger.error('Valid out data type not specified!')
            return Exception

    target = GPF.createProduct("Calibration", parameters, sat)
    ProductIO.writeProduct(target, output, 'GeoTiff')

    FileNames.append(outname + '.tif')

def makeAmp(siglib, img, newFile=True, save=True):
    '''
    Use snap bandMaths to create amplitude band for SLC products

    **Parameters**

        *newFile* :  True if the inname is the product file

        *save*    :  True if output filename should be added into internal filenames array for later use
    '''

    count = 0
    bands = snappy.jpy.array('org.esa.snap.core.gpf.common.BandMathsOp$BandDescriptor', siglib.meta.n_bands)

    if newFile:
        outname = img.fnameGenerate()[2]
        inname = os.path.join(img.path, siglib.fname)
        output = os.path.join(siglib.tmpDir, outname)
    else:
        inname = FileNames[-1]
        output = os.path.splitext(FileNames[-1])[0] + '_amp'

    while count < siglib.meta.n_bands:
        if siglib.meta.n_bands == 1:
            band = str(img.bandNames)
        else:
            band = str(img.bandNames[count])[1:]

        target = BandMathsOp()
        target.name = 'Amplitude_' + band
        target.type = 'float32'
        target.expression = 'sqrt(pow(i_' + band + ', 2) + pow(q_' + band + ', 2))'
        bands[count] = target
        count += 1

    parameters = HashMap()
    product = ProductIO.readProduct(inname)
    parameters.put('targetBands', bands)
    t = GPF.createProduct('BandMaths', parameters, product)
    ProductIO.writeProduct(t, output, 'BEAM-DIMAP')
    count += 1
    if save:
        FileNames.append(output + '.dim')


# POLARIMETRIC SPECIFIC
def beaconIntersections(db, beacontable, granule):
    """
    Get id, lat and long of beacons that intersect the image within a ninty minutes of the image data being collected

    **Parameters**

        *beacontable* : database table containing beacon tracks

        *granule* : unique identifier of image being analysed

    **Returns**

        *rows* : all beacon pings that meet requirements. Each row has three columns: beacon id, lat, and long
    """

    sql = """SELECT """ + beacontable + """.beacnid, """+ beacontable + """.latitud, """ + beacontable + """.longitd """ +\
    """FROM """ + db.table_to_query +""", """ + beacontable + \
    """ WHERE """ + db.table_to_query + """.granule = %(granule)s """ + \
    """AND """ + beacontable + """.dtd_utc <= """ + db.table_to_query + """.acdatetime + interval '91 minutes' """ +\
    """AND """ + beacontable + """.dtd_utc >= """ + db.table_to_query + """.acdatetime - interval '91 minutes' """ +\
    """AND ST_Contains(ST_Transform(""" + db.table_to_query + """.geom, 4326), ST_Transform(""" + beacontable + """.geom, 4326)) """ +\
    """ORDER BY """ + beacontable + """.beacnid, ((DATE_PART('day', """ + beacontable + """.dtd_utc - """ + db.table_to_query + """.acdatetime)*24 + """ +\
    """DATE_PART('hour', """ + beacontable + """.dtd_utc - """ + db.table_to_query + """.acdatetime))*60 + """ +\
    """DATE_PART('minute', """ + beacontable + """.dtd_utc - """ + db.table_to_query + """.acdatetime))*60 + """ +\
    """DATE_PART('second', """ + beacontable + """.dtd_utc - """ + db.table_to_query + """.acdatetime) ASC"""

    param = {'granule': granule}
    curs = db.connection.cursor()

    try:
        curs.execute(sql, param)
    except Exception as e:
        logger.error('ERROR(programming): Confirm the SQL statement is valid--> ' + str(e))
        return

    db.connection.commit()

    rows = curs.fetchall()

    return rows

# POLARIMETRIC SPECIFIC
def polarimetricDonuts(db, granule, beaconid):
    '''
    **Parameters**

        *granule* : unique name of an image in string format

        *beaconid* : identification number of a tracking beacon assocated with this image

    **Returns**

        *results* : array of polygonal masks in wkt format associated witht eh above beacon and image
    '''

    curs = db.connection.cursor()

    param = {'granule': granule}

    sql = "SELECT dimgname FROM " + db.table_to_query + " WHERE granule LIKE %(granule)s"
    curs.execute(sql, param)
    dimgname = str(curs.fetchone()[0])

    param = {'beacnid': beaconid, 'dimgname': dimgname, 'srid': '4326'}

    sql2 = """SELECT ST_AsText(ST_Transform(ST_Buffer(ii_polygons.geom, -30), 4326)) FROM ii_polygons """ + \
           """WHERE ii_polygons.beaconid = %(beacnid)s AND %(dimgname)s LIKE ii_polygons.imgref"""

    results = []
    # How to execute each command and get buffer polygon back to add to results above
    try:
        curs.execute(sql2, param)
    except Exception as e:
        logger.error(e)
        return []
    result = str(curs.fetchone()[0])
    if result == 'GEOMETRYCOLLECTION EMPTY':
        return []
    results.append(result)

    sql3 = """SELECT ST_AsText(ST_Transform(ST_Multi(ST_Difference(ST_Buffer(ii_polygons.geom, 400), ST_Buffer(ii_polygons.geom, 30))), 4326)) """ + \
           """FROM ii_polygons WHERE ii_polygons.beaconid = %(beacnid)s AND %(dimgname)s LIKE ii_polygons.imgref"""

    try:
        curs.execute(sql3, param)
    except Exception as e:
        logger.error(e)
        return []
    result = str(curs.fetchone()[0])
    if result == 'GEOMETRYCOLLECTION EMPTY':
        return []
    results.append(result)

    return results

# TO BE REMOVED
def polarimetric(db, siglib, img, zipfile, newTmp):
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

    snapCalibration(img, siglib, newTmp, saveInComplex=True)


    beaconTable = 'beacon_tracks'
    beacons = beaconIntersections(db, beaconTable, siglib.granule)  # Get beacon instances that overlap this image

    if len(beacons) == 0:
        logger.debug(siglib.granule + ' has no beacons associated with it!')
        return

    logger.debug("Found " + str(len(beacons)) + " beacons in this image!")

    seenBeacons = []
    FileNames = []
    for i in range(len(beacons)):
        os.chdir(newTmp)

        r = len(FileNames) - 1

        while r > 1:
            FileNames.remove(FileNames[r])
            r -= 1

        beaconid = beacons[i][0]
        latitude = float(beacons[i][1])
        longitude = float(beacons[i][2])

        if beaconid in seenBeacons:
            logger.debug("Seen best beacon for " + str(beaconid) + ", moving on")
            continue

        # sar_img.cleanFileNames()
        logger.debug("Found beacon " + str(beaconid) + " in image " + siglib.granule + ". Processing...")
        seenBeacons.append(beaconid)

        finalsDir = os.path.join(newTmp, 'finals_' + str(beaconid))  # directory in tmp for images to be masked
        if os.path.isdir(finalsDir):
            pass
        else:
            os.makedirs(finalsDir)

        try:
            snapSubset(siglib, img, newTmp, beaconid, latitude, longitude, finalsDir)  # subset
        except:
            logger.error("Problem with subset, moving on")
            continue

        # sar_img.makeAmp(newFile = False, save = False)

        matrices = ['C3', 'T3']

        tmpArr = FileNames
        for matrix in matrices:
            FileNames = tmpArr
            matrix_generation(matrix)  # Generate each type of matrix, one at a time

        logger.debug("All matrices generated!")

        decompositions = ['Sinclair Decomposition', 'Pauli Decomposition', 'Freeman-Durden Decomposition',
                          'Yamaguchi Decomposition', 'van Zyl Decomposition', 'H-A-Alpha Quad Pol Decomposition',
                          'Cloude Decomposition', 'Touzi Decomposition']

        for decomposition in decompositions:
            try:
                if decomposition == "H-A-Alpha Quad Pol Decomposition":
                    i = 1
                    while i <= 4:
                        decomposition_generation(decomposition, outputType=i)
                        i += 1

                elif decomposition == "Touzi Decomposition":
                    i = 5
                    while i <= 8:
                        decomposition_generation(decomposition, outputType=i)
                        i += 1

                else:
                    decomposition_generation(decomposition)

            except:
                logger.error("Error with " + decomposition + ", moving on")

        logger.debug('All matrix-decomposition combinations generated!')

        masks = polarimetricDonuts(db, siglib.granule, beaconid)
        count = 0
        for mask in masks:
            if count == 0:
                type = 'ii'
            elif count == 1:
                type = 'donut'
            else:
                break

            uploads = os.path.join(finalsDir, 'uploads_' + str(beaconid) + type)
            if os.path.isdir(uploads):
                pass
            else:
                os.makedirs(uploads)

            shpname = 'instmask_' + siglib.zipname + '_' + str(beaconid) + '_' + type
            Util.wkt2shp(shpname, finalsDir, '4326', siglib.projDir, mask)

            for dirpath, dirnames, filenames in os.walk(finalsDir):
                for filename in filenames:
                    if filename.endswith('.dim'):
                        name = os.path.splitext(filename)[0]
                        try:
                            slantRangeMask(shpname, name, finalsDir, uploads)
                        except:
                            logger.error("Error with masking!")

            for dirpath, dirnames, filenames in os.walk(uploads):
                for filename in filenames:
                    if filename.endswith('.img'):
                        os.chdir(dirpath)
                        name = os.path.splitext(filename)[0]
                        try:
                            imgData = img.getBandData(1, name + '.img')
                            db.imgData2db(imgData, name + '_' + type, str(beaconid), siglib.sar_meta.dimgname, siglib.granule, table='polarimetryData')
                        except:
                            logger.error("Unable to extract image data for {}! Skipping scene".format(name))

            count += 1

# POLARIMETRIC SPECIFIC?
def matrix_generation(matrix):
    '''
    Generate chosen matrix for calibrated quad-pol product

    **Parameters**

        *matrix* : matrix to be generated. options include: C3, C4, T3, or T4

    **Returns**

        *output* : filename of new product
    '''

    output = os.path.splitext(FileNames[-1])[0] + '_' + matrix
    inname = FileNames[-1]

    rsat = ProductIO.readProduct(inname)
    parameters = HashMap()

    parameters.put('matrix', matrix)

    target = GPF.createProduct('Polarimetric-Matrices', parameters, rsat)
    parameters2 = HashMap()

    parameters2.put('filter', 'Refined Lee Filter')
    parameters2.put('windowSize', '5x5')
    parameters2.put('numLooksStr', '3')

    target2 = GPF.createProduct('Polarimetric-Speckle-Filter', parameters, target)

    ProductIO.writeProduct(target2, output, 'BEAM-DIMAP')

    logger.debug(matrix + ' generated sucessfully')

    FileNames.append(output + '.dim')

# POLARIMETRIC SPECIFIC
def decomposition_generation(decomposition, outputType=0):
    '''
    Generate chosen decomposition on a fully polarimetric product, if looking to create an amplitude image with quad-pol data, set amp to True
    and send either a Pauli Decomposition or Sinclair Decomposition

    **Parameters**

        *decomposition* : decomposition to be generated. options include: Sinclair Decomposition, Pauli Decomposition, Freeman-Durden Decomposition, Yamagushi Decomposition, van Zyl Decomposition, H-A-Alpha Quad Pol Decomposition, Cloude Decomposition, or Touzi Decompositio

        *outputType* : option to select which set of output parameters that will be used for the Touzi or HAAlpha (1-8, leave 0 for none)

    **Returns**

        *output* : filename of new product
    '''


    output = os.path.splitext(FileNames[-1])[0] + '_' + decomposition
    inname = FileNames[-1]

    rsat = ProductIO.readProduct(inname)
    parameters = HashMap()

    parameters.put('decomposition', decomposition)
    if outputType == 1:
        parameters.put('outputAlpha123', True)
        output = output + "_1"
    elif outputType == 2:
        parameters.put('outputBetaDeltaGammaLambda', True)
        output = output + "_2"
    elif outputType == 3:
        parameters.put('outputHAAlpha', True)
        output = output + "_3"
    elif outputType == 4:
        parameters.put('outputLambda123', True)
        output = output + "_4"
    elif outputType == 5:
        parameters.put('outputTouziParamSet0', True)
        output = output + "_1"
    elif outputType == 6:
        parameters.put('outputTouziParamSet1', True)
        output = output + "_2"
    elif outputType == 7:
        parameters.put('outputTouziParamSet2', True)
        output = output + "_3"
    elif outputType == 8:
        parameters.put('outputTouziParamSet3', True)
        output = output + "_4"

    try:
        parameters.put('windowSize', 5)
    except:
        pass

    target = GPF.createProduct('Polarimetric-Decomposition', parameters, rsat)
    ProductIO.writeProduct(target, output, 'BEAM-DIMAP')

    logger.debug(decomposition + ' generated sucessfully')


def slantRangeMask(mask, inname, workingDir, uploads):
    '''
    This function takes a wkt with lat long coordinates, and traslates it into a wkt in line-pixel coordinates

    **Parameters**

        *mask*  :  wkt file of a polygonal mask in wgs84 coordinates

        *inname*  :  snap product to cross-reference mask corrdinated to convert them to slant-range

    **Returns**

        *newMask*  : wkt file of a polygonal mask in slant-range line-pixel coordinates
    '''

    input = os.path.join(workingDir, inname + '.dim')
    output = os.path.join(workingDir, inname)

    rsat = ProductIO.readProduct(input)

    parameters = HashMap()
    parameters.put('vectorFile', os.path.join(workingDir, mask + '.shp'))

    t = GPF.createProduct('Import-Vector', parameters, rsat)
    ProductIO.writeProduct(t, output, 'BEAM-DIMAP')

    masking = ProductIO.readProduct(input)
    parameters = HashMap()
    parameters.put('geometry', mask + '_1')

    o = GPF.createProduct('Land-Sea-Mask', parameters, masking)
    ProductIO.writeProduct(o, os.path.join(uploads, inname), 'BEAM-DIMAP')

def snapSubset(siglib, img, tmpDir, idNum, lat, longt, dir):
    '''
    Using lat long provided, create bounding box 1200x1200 pixels around it, and subset this
    (This requires id, lat, and longt)
    OR
    Using ul and lr coordinates of bounding box, subset
    (This requires idNum and ullr)

    **parameters**

        *idNum* : id # of subset region (for filenames, each subset region should have unique id)

        *lat* : latitiude of beacon at centre of subset (Optional)

        *longt* : longitude of beacon at centre of subset (Optional)

        *dir* : location output will be stored

        *ullr* : Coordinates of ul and lr corners of bounding box to subset to (Optional)

    '''

    inname = os.path.join(tmpDir, FileNames[-1])
    output = os.path.join(dir, os.path.splitext(FileNames[-1])[0] + '__' + str(idNum) + '_subset')

    rsat = ProductIO.readProduct(inname)
    info = rsat.getSceneGeoCoding()

    geoPosOP = snappy.jpy.get_type('org.esa.snap.core.datamodel.PixelPos')


    pixel_pos = info.getPixelPos(GeoPos(lat, longt), geoPosOP())

    x = int(round(pixel_pos.x))
    y = int(round(pixel_pos.y))

    topL_x = x - 600
    if topL_x <= 0:
        topL_x = 0
    topL_y = y - 600
    if topL_y <= 0:
        topL_y = 0
    width = 1200
    if (x + width) > img.n_cols:
        width = img.n_cols
    height = 1200
    if (x + height) > img.n_rows:
        height = img.n_rows

    parameters = HashMap()

    parameters.put('region', "%s,%s,%s,%s" % (topL_x, topL_y, width, height))
    target = GPF.createProduct('Subset', parameters, rsat)
    ProductIO.writeProduct(target, output, 'BEAM-DIMAP')

    logger.debug("Subset complete on " + siglib.zipname + " for id " + str(idNum))
    FileNames.append(output + '.dim')

def main():
    global FileNames
    global logger
    siglib = SigLib()
    siglib.createLog()

    loghandler = siglib.loghandler  # Logging setup if loghandler sent, otherwise, set up a console only logging system
    logger = logging.getLogger(__name__)
    logger.addHandler(loghandler)
    logger.propagate = False
    logger.setLevel(logging.DEBUG)

    db = Database(siglib.table_to_query, siglib.dbName, loghandler=siglib.loghandler, host=siglib.dbHost)

    ziproots = []

    for dirpath, dirnames, filenames in os.walk(siglib.scanDir):
        ziproots.extend(glob(os.path.join(dirpath, siglib.scanFor)))

    ziproots.sort()  # Nice to have this in some kind of order

    for zipfile in ziproots:
        FileNames = []
        siglib.proc_File(zipfile, cleanup=False)

        newTmp = os.path.join(siglib.tmpDir, siglib.granule)

        img = Image(siglib.fname, siglib.unzipdir, siglib.sar_meta, siglib.imgType, siglib.imgFormat, siglib.zipname, siglib.imgDir, newTmp, loghandler, initOnly=True)

        if 'Q' not in siglib.sar_meta.beam:
            logger.debug("This is not a quad-pol scene, skipping")
            return Exception

        polarimetric(db, siglib, img, zipfile, newTmp)

    db.removeHandler()

def parallel(zipfile):
    global FileNames
    global logger
    siglib = SigLib()
    siglib.createLog()

    loghandler = siglib.loghandler  # Logging setup if loghandler sent, otherwise, set up a console only logging system
    logger = logging.getLogger(__name__)
    logger.addHandler(loghandler)
    logger.propagate = False
    logger.setLevel(logging.DEBUG)

    db = Database(siglib.table_to_query, siglib.dbName, loghandler=siglib.loghandler, host=siglib.dbHost)

    FileNames = []
    siglib.proc_File(zipfile, cleanup=False)

    newTmp = os.path.join(siglib.tmpDir, siglib.granule)

    img = Image(siglib.fname, siglib.unzipdir, siglib.sar_meta, siglib.imgType, siglib.imgFormat, siglib.zipname,
                siglib.imgDir, newTmp, siglib.projDir, loghandler, initOnly=True)

    if 'Q' not in siglib.sar_meta.beam:
        logger.debug("This is not a quad-pol scene, skipping")
        return Exception

    polarimetric(db, siglib, img, zipfile, newTmp)

    db.removeHandler()

if __name__ == "__main__":
    main()
    #parallel(os.path.abspath(os.path.expanduser(str(sys.argv[-1]))))

    #Only run below if table to store results needs to be created (do before running either main() or parallel()
    """
    siglib = SigLib()
    db = Database(siglib.table_to_query, siglib.dbName, loghandler=None, host=siglib.dbHost)

    # create table to upload results
    sql = 'create table polarimetryData (granule varchar(100), bandname varchar(100), inst int, dimgname varchar(100), mean varchar(25), var varchar(25),\
            maxdata varchar(25), mindata varchar(25), median varchar(25), quart1 varchar (25), quart3 varchar(25), skew varchar (25), kurtosis varchar (25))'

    db.qryFromText(sql)
    """
