from Image import Image

# POLARIMETRIC SPECIFIC
def beaconIntersections(beacontable, granule):
    """
    Get id, lat and long of beacons that intersect the image within a ninty minutes of the image data being collected

    **Parameters**

        *beacontable* : database table containing beacon tracks

        *granule* : unique identifier of image being analysed

    **Returns**

        *rows* : all beacon pings that meet requirements. Each row has three columns: beacon id, lat, and long
    """

    sql = """SELECT """ + beacontable + """.beacnid, """+ beacontable + """.latitud, """ + beacontable + """.longitd """ +\
    """FROM """ + self.table_to_query +""", """ + beacontable + \
    """ WHERE """ + self.table_to_query + """.granule = %(granule)s """ + \
    """AND """ + beacontable + """.dtd_utc <= """ + self.table_to_query + """.acdatetime + interval '91 minutes' """ +\
    """AND """ + beacontable + """.dtd_utc >= """ + self.table_to_query + """.acdatetime - interval '91 minutes' """ +\
    """AND ST_Contains(ST_Transform(""" + self.table_to_query + """.geom, 96718), ST_Transform(""" + beacontable + """.geom, 96718)) """ +\
    """ORDER BY """ + beacontable + """.beacnid, ((DATE_PART('day', """ + beacontable + """.dtd_utc - """ + self.table_to_query + """.acdatetime)*24 + """ +\
    """DATE_PART('hour', """ + beacontable + """.dtd_utc - """ + self.table_to_query + """.acdatetime))*60 + """ +\
    """DATE_PART('minute', """ + beacontable + """.dtd_utc - """ + self.table_to_query + """.acdatetime))*60 + """ +\
    """DATE_PART('second', """ + beacontable + """.dtd_utc - """ + self.table_to_query + """.acdatetime) ASC"""

    param = {'granule': granule}
    curs = self.connection.cursor()

    try:
        curs.execute(sql, param)
    except psycopg2.ProgrammingError as e:
        self.logger.error('ERROR(programming): Confirm the SQL statement is valid--> ' + str(e))
        return

    self.connection.commit()

    rows = curs.fetchall()

    return rows

# POLARIMETRIC SPECIFIC
def polarimetricDonuts(self, granule, beaconid):
    '''
    **Parameters**

        *granule* : unique name of an image in string format

        *beaconid* : identification number of a tracking beacon assocated with this image

    **Returns**

        *results* : array of polygonal masks in wkt format associated witht eh above beacon and image
    '''

    curs = self.connection.cursor()

    param = {'granule': granule}

    sql = "SELECT dimgname FROM " + self.table_to_query + " WHERE granule LIKE %(granule)s"
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
        self.logger.error(e)
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
        self.logger.error(e)
        return []
    result = str(curs.fetchone()[0])
    if result == 'GEOMETRYCOLLECTION EMPTY':
        return []
    results.append(result)

    return results

# TO BE REMOVED
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
    newTmp = os.path.join(self.tmpDir, granule)

    beaconTable = 'beacon_tracks'
    beacons = db.beaconIntersections(beaconTable, granule)  # Get beacon instances that overlap this image

    if len(beacons) == 0:
        self.logger.debug(granule + ' has no beacons associated with it!')
        return

    self.logger.debug("Found " + str(len(beacons)) + " beacons in this image!")

    # Process the image
    sar_img = Image(fname, unzipdir, sar_meta, self.imgType, self.imgFormat, zipname, self.imgDir, newTmp,
                    self.loghandler, pol=True)

    seenBeacons = []
    for i in range(len(beacons)):
        os.chdir(newTmp)

        r = len(sar_img.FileNames) - 1

        while r > 1:
            sar_img.FileNames.remove(sar_img.FileNames[r])
            r -= 1

        beaconid = beacons[i][0]
        latitude = float(beacons[i][1])
        longitude = float(beacons[i][2])

        if beaconid in seenBeacons:
            self.logger.debug("Seen best beacon for " + str(beaconid) + ", moving on")
            continue

        # sar_img.cleanFileNames()
        self.logger.debug("Found beacon " + str(beaconid) + " in image " + granule + ". Processing...")
        seenBeacons.append(beaconid)

        finalsDir = os.path.join(newTmp, 'finals_' + str(beaconid))  # directory in tmp for images to be masked
        if os.path.isdir(finalsDir):
            pass
        else:
            os.makedirs(finalsDir)

        try:
            sar_img.snapSubset(beaconid, latitude, longitude, finalsDir)  # subset
        except:
            self.logger.error("Problem with subset, moving on")
            continue

        # sar_img.makeAmp(newFile = False, save = False)

        matrices = ['C3', 'T3']

        tmpArr = sar_img.FileNames
        for matrix in matrices:
            sar_img.FileNames = tmpArr
            sar_img.matrix_generation(matrix)  # Generate each type of matrix, one at a time

        self.logger.debug("All matrices generated!")

        decompositions = ['Sinclair Decomposition', 'Pauli Decomposition', 'Freeman-Durden Decomposition',
                          'Yamaguchi Decomposition', 'van Zyl Decomposition', 'H-A-Alpha Quad Pol Decomposition',
                          'Cloude Decomposition', 'Touzi Decomposition']

        for decomposition in decompositions:
            try:
                if decomposition == "H-A-Alpha Quad Pol Decomposition":
                    i = 1
                    while i <= 4:
                        sar_img.decomposition_generation(decomposition, outputType=i)
                        i += 1

                elif decomposition == "Touzi Decomposition":
                    i = 5
                    while i <= 8:
                        sar_img.decomposition_generation(decomposition, outputType=i)
                        i += 1

                else:
                    sar_img.decomposition_generation(decomposition)

            except:
                self.logger.error("Error with " + decomposition + ", moving on")

        self.logger.debug('All matrix-decomposition combinations generated!')

        masks = db.polarimetricDonuts(granule, beaconid)
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

            shpname = 'instmask_' + zipname + '_' + str(beaconid) + '_' + type
            Util.wkt2shp(shpname, finalsDir, self.proj, self.projDir, mask)

            for dirpath, dirnames, filenames in os.walk(finalsDir):
                for filename in filenames:
                    if filename.endswith('.dim'):
                        name = os.path.splitext(filename)[0]
                        try:
                            sar_img.slantRangeMask(shpname, name, finalsDir, uploads)
                        except:
                            self.logger.error("Error with masking!")

            for dirpath, dirnames, filenames in os.walk(uploads):
                for filename in filenames:
                    if filename.endswith('.img'):
                        os.chdir(dirpath)
                        name = os.path.splitext(filename)[0]
                        try:
                            imgData = sar_img.getBandData(1, name + '.img')
                            db.imgData2db(imgData, name + '_' + type, str(beaconid), sar_img.meta.dimgname, granule)
                        except:
                            self.logger.error("Unable to extract image data for {}! Skipping scene".format(name))

            count += 1

    sar_img.removeHandler()
    sar_meta.removeHandler()

# POLARIMETRIC SPECIFIC?
def matrix_generation(self, matrix):
    '''
    Generate chosen matrix for calibrated quad-pol product

    **Parameters**

        *matrix* : matrix to be generated. options include: C3, C4, T3, or T4

    **Returns**

        *output* : filename of new product
    '''

    output = os.path.splitext(self.FileNames[-1])[0] + '_' + matrix
    inname = self.FileNames[-1]

    rsat = ProductIO.readProduct(inname)
    parameters = self.HashMap()

    parameters.put('matrix', matrix)

    target = GPF.createProduct('Polarimetric-Matrices', parameters, rsat)
    parameters2 = self.HashMap()

    parameters2.put('filter', 'Refined Lee Filter')
    parameters2.put('windowSize', '5x5')
    parameters2.put('numLooksStr', '3')

    target2 = GPF.createProduct('Polarimetric-Speckle-Filter', parameters, target)

    ProductIO.writeProduct(target2, output, 'BEAM-DIMAP')

    self.logger.debug(matrix + ' generated sucessfully')

    self.FileNames.append(output + '.dim')

    # POLARIMETRIC SPECIFIC

def polarFilter(self):
    '''
    Apply a speckle filter on a fully polarimetric product

    **Parameters**

        *save*   : True if output filename should be added into internal filenames array for later use

    **Returns**

        *output* : filename of new product
    '''

    output = os.path.splitext(self.FileNames[-1])[0] + '_filter'
    inname = self.FileNames[-1]

    rsat = ProductIO.readProduct(inname)
    parameters = self.HashMap()

    parameters.put('filter', 'Refined Lee Filter')
    parameters.put('windowSize', '5x5')
    parameters.put('numLooksStr', '3')

    target = GPF.createProduct('Polarimetric-Speckle-Filter', parameters, rsat)
    ProductIO.writeProduct(target, output, 'BEAM-DIMAP')

    self.logger.debug('Filtering successful')

    self.FileNames.append(output + '.dim')

# POLARIMETRIC SPECIFIC
def decomposition_generation(self, decomposition, outputType=0, amp=False):
    '''
    Generate chosen decomposition on a fully polarimetric product, if looking to create an amplitude image with quad-pol data, set amp to True
    and send either a Pauli Decomposition or Sinclair Decomposition

    **Parameters**

        *decomposition* : decomposition to be generated. options include: Sinclair Decomposition, Pauli Decomposition, Freeman-Durden Decomposition, Yamagushi Decomposition, van Zyl Decomposition, H-A-Alpha Quad Pol Decomposition, Cloude Decomposition, or Touzi Decompositio

        *outputType* : option to select which set of output parameters that will be used for the Touzi or HAAlpha (1-8, leave 0 for none)

        *amp* : True if looking to generate Pauli or Sinclair decomposition to create quad-pol amp image

    **Returns**

        *output* : filename of new product
    '''

    if amp:
        os.chdir(self.tmpDir)

        inname = os.path.join(self.path, self.fname)
        outname = self.fnameGenerate()[2]
        output = os.path.join(self.tmpDir, outname)

        rsat = ProductIO.readProduct(inname)
        parameters = self.HashMap()

        parameters.put('decomposition', decomposition)
        target = GPF.createProduct('Polarimetric-Decomposition', parameters, rsat)
        ProductIO.writeProduct(target, output, 'BEAM-DIMAP')

        self.logger.debug(decomposition + ' generated sucessfully')
        self.FileNames.append(outname + '.dim')

    else:
        output = os.path.splitext(self.FileNames[-1])[0] + '_' + decomposition
        inname = self.FileNames[-1]

        rsat = ProductIO.readProduct(inname)
        parameters = self.HashMap()

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

        self.logger.debug(decomposition + ' generated sucessfully')


def slantRangeMask(self, mask, inname, workingDir, uploads):
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

    parameters = self.HashMap()
    parameters.put('vectorFile', os.path.join(workingDir, mask + '.shp'))

    t = GPF.createProduct('Import-Vector', parameters, rsat)
    ProductIO.writeProduct(t, output, 'BEAM-DIMAP')

    masking = ProductIO.readProduct(input)
    parameters = self.HashMap()
    parameters.put('geometry', mask + '_1')

    o = GPF.createProduct('Land-Sea-Mask', parameters, masking)
    ProductIO.writeProduct(o, os.path.join(uploads, inname), 'BEAM-DIMAP')

if __name__ == "__main__":
    if self.polarimetricProcess == '1':
        if 'Q' not in sar_meta.beam:
            self.logger.debug("This is not a quad-pol scene, skipping")
            return Exception
        db = Database(self.table_to_query, self.dbName, loghandler=self.loghandler, host=self.dbHost)
        self.polarimetric(db, fname, imgname, zipname, sattype, granule, zipfile, sar_meta, unzipdir)
        db.removeHandler()