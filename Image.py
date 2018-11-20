"""
**imgProcess.py**

**Created on** ??? Jul  ? ??:??:?? 2009 **@author:** Derek Mueller

This module creates an instance of class Img and opens a file to return a
gdal dataset to be processed into an amplitude, calibrated, noise or theta
(incidence angle) image, etc. This image can be subsequently projected,
cropped, masked, stretched, etc.

**Modified on** ??? Feb  ? ??:??:?? 2012 **@reason:** Repackaged for r2convert **@author:** Derek Mueller
**Modified on** 23 May 14:43:40 2018 **@reason:** Added logging functionality **@author:** Cameron Fitzpatrick
"""

import os
import numpy
import subprocess
import glob
import math
import logging
import shlex
import sys
import shutil
try:
    import snappy
    from snappy import ProductIO
    from snappy import GPF
except:
    pass
        
import gc

from osgeo import gdal
from osgeo.gdalconst import *
from osgeo import gdal_array

import Util

class Image(object):
    """
    This is the Img class for each image.  RSAT2, RSAT1 (ASF and CDPF)
    
    Opens the file specified by fname, passes reference to the meta class and declares the imgType of interest.

        **Parameters**
            
            *fname*     : filename

            *path*      : full directory path of the filename

            *meta*      : reference to the meta class

            *imgType*   : amp, sigma, noise, theta...

            *imgFormat* : gdal format code gtiff, vrt

            *zipname*   : name of the image's zipfile
    """

    def __init__(self, fname, path, meta, imgType, imgFormat, zipname, imgDir, tmpDir, loghandler = None):

#TODO - consider a secondary function to create the image so the class can be initialized without CPU time... 

        assert imgType in ['amp','sigma','noise','theta']
        assert imgFormat.lower() in ['gtiff','hfa','envi','vrt']

        self.status = "ok"  ### For testing
        self.tifname = ""   ### For testing
        self.fname_nosubest = ""
        
        if loghandler != None:
            self.loghandler = loghandler               #Logging setup if loghandler sent, otherwise, set up a console only logging system
            self.logger = logging.getLogger(__name__)
            self.logger.addHandler(loghandler)
            self.logger.propagate = False
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger = logging.getLogger(__name__)                        
            self.logger.setLevel(logging.DEBUG)
            self.logger.addHandler(logging.StreamHandler())

        self.fname = fname # the filename to open
        self.path = path
        self.meta = meta
        self.imgType = imgType
        self.imgFormat = imgFormat
        self.FileNames = [os.path.splitext(zipname)[0]] # list of all generated files
        self.proj = 'nil' # initialize to nil (then change as appropriate)
        
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()
        self.HashMap = snappy.jpy.get_type('java.util.HashMap')

        # if values might change make a local copy
        self.polarization = self.meta.polarization
        self.sattype = self.meta.sattype
        self.bitsPerSample = self.meta.bitsPerSample
        
        self.imgDir = imgDir
        self.tmpDir = tmpDir
        
        self.polar = []

        if self.imgType == 'noise' or self.imgType == 'theta':
            self.bandNames = self.imgType
        else:
            self.bandNames = self.polarization  # Assume these are in order

        if imgFormat.lower() == 'gtiff':
            self.imgExt = '.tif'
        if imgFormat.lower() == 'hfa':
            self.imgExt = '.img'
            
        #self.snapImageProcess()
            
        #TODO: Consider decoupling the class initialization from creating an image. This will 
            #provide access to the object without large amts of CPU time (or need to have open zipfile on hand)
            #1) make an Image; 2) read/open; 3) write
            
               
        self.openDataset(self.fname, self.path)

        #write out a tif of this imgType
        if imgType == 'amp' and 'Q' in self.meta.beam:  # this would be a quad pol scene...
            self.decomp(format ='GTiff') # recommend using GTiff for this first one
        else:
            self.status = self.imgWrite(format ='GTiff')
      
        self.closeDataset()
                 
        
    def openDataset(self, fname, path=''):
        """
        Opens a dataset with gdal

        **Parameters**
            
            *fname* : filename
        """
        
        gdal.AllRegister() # for all purposes
        self.inds = gdal.Open(os.path.join(path, fname), GA_ReadOnly) # open file
        self.n_cols = self.inds.RasterXSize
        self.n_rows = self.inds.RasterYSize

        #If no rows are found take from metadata
        if(self.n_rows == 0):
            self.n_rows = self.meta.n_rows

        self.n_bands = self.inds.RasterCount

    def closeDataset(self):
        self.inds = None

    def imgWrite(self, format='imgFormat', stretchVals=None):
        """
        Takes an input ds and writes an image.

        self.imgType could be 1) amp, 2) sigma, 3) noise, 4) theta

        all bands are output (amp, sigma)

        Also used to scale an integer img to byte with stretch, if stretchVals are included
        
        Note there is a parameter called chunk_size hard coded here that could be changed 
            If you are running with lots of RAM
        """

        #Use ASF Tools for ASF_CEOS data
        if self.sattype == 'ASF_CEOS':
            self.logger.error("Cannot write ASF_CEOS")
            
            return "error"
            '''
            self.ASFimgWrite()
            return
            '''

        chunkSize = 300 # 300 seems to work ok, go lower if RAM is wimpy...

        ############################################## SETUP FOR OUTPUT
        
        if format.lower() == 'gtiff':
            ext = '.tif'
            driver = gdal.GetDriverByName('GTiff')
            options = ['COMPRESS=LZW']
            if self.n_bands > 3:
                    options = ['PHOTOMETRIC=MINISBLACK'] #probs here if you LZW compress
        elif format.lower() == 'hfa':
            ext = '.img'
            driver = gdal.GetDriverByName('HFA')
            
        elif format.lower() == 'vrt':
            self.logger.error('Cannot write a vrt as an original image')
            return
        
        elif format.lower() == 'imgformat':
            ext = self.imgExt
            driver = gdal.GetDriverByName(self.imgFormat)
            if self.imgFormat.lower() == 'gtiff':
                options = ['COMPRESS=LZW']
                if self.n_bands > 3:
                    options = ['PHOTOMETRIC=MINISBLACK'] #probs here if you LZW compress
            else:
                options = ['']
        else:
            self.logger.error('That image type is not supported')
            return "error"

        n_bands, dataType, outname = self.fnameGenerate()

        if stretchVals is not None:
            outname = outname +'_temp_stretch'
            dataType = GDT_Byte # Hard coded here...

        outds = driver.Create(outname+ext, self.n_cols, self.n_rows, n_bands,
                              dataType, options) # not working with options?? , options)
        
        print outds

        ############################################## READ RAW DATA
        for band in range(1,n_bands+1):
            self.logger.info('Processing band ' + str(band))

                #+ ' of ' + str(self.n_bands) + ' bands'
            bandobj = self.inds.GetRasterBand(band)

            if stretchVals is not None:
                scaleRange = stretchVals[band-1,1]
                dynRange = stretchVals[band-1,2]
                minVal = stretchVals[band-1,3]
                offset = stretchVals[band-1,4]

                #PROCESS IN CHUNKS
            n_chunks = self.n_rows / chunkSize + 1
            n_lines = chunkSize

            for chunk in range( n_chunks ):

                first_line = chunkSize*chunk
                if chunk == n_chunks - 1:
                    n_lines = self.n_rows - first_line


                # read in a chunk of data
                datachunk = gdal_array.BandReadAsArray( bandobj, 0, first_line,
                                                       self.n_cols, n_lines )
                ### ERROR 5 handler
                ##/FutureWarning: comparison to `None` will result in an elementwise object comparison in the future.
                if datachunk is None:
                    self.logger.error("Error datachunk =  None")

                    self.tifname = outname+ext          ###
                    return "error"

                '''
                ###Flip the image around the y axis here if it is RS1
                ###if self.meta.passDirection == "Ascending" and self.meta.sattype != 'RS2':
                if self.meta.passDirection == "Descending" and self.meta.sattype != 'RS2':
                    #print "Ascending RSAT1, flipping image"
                    print "Descending RSAT1, flipping image"
                    datachunk = numpy.fliplr(datachunk)
                    #datachunk = datachunk[:,::-1]
                '''

                if stretchVals is not None:
                    outdata = self.stretchLinear(datachunk, scaleRange,
                                                 dynRange, minVal, offset)

                else:
                    # decide what to do with the datachunk
                    if self.imgType == 'amp':  # assumes no values will be zero
                        if datachunk.dtype == numpy.complex64 or \
                        datachunk.dtype == numpy.complex128:
                            outdata = self.getMag(datachunk)
                        else:
                            outdata = self.getAmp(datachunk)
                    if self.imgType == 'sigma':
                        outdata =  self.getSigma(datachunk, n_lines)
                    if self.imgType == 'theta':
                        outdata = self.getTheta(n_lines)
                    if self.imgType == 'noise':
                        outdata = self.getNoise(n_lines)
                    if self.imgType == 'phase':
                        outdata = self.getPhase(datachunk)

                # write caldata from datachunk to outds
                if self.imgType == 'amp' and stretchVals is None:
                    gdal_array.BandWriteArray( outds.GetRasterBand(band), outdata.astype(int), 0, first_line )
                elif stretchVals is not None:
                    gdal_array.BandWriteArray( outds.GetRasterBand(band), outdata.astype(int), 0, first_line )  ###BYTE? outds should be defined as byte anyhow (see above)
                else:
                    gdal_array.BandWriteArray( outds.GetRasterBand(band), outdata.astype(float), 0, first_line )

                outds.FlushCache()   # flush all write cached data to disk
                ##end chunk loop

            outBand = outds.GetRasterBand(band)
            outBand.SetNoDataValue(0)  # if warranted (if before stats, then good)
            outBand.FlushCache()
            outBand.GetStatistics(False, True)
            band = None
            ## end band loop

        # finish the geotiff file
        
        if self.proj == 'nil':
            outds.SetGCPs(self.meta.geopts, self.meta.geoptsGCS)
        else:
            # copy the proj info from before...
            outds.SetGeoTransform(self.inds.GetGeoTransform())
            outds.SetProjection(self.inds.GetProjection())
        
        if stretchVals is None:
            self.FileNames.append(outname)
            self.logger.debug('Image written ' + outname+ext)

            self.tifname = outname+ext          ###
        
        outds = None         # release the dataset so it can be closed


    def reduceImg(self, xfactor, yfactor):
        """
        Uses gdal to reduce the image by a given factor (i.e, factor 2 is 50%
        smaller or half the # of pixels) and saves as a temporary file and then overwrites.

        **Parameters**
            
            *xfactor* : float

            *yfactor* : float
        """

        #assert here for x and y factor

        #convert to percent
        xfactor = str(100.0/xfactor)
        yfactor = str(100.0/yfactor)

        imgFormat = self.imgFormat
        ext = self.imgExt

        inname = self.FileNames[-1]
        tempname = 'tmp_reduce'


        cmd = 'gdal_translate -outsize ' + xfactor +'% ' + yfactor +'%  -of '+ imgFormat +' ' +\
            inname+ext + ' ' + tempname+ext

        command = shlex.split(cmd)

        ok = subprocess.Popen(command).wait()

        if ok == 0:
            os.remove(inname+ext)
            os.rename(tempname+ext, inname+ext)
            self.logger.debug('img reduced in size')

            #self.FileNames.append(outname)
        else:
            self.logger.error('Could not reduce image')
        return ok

    def projectImg(self, projout, projdir, format=None, resample='bilinear', clobber=True):
        """
        Looks for a file, already created and projects it to a vrt file.

        **Parameters**
            
            *projout*  : projection base name

            *projdir*  : path to the projection

            *format*   : the image format, defaults to VRT

            *resample* : resample method (as per gdalwarp)

            *clobber*  : True/False should old output be overwritten?

        NOTE THE PIXEL SIZE IS NOT PROSCRIBED! (it will be the smallest possible)
        """
        if format == None:
            imgFormat = 'VRT'
            ext = '.vrt'
        else:
            imgFormat = self.imgFormat
            ext = self.imgExt
        if clobber:
            clobber=' -overwrite '
        else: 
            clobber= None

        #Use ASF Tools for ASF_CEOS data
        if self.sattype == 'ASF_CEOS':
        
            self.logger.error("cannot project afs_ceos")
            return

            '''
            self.ASFprojectImg(self.imgType, projin, projout, projdir)
            return
            '''
        os.chdir(self.imgDir)
        inname = self.FileNames[-1] #last file
        print inname
        outname = self.fnameGenerate(projout=projout)[2]
        command = 'gdalwarp -of ' + imgFormat +  ' -t_srs ' +\
                os.path.join(projdir, projout+'.wkt') + \
                    ' -order 3 -dstnodata 0 -r '+ resample +' '+clobber+ \
                    inname+'.tif' + ' ' + outname+ext
        try:
            ok = subprocess.Popen(command).wait()  # run the other way on linux
        except:
            cmd = shlex.split(command)  #TODO this may be a problem for windows
            ok = subprocess.Popen(cmd).wait()


        #os.remove(inname+'.tif') #would remove the original file
        if ok == 0:
            self.proj = projout
            self.projdir = projdir
            self.logger.info('Completed image projection')
            
            self.FileNames.append(outname)
        else:
            self.logger.error('Image projection failed')
        return ok


    def fnameGenerate(self, projout=None, subset=None, band=None):
        """
        Decide on some parameters based on self.imgType we want...
        
        **Returns**
            
            *bands*    : Integer

            *dataType* : GDal data type

            *outname*  : String
        """

        if self.imgType == "amp":
            bands = self.n_bands
            if self.bitsPerSample == 8:
                dataType = GDT_Byte
            else:
                dataType = GDT_UInt16
            self.bandNames = None
        if self.imgType == "phase":
            bands = self.n_bands
            dataType = GDT_Float32
            self.bandNames = None
        if self.imgType == "sigma":
            bands = self.n_bands
            dataType = GDT_Float32
            self.bandNames = None
        if self.imgType == "noise":
            bands = 1
            dataType = GDT_Float32
            self.bandNames = ['noise']
        if self.imgType == "theta":
            bands = 1
            self.bandNames = ['theta']
            dataType = GDT_Float32

        if self.bandNames == None:
            self.bandNames = []
            names = self.polarization.split()
            for name in names:
                self.bandNames.append(self.imgType[0].lower()+name)

        sep = '_'
        if projout==None:
            proj = self.proj
        else:
            proj = projout
        if subset != None:
            subset = sep+subset
        else:
            subset = ''

        outname = str(self.meta.dimgname+sep+self.imgType[0:1].lower()+\
                sep+proj+subset)

        return bands, dataType, outname


    def cropImg(self, ullr, subscene):
        """
        Given the cropping coordinates, this function tries to crop in a straight-forward way.
        If this cannot be accomplished (likely because the corner coordinates of an image are not known to a sufficient precision)
        then gdalwarp (cropBig) will do the job.

        **Parameters**
            
            *ullr*     : upper left and lower right coordinates

            *subscene* : the name of a subscene
        """

        if ullr == 0:
            return -1
        ok = self.cropSmall(ullr, subscene)
        if ok != 0:
            llur = Util.ullr2llur(ullr) 
            ok = self.cropBig(llur, subscene)

        return ok


    def cropBig(self, llur, subscene):
        """
        Here we have a way to crop that will expand the area of an image.
        However, this uses gdalwarp - and resampling/offsetting could skew result - by a fraction of a pixel obviously, but still..

        **Parameters**
            
            *llur*     : list/tuple of tuples in projected units

            *subscene* : the name of a subscene
        """

        imgFormat = 'vrt'
        ext = '.vrt'
        inname = self.FileNames[-1] # this is potentially an issue here
        outname = inname+'_'+subscene
        tempname = 'tmp_'+outname
        
        sep = ' '
        crop = str(llur[0][0]) +sep+ str(llur[0][1]) +sep+ str(llur[1][0]) +sep+ str(llur[1][1])
        cmd = 'gdalwarp -of ' + imgFormat + ' -te ' + crop + ' -t_srs ' +\
                os.path.join(self.projdir, self.proj+'.wkt') +\
            ' -r near -order 1 -dstnodata 0 ' +\
            inname+ext + ' ' + tempname+ext

        command = shlex.split(cmd)

        ok = subprocess.Popen(command).wait()
        if ok == 0:
            os.rename(tempname+ext, outname+ext)
            self.logger.debug('img cropped -method warp') 
            self.FileNames.append(outname)
        else:
            self.logger.error('Could not crop image in cropBig')
        return ok

    def cropSmall(self, urll, subscene):
        """
        This is a better way to crop b/c no potential for warping...
        However, this will only work if the region falls completely within the image.

        **Parameters**
            
            *urll*     : list/tuple of tuples in projected units

            *subscene* : the name of a subscene
        """

        imgFormat = 'vrt'
        ext = '.vrt'
        inname = self.FileNames[-1] # this is potentially an issue here
        self.fname_nosubest = inname
        outname = inname+'_'+subscene
        tempname = 'tmp_'+outname
       
        sep = ' '
        crop = str(urll[0][0]) +sep+ str(urll[0][1]) +sep+ str(urll[1][0]) +sep+ str(urll[1][1])
        cmd = 'gdal_translate -projwin ' + crop + ' -a_nodata 0 -of '+ imgFormat +' ' +\
            inname+ext + ' ' + tempname+ext

        command = shlex.split(cmd)

        ok = subprocess.Popen(command).wait()
        if ok == 0:
            os.rename(tempname+ext, outname+ext)
            self.logger.debug('img cropped -method crop_Small')
            self.FileNames.append(outname)
        else:
            self.logger.error('Could not crop image in crop_Small')
        return ok

    def maskImg(self, mask, vectdir, side, imgType):
        """
        Masks all bands with gdal_rasterize using the 'layer'

        side = 'inside' burns 0 inside the vector, 'outside' burns outside the vector

        Note: make sure that the vector shapefile is in the same proj as img (Use reprojSHP from ingestutil)

        **Parameters**
            
            *mask*    : a shapefile used to mask the image(s) in question

            *vectdir* : directory where the mask shapefile is

            *side*    : 'inside' or 'outside' depending on desired mask result

            *imgType* : the image type
        """

        inname = self.FileNames[-1] # this is potentially an issue here

        if side.lower() == 'inside':
            sidecode = ''
        elif side.lower() == 'outside':
            sidecode = '-i'
        else:
            self.logger.error('Mask inside or outside your polygons')
            return

        bandFlag = ' -b '
        bstr = ''

        # must list the bands to mask if more than 1 band
        if imgType == 'sigma' or imgType == 'amp':
            for band in range(1,self.n_bands+1):
                bstr = bstr + bandFlag + str(band)
                

        cmd = 'gdal_rasterize ' + sidecode + bstr +\
            ' -burn 0' + ' -l ' +\
            mask + ' ' + os.path.join(vectdir, mask  + '.shp') +\
            ' ' + inname+self.imgExt

        #command = shlex.split(cmd)

        #ok = subprocess.Popen(command).wait()

        ok = os.system(cmd)

        if ok == 0:
            self.logger.info('Completed image mask')
        else:
            self.logger.error('Image masking failed')

    def makePyramids(self):
        """
        Uses gdaladdo to make pyramids aux style
        """

        inname = self.FileNames[-1]

        cmd = 'gdaladdo -r gauss -ro --config COMPRESS_OVERVIEW DEFLATE ' +\
            ' --config USE_RRD YES ' +\
            inname+self.imgExt +' 2 4 8 16 32 64'

        command = shlex.split(cmd)

        ok = subprocess.Popen(command).wait()
        if ok == 0:
            self.logger.info('Completed image pyramids')

        else:
            self.logger.error('Image pyramid scheme collapsed')

    def vrt2RealImg(self, subset=None):
        """
        When it is time to convert a vrt to a tiff (or even img, etc) use this
        """

        inname = self.FileNames[-1]
        outname = self.fnameGenerate(subset=subset)[2]

        cmd = 'gdal_translate -of '+ self.imgFormat +' -co \"COMPRESS=LZW\" -a_nodata 0 ' +\
            inname+'.vrt' +' '+ outname+self.imgExt

        command = shlex.split(cmd)
        try:
		ok = subprocess.Popen(command).wait()
        except:
            self.logger.error("vrt2RealImg failed")
		
        if ok == 0:
            outname = self.fnameGenerate(subset=subset)[2]       ###  Why the name change here? Could be crutial?
            self.FileNames.append(outname)          ###
            self.logger.debug('Completed export to tiff ' + outname+'.tif')
        else:
            self.logger.error('Image export failed')

    def getImgStats(self):
        """
        Opens a raster and calculates (approx) the stats
        returns an array - 1 row per band
        cols: band, dynamicRange, dataType, nodata value, min, max, mean, std
        
        **Returns**
            
            *stats* : stats from raster returned in an array of 1 row per band
        """

        ext=self.imgExt
        inname = self.FileNames[-1]
        imgfile = inname+ext  

        self.logger.info("Getting image stats for " + imgfile)

        self.openDataset(imgfile)

        # find out what size the image is now and replace self.n_cols, rows
        n_bands = self.n_bands

        #array to hold: band, range, dtype, nodata, min,max,mean,std
        stats = numpy.zeros((n_bands, 8))

        for band in range(1,n_bands+1):
            bandobj = self.inds.GetRasterBand(band)
            bandobj.FlushCache()
            band_stats = bandobj.GetStatistics(False,True)
            dynRange = band_stats[1]-band_stats[0]
            band_dtype = bandobj.DataType
            nodata = bandobj.GetNoDataValue()
            band_stats[:0] = [band, dynRange, band_dtype, nodata]
            stats[band-1,:] = band_stats

            ###self.inds.band.GetHistogram(min, max, n_buckets) # works!

        bandobj = None
        self.closeDataset()
        return stats

    def applyStretch(self, stats, procedure='std', sd=3, bitDepth=8, sep='tog'):
        """
        Given stats... will stretch a multiband image to the dataType based on
        procedure (either sd for standard deviation, with +ve int in keyword sd,
        or min-max, also a linear stretch).

        !!A nodata value of 0 is used in all cases!!

        !!For now, dataType is byte and that's it!!

        **Note:** gdal_translate -scale does not honour nodata values
        See: http://trac.osgeo.org/gdal/ticket/3085

        Have to run this one under the imgWrite code. The raster bands must be integer, float? or byte
        and int data assumed to be only positive. Won't work very well for dB scaled data (obviously)
        it is important that noData is set to 0 and is meaningful.

        sep = separate: applies individual stretches to each band (=better visualization/contrast)

        tog = together: applies the same stretch to all bands (looks for the band with the greatest dynamic range) (=more 'correct')

        For further ideas see: http://en.wikipedia.org/wiki/Histogram_equalization
        """

        if procedure.lower() == 'std':
            assert sd > 0 and (type(sd) == type(1) or type(sd) == type(1.0))
        if stats[0, 2] not in [1,2,6]:
            self.logger.error('Image scaling from this image\'s data type are not supported')
            return
        
        if stats[0, 2] == 1 and bitDepth == 16:
            self.logger.error('No sense scaling an 8 bit image to 16 bits... not going to do it')
                
            bitDepth = 8
        scaleRange = (2**bitDepth)-2  #save 1 position for nodata and one for 0...

        # create an array with band number, scaleRange, dynRange, minVal, maxVal
        stretchVals = numpy.zeros((self.n_bands, 5))
        for band in range(1,len(stats[:,0])+1):
            #go through each band to make the new array
            index = band-1
            if procedure.lower() == 'min-max':
                minVal = stats[index, 4]
                maxVal = stats[index, 5]
                dynRange = stats[index, 1]

            elif procedure.lower() == 'std':
                #determine -scale src (min max) dst (1 max)
                mean = stats[index,6]
                std = stats[index,7]
                minVal = mean-(std*sd)
                maxVal = mean+(std*sd)
                # use valid data ONLY for stretch in case of a skewed hist
                # this will CHANGE THE SHAPE of the histogram - spreading it out!
                # However, contrast will be improved because all pixel values will be used
                if stats[0,2] in [1,2]: #byte and int
                    if stats[0,2] == 1:
                        limitMax, limitMin = 2**8, 1e-9
                    elif stats[0,2] == 2:
                        limitMax, limitMin = 2**16, 1e-9
                    minVal = max(minVal, limitMin)
                    maxVal = min(maxVal, limitMax)
                    dynRange = maxVal-minVal
                if stats[0,2] == 6: # float data
                    if stats[0,5] <= 0: #assumes valid data are below zero!!
                        limitMax = 1e-9
                        maxVal = min(maxVal, limitMax)
                    if stats[0,4] >= 0: #assumes valid data are above zero!!
                        limitMin = 1e-9
                        minVal = max(minVal, limitMin)
                    dynRange = maxVal-minVal

            else:
                self.logger.error('Stretch procedure ' + procedure + ' not supported')
                return 'error'

            stretchVals[index,:] = [band, scaleRange, dynRange, minVal, maxVal]

        if sep.lower()[0:3] == 'tog' and stretchVals.shape[1] > 1:
            #get the greatest dynamic range (as assigned above)
            index = numpy.argmax(stretchVals[:,2]) #takes the 1st band if tied

            # find the most extreme value outside the dynRange of the master
            # 'master band'. Other bands need to be brought closer to the middle
            # of the master band or they will be truncated.  All bands will shift
            # closer to the 'master' middle in proportion - prior to rescale
            mins = stretchVals[index,3]-stretchVals[:,3] #+ve is more extreme
            maxs = stretchVals[:,4]-stretchVals[index,4] #+ve is more extreme
            greatestShift = numpy.append(mins, maxs).max()
            if greatestShift != 0:
                self.logger.debug('Using shift technique to avoid stretch bias')
                    
                # is met, debug to make sure the following works
                middles = stretchVals[:,3]+(stretchVals[:, 2]/2)
                masterMiddle = middles[index, ]
                middleDiffs = masterMiddle - middles
                greatestMiddleDiff = numpy.max(numpy.absolute(middleDiffs))
                offsets = middleDiffs
                for i, offset in enumerate(offsets):
                    if offset != 0:
                        offsets[i] = (offset*greatestShift)/greatestMiddleDiff
            else:
                offsets = 0
            # rebuild stretchVals with offsets
            stretchVals[:,1:4] = stretchVals[index,1:4]
            stretchVals[:,4] = offsets

        elif sep.lower()[0:3] == 'sep':
            # rebuild stretchVals with offset
            stretchVals[:,4] = 0

        else:
            self.logger.error('Error: Do you want bands stretched separately or together?')
            return

        filename = self.FileNames[-1]
        #filename = self.fnameGenerate(subset=subset)[2]
        ext = self.imgExt
        self.openDataset(filename+ext)

        #write out a tif of this imgType
        self.imgWrite(stretchVals=stretchVals)
        self.closeDataset()

        # delete original file and rename tmp
        if self.fname_nosubest != 0:
            filename = self.fname_nosubest
        try:
            os.rename(filename+'_temp_stretch'+ext, filename+ext)
            os.remove(filename+ext)
            #os.rename('temp_stretch'+ext, 'temp_stretch'+'_'+procedure+str(sd)+sep+ext)
        except:
            #os.remove(filename+ext)
            #os.rename(filename+'_temp_stretch'+ext, filename+ext)
            #os.rename('temp_stretch'+ext, 'temp_stretch'+'_'+procedure+str(sd)+sep+ext)
            pass
        self.logger.info('Image stretched... ')
            
    def stretchLinear(self, datachunk, scaleRange, dynRange, minVal, offset=0):
        """
        Simple linear rescale: where min (max) can be the actual min/max or mean+/- n*std or any other cutoff

        Note: make sure min/max don't exceed the natural limits of dataType
        takes a numpy array datachunk the range to scale to, the range to scale
        from, the minVal to start from and an offset required for some stretches
        (see applyStretch keyword sep/tog)
        
        **Parameters**
            
            *datachunk*   : array

            *scaleRange*  : Range to scale to

            *dynRange*    : Range to scale from

            *minVal*      : strating min value
        
        **Returns**

            *stretchData* : datachunk, now linearly rescaled
        """

        noDataVal = 0  # Hard coded here for now

        datachunk = numpy.ma.masked_equal(datachunk, noDataVal)
        datachunk = datachunk*1.0 #convert to float

        datachunk = datachunk+offset # this adjusts the middles relative to each other
        stretchData = scaleRange * (datachunk - minVal) / dynRange

        stretchData = numpy.rint(stretchData) # turn into nearest integer
        stretchData = numpy.clip(stretchData, 0, scaleRange)+1
        #change stretchData to a regular array filled with nodata = 0
        stretchData = numpy.ma.filled(stretchData, noDataVal)

        return stretchData


    def getBandData(self, band):
        """
        opens an img file and reads in data from a given band
        assume that the dataset is small enough to fit into memory all at once
        
        **Returns**
            
            *imgData*  : data from the given band

            *xSpacing* :

            *ySpacing* :
        """

        gdal.AllRegister() # for all purposes
        inname = self.FileNames[-1]
        fname = inname+self.imgExt
        ds = gdal.Open(fname, GA_ReadOnly)
        n_cols = ds.RasterXSize
        n_lines = ds.RasterYSize
        bandobj = ds.GetRasterBand(band)
        # read in all data (better not be too big for memory!)
        imgData = gdal_array.BandReadAsArray(bandobj, 0, 0, n_cols, n_lines )

        geotransform = ds.GetGeoTransform()
        xSpacing = geotransform[1]
        ySpacing = geotransform[5]

        bandobj = None
        ds = None
        return imgData, xSpacing, ySpacing

    def cleanFiles(self, levels=['crop']):
        """
        Removes files that have been written.

        Input a list of items to delete: raw, nil, proj,crop
        
        **Parameters**
            
            *levels* : a list of different types of files to delete
        """
        
        ext = ['.zip', self.imgExt, '.vrt', self.imgExt]
        deleteMe = []
        if 'crop' in levels:
            deleteMe = glob.glob(self.FileNames[2]+'_*'+ext[2]) #The wildcard is the subscene name
        if 'proj' in levels:
            deleteMe = deleteMe+[self.FileNames[2]+ext[2]]
        if 'nil' in levels:
            deleteMe = deleteMe+[self.FileNames[1]+ext[1]]
        if 'raw' in levels:
            deleteMe = deleteMe+[self.FileNames[0]+ext[0]]

        for filename in deleteMe:
            if filename != []:
                try:
                    os.remove( filename )
                except:
                    self.logger.error('File not deleted: ' + filename)

    def getSigma(self, datachunk, n_lines):
        """
        Calibrate data to Sigma Nought values (linear scale)
        """

        # create an array the size of datachunk that we'll fill with calibrated data
        caldata = numpy.zeros((n_lines, self.n_cols), dtype=numpy.float32)

        ## note data are calibrated differently if they are slc or detected
        if datachunk.dtype == numpy.complex64 or datachunk.dtype == numpy.complex128:
            #convert to detected image
            datachunk = pow(numpy.real(datachunk), 2) + pow(numpy.imag(datachunk), 2)
            gains = self.meta.calgain**2

        else:       # magnitude detected data
            datachunk = numpy.float32(datachunk)**2 # convert to float, prevent integer overflow
            datachunk = datachunk - self.meta.caloffset
            gains = self.meta.calgain

        for i, gain in enumerate(gains):
                caldata[:, i] = datachunk[:, i] / gain

        return caldata


    def getTheta(self, n_lines):
        """
        For making an image with the incidence angle as data
        """
        
        outdata = numpy.zeros((n_lines, self.n_cols), dtype=numpy.float32)
        for i in range(self.meta.n_cols):
            outdata[:,i] = self.meta.theta[i]
        return outdata

    def getNoise(self, n_lines):
        """
        For making an image with the noise floor as data
        """
        
        outdata = numpy.zeros((n_lines, self.n_cols), dtype=numpy.float32)
        for i in range(self.n_cols):
            outdata[:,i] = self.meta.noise[i]
        return outdata

    def getMag(self, datachunk):
        """
        return the magnitude of the complex number
        """
        
        outData = pow((pow(numpy.real(datachunk), 2) + pow(numpy.imag(datachunk), 2)), 0.5)
        return numpy.ceil(outData)

    def getAmp(self, datachunk):
        """
        return the amplitude, given the amplitude... but make room for
        the nodata value by clipping the highest value...
        """
        
        clipMax = (2**self.bitsPerSample)-2
        outdata = numpy.clip(datachunk, 0, clipMax)
        return outdata+1

    def getPhase(self, datachunk):
        """
        Return the phase (in radians) of the data (must be complex/SLC)
        """
        
        return numpy.angle(datachunk)

    def decomp(self, format='imgFormat'):
        """
        Takes an input ds of a fully polarimetric image and writes an image of
        the data using a decomposition - could be 1) pauli
        
        TODO: 2) freeman 3) cloude

        Differs from imgWrite b/c it ingests all bands at once...
        """

        #Quick check to see if image supported
        if self.sattype != 'RSAT2' and 'Q' not in self.meta.beam:
            self.logger.error('Image cannot be decomposed')
            return  "error"

        chunkSize = 300 # seems to work ok, go lower if RAM is wimpy...

        ############################################## SETUP FOR OUTPUT
        #
        if format.lower() == 'gtiff':
            ext = '.tif'
            driver = gdal.GetDriverByName('GTiff')
            options = ['COMPRESS=LZW']
        elif format.lower() == 'hfa':
            ext = '.img'
            driver = gdal.GetDriverByName('HFA')
        elif format.lower() == 'vrt':
            self.logger.error('Cannot write a vrt as an original image')
            return  "error"
        elif format.lower() == 'imgformat':
            ext = self.imgExt
            driver = gdal.GetDriverByName(self.imgFormat)
            if self.imgFormat.lower() == 'gtiff':
                options = ['COMPRESS=LZW']
            else:
                options = ['']
        else:
            self.logger.error('That image type is not supported')
            return "error"

        outname = self.fnameGenerate()[2]

        n_bands = 3

        dataType = GDT_Float32  # use Float to avoid truncation
        outds = driver.Create(outname+ext, self.n_cols, self.n_rows, n_bands,
                              dataType, options)

        ############################################## READ RAW DATA
        hh = self.inds.GetRasterBand(1)
        vv = self.inds.GetRasterBand(2)
        hv = self.inds.GetRasterBand(3)
        vh = self.inds.GetRasterBand(4)

        #PROCESS IN CHUNKS
        n_chunks = self.n_rows / chunkSize + 1
        n_lines = chunkSize

        for chunk in range( n_chunks ):

            first_line = chunkSize*chunk
            if chunk == n_chunks - 1:
                n_lines = self.n_rows - first_line

            # read in a chunk of data
            datachunk_hh = gdal_array.BandReadAsArray(hh, 0, first_line,
                                                   self.n_cols, n_lines )
            datachunk_vv = gdal_array.BandReadAsArray(vv, 0, first_line,
                                                   self.n_cols, n_lines )
            datachunk_hv = gdal_array.BandReadAsArray(hv, 0, first_line,
                                                   self.n_cols, n_lines )
            datachunk_vh = gdal_array.BandReadAsArray(vh, 0, first_line,
                                                   self.n_cols, n_lines )

            # The following is for Pauli decomp - Add more when you can...

            # these data are not calibrated - i.e., the LUT is applied
            #pauli1 =  numpy.abs(datachunk_hh - datachunk_vv)
            #pauli2 =  numpy.abs(datachunk_hv + datachunk_vh)/2.0  #Paris had this formula
            #pauli2 =  (numpy.abs(datachunk_hv) + numpy.abs(datachunk_vh) )/2.0 # IAPRO does this (pretty similar to above
            #pauli3 =  numpy.abs(datachunk_hh + datachunk_vv)


            #red = (shh + svv)/sqrt(2), green = sqrt(2)*Shv, blue = (Shh-Svv)/sqrt(2)
            #these are complex quantities single-bounce, volume scat. double-bounce
            pauli1 =  numpy.abs( (datachunk_hh - datachunk_vv) / math.sqrt(2) )
            pauli2 =  numpy.abs( ((datachunk_hv + datachunk_vh)/2.0)* math.sqrt(2) )
            pauli3 =  numpy.abs(  (datachunk_hh + datachunk_vv) / math.sqrt(2) )


            datachunk_hh, datachunk_vv, datachunk_hv, datachunk_vh = 0, 0, 0, 0 #free memory

            # write computed bands from datachunk to outds
            gdal_array.BandWriteArray( outds.GetRasterBand(1), pauli1.astype(float), 0, first_line )
            gdal_array.BandWriteArray( outds.GetRasterBand(2), pauli2.astype(float), 0, first_line )
            gdal_array.BandWriteArray( outds.GetRasterBand(3), pauli3.astype(float), 0, first_line )
            # since the data are not calibrated, try output as int.  Watch for truncation... pauli.astype(float)

            outds.FlushCache()   # flush all write cached data to disk
            ##end chunk loop

        for i in range(1, 4):
            outds.GetRasterBand(i).SetNoDataValue(0)  # if warranted (if before stats, then good)
            outds.GetRasterBand(i).FlushCache()
            outds.GetRasterBand(i).GetStatistics(False, True)

        # finish the geotiff file
        if self.proj == 'nil':
            outds.SetGCPs(self.meta.geopts, self.meta.geoptsGCS)
        else:
            # copy the proj info from before...
            outds.SetGeoTransform(self.inds.GetGeoTransform())
            outds.SetProjection(self.inds.GetProjection())

        self.FileNames.append(outname)
        self.logger.debug('Image written ' + outname+ext)

        outds = None         # release the dataset so it can be close
        
    def removeHandler(self):
        self.logger.handlers = []
    
    def cleanFileNames(self):
        temp = [0,0]
        temp[0] = self.FileNames[0]
        temp[1] = self.FileNames[1]
        self.FileNames = temp
        
    def combineTif(self, imgdir, zipname, mergedir):
        """
        Takes a set of tif images made in scientific mode and combines
        them into a single tif image
        
         **Parameters**
            
            *imgdir*   : directory containing images to combine
            
            *zipname   : name of zipfile (no path)
            
            *mergedir* : directory containing gdal_merge.py
        """
        
        os.chdir(mergedir)
        
        import gdal_merge as gm
        
        os.chdir(imgdir)
        
        outname = zipname+"_scenes.tif"
        
        imgs = ['-o', outname, '-of', 'GTiff']        
        
        for files in os.listdir(imgdir):
            if files.endswith(".tif") and self.meta.dimgname in files:
                if "nil" in files:
                    continue
                else:
                    imgs.append(files)
                
        sys.argv[1:] = imgs
        gm.main()
       
    def snapImageProcess(self):
        """
        This fuction does the basic image processing for radarsat scenes using
        the Snappy library. Each band is subject to a calibration step, a
        speckle filter step, then finally a terrain correction step. All the
        processed bands are then merged together to create the final product.
        
        NOTE: Only works on Windows, Linux is a work in progress!!!!
        """
        
        os.chdir(self.tmpDir)
        
        sys.path.append('C:\Users\cfitzpatrick\.snap\snap-python')
        
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()
        HashMap = snappy.jpy.get_type('java.util.HashMap')
        
        self.openDataset(self.fname, self.path)
        
        gc.enable()
        
        bands, dataType, outname = self.fnameGenerate()         
        
        i = 0
        while i < len(self.polarization.split()):
            self.polar.append(self.polarization.split(' ')[i])
            i+= 1
                
        for p in self.polar:
            #Part 1: Calibration
            input = os.path.join(self.path, self.fname)

            rsat = ProductIO.readProduct(input)
            
            output = os.path.join(self.tmpDir, outname  + p + '_cal')
            
            parameters = HashMap()
            parameters.put('sourceBands', 'Intensity_' + p) 
            parameters.put('selectedPolarisations', p)
            
            target = GPF.createProduct("Calibration", parameters, rsat)
            
            ProductIO.writeProduct(target, output, 'BEAM-DIMAP')
            
            #Part 2: Speckle Filter
            input = output + '.dim'
            
            print input
            
            output = os.path.join(self.tmpDir, outname + '_spec')
            
            rsat2 = ProductIO.readProduct(input)
            
            parameters = HashMap()
            
            parameters.put('sourceBands', 'Sigma0_' + p)
            parameters.put('filter', 'Refined Lee')
            #parameters.put('filterSizeX', 5)
            #parameters.put('filterSizeY', 5)
            
            target = GPF.createProduct("Speckle-Filter", parameters, rsat2)
            
            ProductIO.writeProduct(target, output, 'BEAM-DIMAP')
            
            #Part 3: Terrain Correction
            input = output + '.dim'
            
            output = os.path.join(self.tmpDir, 'final_' + p)
            
            rsat3 = ProductIO.readProduct(input)
            
            parameters = HashMap()
            
            parameters.put('sourceBands', 'Sigma0_' + p)
            parameters.put('demName', 'GETASSE30')
            
            target = GPF.createProduct("Terrain-Correction", parameters, rsat3)
            
            ProductIO.writeProduct(target, output, 'BEAM-DIMAP')
                        
        self.closeDataset()        
 
        #Part 4: Merge Bands        
        product_set = []
        
        for p in self.polar:            
            input = os.path.join(self.tmpDir, 'final_' + p + '.dim')
            
            product_set.append(ProductIO.readProduct(input))
                
        parameters = self.HashMap()
        
        parameters.put('resamplingType', None)

        
        target = GPF.createProduct('CreateStack', parameters, product_set)
        
        output = os.path.join(self.imgDir, outname + '_final')
        
        ProductIO.writeProduct(target, output, 'GeoTIFF')
        
        self.FileNames.append(output)

       