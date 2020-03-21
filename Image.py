"""
**imgProcess.py**

**Created on** 14 Jul  9:22:16 2009 **@author:** Derek Mueller

This module creates an instance of class Image. It creates an image to 
contain Remote Sensing Data as an amplitude, sigma naught, noise or theta
(incidence angle) image, etc. This image can be subsequently projected,
cropped, masked, stretched, etc.

**Modified on** 3 Feb  1:52:10 2012 **@reason:** Repackaged for r2convert **@author:** Derek Mueller
**Modified on** 23 May 14:43:40 2018 **@reason:** Added logging functionality **@author:** Cameron Fitzpatrick
**Modified on** 9 Aug  11:53:42 2019 **@reason:** Added in/replaced functions in this module with snapPy equivilants **@author:** Cameron Fitzpatrick

**Common Parameters of this Module:**

*zipfile* : a valid zipfile name with full path and extension

*zipname* : zipfile name without path or extension

*fname* : image filename with extention but no path

*imgname* : image filename without extention

*granule* : unique name of an image in string format

*path* : path to the image in string format
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
import re
try:
    import snappy
    from snappy import ProductIO
    from snappy import GPF
    from snappy import WKTReader
    from snappy import GeoPos
except:
    pass
        
import gc

from osgeo import gdal
from osgeo.gdalconst import *
from osgeo import gdal_array
from osgeo import osr

import Util

class Image(object):
    """
    This is the Img class for each image.  RSAT2, RSAT1 (CDPF)
    
    Opens the file specified by fname, passes reference to the metadata class and declares the imgType of interest.

        **Parameters**
            
            *fname*     

            *path*   

            *meta*      : reference to the meta class

            *imgType*   : amp, sigma, beta or gamma

            *imgFormat* : gdal format code gtiff, vrt

            *zipname*  
    """

    def __init__(self, fname, path, meta, imgType, imgFormat, zipname, imgDir, tmpDir, loghandler = None, pol = False):

#TODO - consider a secondary function to create the image so the class can be initialized without CPU time... 

        #assert imgType in ['amp','sigma','noise','theta']
        assert imgFormat.lower() in ['gtiff','hfa','envi','vrt']

        self.status = "ok"  ### For testing
        self.tifname = ""   ### For testing
        self.fname_nosubest = None
        
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
        self.tmpFiles = []
        self.proj = 'nil' # initialize to nil (then change as appropriate)
        
        try:
            GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()
            self.HashMap = snappy.jpy.get_type('java.util.HashMap')
            self.SubsetOp = snappy.jpy.get_type('org.esa.snap.core.gpf.common.SubsetOp')
            self.ReprojectOp = snappy.jpy.get_type('org.esa.snap.core.gpf.common.reproject.ReprojectionOp')
            self.BandMathsOp = snappy.jpy.get_type('org.esa.snap.core.gpf.common.BandMathsOp$BandDescriptor')
            gc.enable()
        except:
            pass

        # if values might change make a local copy
        self.polarization = self.meta.polarization
        self.sattype = self.meta.sattype
        self.bitsPerSample = self.meta.bitsPerSample
        
        self.imgDir = imgDir
        self.tmpDir = tmpDir
        self.zipname = zipname
        

        if self.imgType == 'noise' or self.imgType == 'theta':
            self.bandNames = self.imgType
        else:
            self.bandNames = self.polarization  # Assume these are in order

        if imgFormat.lower() == 'gtiff':
            self.imgExt = '.tif'
        if imgFormat.lower() == 'hfa':
            self.imgExt = '.img'
        
        if pol: 
            self.snapCalibration(saveInComplex=True)
        else:                          
            self.openDataset(self.fname, self.path)
            '''
            #write out a tif of this imgType
            try:
                if self.imgType == 'amp' and 'Q' in self.meta.beam:  # this would be a quad pol scene...
                    #self.decomp(format ='GTiff') # recommend using GTiff for this first one
                    self.decomposition_generation('Pauli Decomposition', amp = True)
                        
                elif self.imgType == 'sigma':
                    self.snapCalibration()
                    
                elif self.imgType == 'beta':
                    self.snapCalibration(outDataType='beta')
                    
                elif self.imgType == 'gamma':
                    self.snapCalibration(outDataType='gamma')
                
                elif self.imgType == 'amp' and self.meta.productType == 'SLC':
                    self.makeAmp()
                    
                elif self.imgType == 'amp':
                    pass
                
                else:
                    self.logger.error("No legal image processing mode chosen, returning unprocessed!")
                    self.status = 'error'
            except:
                self.status='error'
            '''
            if self.imgType == 'amp' and 'Q' in self.meta.beam:  # this would be a quad pol scene...
                self.decomp(format='GTiff')
            else:
                self.status = self.imgWrite(format='GTiff')
            
            self.inds = None                 
        
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
        self.n_bands = self.inds.RasterCount

        #If no rows are found take from metadata
        if(self.n_rows == 0):
            self.n_rows = self.meta.n_rows
            self.n_cols = self.meta.n_cols
            self.n_bands = self.meta.n_bands


    def imgWrite(self, format='imgFormat', stretchVals=None):
        """
        Takes an input dataset and writes an image.

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
        
        
        ############################################## READ RAW DATA
        for band in range(1,n_bands+1):
            self.logger.info('Processing band ' + str(band))

            bandobj = self.inds.GetRasterBand(band)

            if stretchVals is not None:
                scaleRange = stretchVals[band-1,1]
                dynRange = stretchVals[band-1,2]
                minVal = stretchVals[band-1,3]
                offset = stretchVals[band-1,4]

                #PROCESS IN CHUNKS
            n_chunks = self.n_rows / chunkSize + 1
            n_lines = chunkSize

            for chunk in range(n_chunks):

                first_line = chunkSize*chunk
                if chunk == n_chunks - 1:
                    n_lines = self.n_rows - first_line

                # read in a chunk of data
                datachunk = gdal_array.BandReadAsArray(bandobj, 0, first_line,            
                                                       self.n_cols, n_lines)

                if datachunk is None:
                    self.logger.error("Error datachunk =  None")

                    self.tifname = outname+ext          ###
                    return "error"

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
            self.FileNames.append(outname+ext)
            self.logger.debug('Image written ' + outname+ext)

            self.tifname = outname+ext          ###
        
        outds = None         # release the dataset so it can be closed


    def reduceImg(self, xfactor, yfactor):
        """
        Uses gdal to reduce the image by a given factor in x and y(i.e, factor 2 is 50%
        smaller or half the # of pixels). It overwrites the original.

        **Parameters**
            
            *xfactor* : float

            *yfactor* : float
        """

        #convert to percent
        xfactor = str(100.0/xfactor)
        yfactor = str(100.0/yfactor)

        imgFormat = self.imgFormat
        ext = self.imgExt

        inname = self.FileNames[-1]
        tempname = 'tmp_reduce' + ext


        cmd = 'gdal_translate -outsize ' + xfactor +'% ' + yfactor +'%  -of '+ imgFormat +' ' +\
            inname + ' ' + tempname

        command = shlex.split(cmd)

        ok = subprocess.Popen(command).wait()

        if ok == 0:
            os.remove(inname+ext)
            os.rename(tempname+ext, inname+ext)
            self.logger.debug('img reduced in size')

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

        **Note** The pixel IS NOT prescribed (it will be the smallest possible)
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

        os.chdir(self.imgDir)
        inname = self.FileNames[-1] #last file
        
        outname = os.path.splitext(inname)[0] + '_proj' + ext

        command = 'gdalwarp -of ' + imgFormat +  ' -t_srs ' +\
                os.path.join(projdir, projout+'.wkt') + \
                    ' -order 3 -dstnodata 0 -r ' + resample +' '+clobber+ \
                    inname + ' ' + outname
        try:
            ok = subprocess.Popen(command).wait()  # run the other way on linux
        except:
            cmd = shlex.split(command)  #TODO this may be a problem for windows
            ok = subprocess.Popen(cmd).wait()

        if ok == 0:
            self.proj = projout
            self.projdir = projdir
            self.logger.info('Completed image projection')
            
            self.FileNames.append(outname)
        else:
            self.logger.error('Image projection failed')
        return ok


    def fnameGenerate(self, names=False):
        """
        Generate a specific filename for this product.
        
        **Returns**
            
            *bands*    : Integer

            *dataType* : Gdal data type

            *outname*  : New filename
        """

        if self.imgType == "amp":
            bands = self.n_bands
            if self.bitsPerSample == 8:
                dataType = GDT_Byte
            else:
                dataType = GDT_UInt16
            self.bandNames = None
        if self.imgType == "sigma":   #Both snap and old Siglib Compatible
            bands = self.n_bands
            dataType = GDT_Float32
            self.bandNames = None
        if self.imgType == "beta":   #Snap-only datatype
            bands = self.n_bands
            dataType = GDT_Float32
            self.bandNames = None
        if self.imgType == "gamma":  #Snap-only datatype
            bands = self.n_bands           
            self.bandNames = None
            dataType = GDT_Float32
        if self.imgType == "noise":    #Old SigLib datatype
            bands = 1
            dataType = GDT_Float32
            self.bandNames['noise']
        if self.imgType == "theta":    #Old SigLib datatype
            bands = 1
            self.bandNames = ['theta']
            dataType = GDT_Float32
        if self.imgType == "phase":    #Old SigLib datatype
            bands = self.n_bands
            dataType = GDT_Float32
            self.bandNames = None

        if self.bandNames == None:
            self.bandNames = []
            names = self.polarization.split()
            for name in names:
                self.bandNames.append(self.imgType[0].lower()+name)


        outname = str(self.meta.dimgname+'_'+self.imgType[0:1].lower())
        return bands, dataType, outname


    def cropImg(self, ullr, subscene):
        """
        Given the cropping coordinates, this function tries to crop in a straight-forward way using cropSmall.
        If this cannot be accomplished then cropBig will do the job.

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
        If cropping cannot be done in a straight-forward way (cropSmall), gdalwarp is used instead
        
        **Parameters**
            
            *llur*     : list/tuple of tuples in projected units

            *subscene* : the name of a subscene
        """

        imgFormat = 'vrt'
        ext = '.vrt'
        inname = self.FileNames[-1] # this is potentially an issue here
        outname = os.path.splitext(inname)[0] +'_'+subscene +ext
        
        sep = ' '
        crop = str(llur[0][0]) +sep+ str(llur[0][1]) +sep+ str(llur[1][0]) +sep+ str(llur[1][1])
        cmd = 'gdalwarp -of ' + imgFormat + ' -te ' + crop + ' -t_srs ' +\
                os.path.join(self.projdir, self.proj+'.wkt') +\
            ' -r near -order 1 -dstnodata 0 ' +\
            inname + ' ' + outname

        command = shlex.split(cmd)

        ok = subprocess.Popen(command).wait()
        if ok == 0:
            self.logger.debug('img cropped -method warp') 
            self.FileNames.append(outname)
        else:
            self.logger.error('Could not crop image in cropBig')
        return ok

    def cropSmall(self, urll, subscene):
        """
        This is a better way to crop because there is no potential for warping.
        However, this will only work if the region falls completely within the image.

        **Parameters**
            
            *urll*     : list/tuple of tuples in projected units

            *subscene* : the name of a subscene
        """

        imgFormat = 'vrt'
        ext = '.vrt'
        inname = self.FileNames[-1] # this is potentially an issue here
        outname = os.path.splitext(inname)[0] +'_'+subscene +ext
        
        inname = self.FileNames[-1] # this is potentially an issue here
        self.fname_nosubest = inname
       
        sep = ' '
        crop = str(urll[0][0]) +sep+ str(urll[0][1]) +sep+ str(urll[1][0]) +sep+ str(urll[1][1])
        cmd = 'gdal_translate -projwin ' + crop + ' -a_nodata 0 -of '+ imgFormat +' ' +\
            inname + ' ' + outname

        command = shlex.split(cmd)

        ok = subprocess.Popen(command).wait()
        if ok == 0:
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
        ds = gdal.Open(inname)
        if imgType == 'sigma' or imgType == 'amp':
            for band in range(1,ds.RasterCount+1):
                bstr = bstr + bandFlag + str(band)
                
        ds = None
        cmd = 'gdal_rasterize ' + sidecode + bstr +\
            ' -burn 0' + ' -l ' +\
            mask + ' ' + os.path.join(vectdir, mask  + '.shp') +\
            ' ' + inname

        ok = os.system(cmd)

        if ok == 0:
            self.logger.info('Completed image mask')
        else:
            self.logger.error('Image masking failed')

    def makePyramids(self):
        """
        Make image pyramids for fast viewing at different scales (used in GIS)
        """

        inname = self.FileNames[-1]

        cmd = 'gdaladdo -r gauss -ro --config COMPRESS_OVERVIEW DEFLATE ' +\
            ' --config USE_RRD YES ' +\
            inname +' 2 4 8 16 32 64'

        command = shlex.split(cmd)

        ok = subprocess.Popen(command).wait()
        if ok == 0:
            self.logger.info('Completed image pyramids')

        else:
            self.logger.error('Image pyramid scheme collapsed')

    def vrt2RealImg(self, subset=None):
        """
        Used to convert a vrt to a tiff (or another image format)
        """
        
        inname = self.FileNames[-1]
        outname = os.path.splitext(inname)[0] + '_subset'+ self.imgExt

        cmd = 'gdal_translate -of '+ self.imgFormat +' -co \"COMPRESS=LZW\" -a_nodata 0 ' +\
            inname +' '+ outname            
            
        command = shlex.split(cmd)
        try:
		ok = subprocess.Popen(command).wait()
        except:
            self.logger.error("vrt2RealImg failed")
		
        if ok == 0:
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

        inname = self.FileNames[-1]

        self.logger.info("Getting image stats for " + inname)

        self.openDataset(inname)

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

        bandobj = None
        self.inds = None
        return stats

    def applyStretch(self, stats, procedure='std', sd=3, bitDepth=8, sep=False):
        """
        Given an array of stats per band, will stretch a multiband image to the dataType based on
        procedure (either std for standard deviation, with +ve int in keyword sd,
        or min-max, also a linear stretch).

        *A nodata value of 0 is used in all cases*

        *For now, dataType is byte and that's it*

        **Note:** gdal_translate -scale does not honour nodata values
        See: http://trac.osgeo.org/gdal/ticket/3085

        Have to run this one under the imgWrite code. The raster bands must be integer, float or byte
        and int data assumed to be only positive. Won't work very well for dB scaled data (obviously)
        it is important that noData is set to 0 and is meaningful.

        sep =  separate: applies individual stretches to each band (=better visualization/contrast)

        !sep = together: applies the same stretch to all bands (looks for the band with the greatest dynamic range) (=more 'correct')

        For further ideas see: http://en.wikipedia.org/wiki/Histogram_equalization
        
        **Parameters**
        
            *stats* : Array of stats for a band, in arrary: band, range, dtype, nodata, min,max,mean,std
            
            *procedure* : std or min-max
            
            *sd* : # of standard deviations
            
            *bitDepth* : # of bits per pixel
            
            *sep* : False for same stretch to all bands, True for individual stretches
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

        if sep == False and stretchVals.shape[1] > 1:
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

        else:
            # rebuild stretchVals with offset
            stretchVals[:,4] = 0

        filename = self.FileNames[-1]
        self.openDataset(filename)

        #write out a tif of this imgType
        self.imgWrite(stretchVals=stretchVals)
        self.inds = None

        # delete original file and rename tmp
        if self.fname_nosubest != None:
            filename = self.fname_nosubest
        
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


    def getBandData(self, band, inname):
        """
        opens an img file and reads in data from a given band
        assume that the dataset is small enough to fit into memory all at once
        
        **Parameters**
        
            *Band* : Array of data for a specific band
        
        **Returns**
            
            *imgData*  : data from the given band

            *xSpacing* : size of pixel in x direction (decimal degrees)

            *ySpacing* : size of pixel in y direction (decimal degrees)
        """

        gdal.AllRegister() # for all purposes
        inname = self.FileNames[-1]
        ds = gdal.Open(inname, GA_ReadOnly)
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
        Removes intermediate files that have been written within the workflow.

        Input a list of items to delete: raw, nil, proj, crop
        
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
        
        **Parameters**
        
            *datachunk* : chunk of data being processed
            
            *n_lines* : size of the chunk
            
        **Returns**
            
            *caldata* : calibrated chunk
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
        
        **Parameters**
        
            *datachunk* : Chunk of data being processed
            
        **Returns**
        
            *outdata* : chunk data in amplitude format        
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
        the data using a pauli decomposition. 

        Differs from imgWrite because it ingests all bands at once...
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

        self.FileNames.append(outname+ext)
        self.logger.debug('Image written ' + outname+ext)

        outds = None         # release the dataset so it can be close
        
    def removeHandler(self):
        self.logger.handlers = []
    
    def cleanFileNames(self):
        temp = [0,0]
        temp[0] = self.FileNames[0]
        temp[1] = self.FileNames[1]
        self.FileNames = temp        

    def snapCalibration(self, outDataType='sigma', saveInComplex=False):
        '''
        This fuction calibrates radarsat images into sigma, beta, or gamma
        
        **Parameters**
        
            *outDataType* : Type of calibration to perform (sigma, beta, or gamma), default is sigma
        
            *saveInComplex* : Output complex sigma data (for polarimetric mode)
        '''
        
        os.chdir(self.tmpDir)       
        self.openDataset(self.fname, self.path)       
        outname = self.fnameGenerate()[2]         
        
        #Sigma calibration
        input = os.path.join(self.path, self.fname)
        rsat = ProductIO.readProduct(input)

        output = os.path.join(self.tmpDir, outname)
        
        parameters = self.HashMap()
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
                self.logger.error('Valid out data type not specified!')
                return Exception
        
        target = GPF.createProduct("Calibration", parameters, rsat)
        ProductIO.writeProduct(target, output, 'BEAM-DIMAP')         
        self.FileNames.append(outname+'.dim')

    def snapSubset(self, idNum, lat, longt, dir, ullr=None):
        '''
        Using lat long provided, create bounding box 200x200 pixels around it, and subset this
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
        
        inname = os.path.join(self.tmpDir, self.FileNames[-1])
        output = os.path.join(dir, os.path.splitext(self.FileNames[-1])[0] + '__' + str(idNum) + '_subset')
        
        rsat = ProductIO.readProduct(inname)
        info = rsat.getSceneGeoCoding()
        
        geoPosOP = snappy.jpy.get_type('org.esa.snap.core.datamodel.PixelPos')
        
        if ullr == None:   #We are in polarimetry mode, make 200x200 BB around lat long
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
            if (x + width) > self.n_cols:
                width = self.n_cols
            height = 1200
            if (x + height) > self.n_rows:
                height = self.n_rows
            
        else:    #We are in Scientific mode/a normal crop, bounding box provided
            pixel_posUL = info.getPixelPos(GeoPos(ullr[0][1], ullr[0][0]), geoPosOP())
            pixel_posLR = info.getPixelPos(GeoPos(ullr[1][1], ullr[1][0]), geoPosOP())
            
            topL_x = int(round(pixel_posUL.x))
            topL_y = int(round(pixel_posUL.y))
            x_LR = int(round(pixel_posLR.x))
            y_LR = int(round(pixel_posLR.y))

            width = x_LR - topL_x
            height = y_LR - topL_y
        
        parameters = self.HashMap()  
        try:
            parameters.put('region', "%s,%s,%s,%s" % (topL_x, topL_y, width, height))       
            target = GPF.createProduct('Subset', parameters, rsat) 
            if ullr == None:
                ProductIO.writeProduct(target, output, 'BEAM-DIMAP')
        except:
            self.logger.error('Subset region invalid!')
            return -1
        
        self.logger.debug("Subset complete on " + self.zipname + " for id " + str(idNum))
        self.FileNames.append(output+'.dim')
        return output
        
        
    def matrix_generation(self, matrix):
        '''
        Generate chosen matrix for calibrated quad-pol product
        
        **Parameters**
        
            *matrix* : matrix to be generated. options include: C3, C4, T3, or T4
            
            *input* : filename with path of snap raster (.dim) to have matrix generated upon (must be calibrated)
            
        **Returns**
        
            *output* : filename of new product        
        '''
         
        output = os.path.splitext(self.FileNames[-1])[0] +  '_' + matrix
        inname = self.FileNames[-1]
        
        rsat = ProductIO.readProduct(inname)
        parameters = self.HashMap()
        
        parameters.put('matrix', matrix)
        
        target = GPF.createProduct('Polarimetric-Matrices', parameters, rsat)
        ProductIO.writeProduct(target, output, 'BEAM-DIMAP')
        
        self.logger.debug(matrix + ' generated sucessfully') 
        
        self.FileNames.append(output+'.dim')        
        return output+'.dim'
        
        
    def polarFilter(self):
        '''
        Apply a speckle filter on a fully polarimetric product
        
        **Parameters**
            
            *input* : filename with path of snap raster (.dim) to filter
            
        **Returns**
        
            *output* : filename of new product
        '''
        
        output = os.path.splitext(self.FileNames[-1])[0] +  '_filter'
        inname = self.FileNames[-1]
        
        rsat = ProductIO.readProduct(inname)
        parameters = self.HashMap()
        
        parameters.put('filter', 'Refined Lee Filter')
        parameters.put('windowSize', '5x5')
        parameters.put('numLooksStr', '3')
        
        
        target = GPF.createProduct('Polarimetric-Speckle-Filter', parameters, rsat)
        ProductIO.writeProduct(target, output, 'BEAM-DIMAP')
        
        self.logger.debug('Filtering successful')
        
        self.FileNames.append(output +'.dim')
        return output                  
        
    def decomposition_generation(self, decomposition, outputType=0, amp = False):
        '''
        Generate chosen decomposition on a fully polarimetric product, if looking to create an amplitude image with quad-pol data, set amp to True
        and send either a Pauli Decomposition or Sinclair Decomposition
        
        **Parameters**
        
            *decomposition* : decomposition to be generated. options include: Sinclair Decomposition, Pauli Decomposition, Freeman-Durden Decomposition, Yamagushi Decomposition, van Zyl Decomposition, H-A-Alpha Quad Pol Decomposition, Cloude Decomposition, or Touzi Decomposition
        pixel_posUL = info.getPixelPos(GeoPos(ullr[0][1], ullr[0][0]), geoPosOP())
            *input* : filename with path of snap raster (.dim) to have decomposition generated upon (must be calibrated and have a matrix generated)    
        
            *matrix* : The matrix (string) that was generated on this product
            
            *dir* : directory that finished tifs will be stored in
            
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
            self.FileNames.append(outname+'.dim')
            return 0
            
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
              
        return output   
    
    def snapTC(self, proj, projDir, smooth = True, outFormat = 'BEAM-DIMAP'):
        '''
        Perform an Ellipsoid-Correction using snap. This function also projects the product
        
        **Parameters**
        
            *proj* : name of wkt projection file (minus file extension)
            
            *projDir* : directory containing projection file
        
            *existingInput* : If a snap-product has already been created before this function (default is True)
            
            *smooth* : If quanitative data, do not smooth (Smooth using Bilinear resampling for quality)
            
            *outFormat* : Format of product this function returns, default is BEAM-DIMAP

        '''
        
        os.chdir(self.tmpDir)     
        outname = self.fnameGenerate(projout=proj, TC=True)[2]         
        inname = self.FileNames[-1]
            
        if outFormat == 'BEAM-DIMAP':
            output = os.path.join(self.tmpDir, outname)
            ext = '.dim'
        else:
            output = os.path.join(self.imgDir, outname)
            ext = '.tif'
                
        parameters = self.HashMap()
        product = ProductIO.readProduct(inname)
        
        #Parameters section##################################
        if not smooth:
            parameters.put('imgResamplingMethod', 'NEAREST_NEIGHBOUR')
            
        if self.imgType == 'beta':
            dt = 'Beta0_'
        elif self.imgType == 'gamma':
            dt = 'Gamma0_'
        elif self.imgType == 'sigma':
            dt = 'Sigma0_'
        elif self.imgType == 'amp':
            if 'Q' in self.meta.beam:
                pass
            else:
                dt = 'Amplitude_'
        else:
            self.logger.error('Invalid out data type!')
            return Exception
        
        #Create string containing all output bands
        if 'Q' not in self.meta.beam:
            count = 0
            bands = ''
            while count < self.meta.n_bands:
                band = str(self.bandNames[count][1:])
                if count == 0:
                    bands = bands + dt + band
                else:
                    bands = bands + ',' + dt + band
                count +=1
                
            parameters.put('sourceBands', bands)  
         
        readProj = open(os.path.join(projDir, proj + '.wkt'), 'r').read()           
        parameters.put('mapProjection', readProj)
        ######################################################	   
        target = GPF.createProduct("Ellipsoid-Correction-GG", parameters, product) 
           
        ProductIO.writeProduct(target, output, outFormat)
        self.logger.debug('Terrain-Correction successful!') 
        
        self.FileNames.append(outname+ext)          
        
    def snapDataTypeConv(self, outFormat='GeoTiff-BigTiff'):
        '''
        Convert image data to byte format using gdal
        
        **Parameters**
            
            *outFormat* : Format of product this function returns, default is GeoTiff-BigTiff
        '''
        
        outname = self.fnameGenerate(DC=True)[2] 
        output = os.path.join(self.imgDir, outname)  #output, post gdal formatting
        inname = self.FileNames[-1]
        
        parameters = self.HashMap()
        product = ProductIO.readProduct(inname)
        
        parameters.put('targetDataType', "float32")
        
        target = GPF.createProduct("Convert-Datatype", parameters, product)            
        ProductIO.writeProduct(target, output, outFormat)      
        
        self.FileNames.append(outname+'.dim')
        self.logger.debug("Img converted to byte successfully")
    
    def compress(self):
        '''
        Use gdal to LZW compress an image
        '''
        
        inname = os.path.splitext(self.FileNames[-1])[0]        
              
        command = "gdal_translate -of GTiff -co COMPRESS=LZW -a_nodata 0 " + inname + '.tif ' + inname + '_tmp.tif' 
        os.system(command)
        
        os.remove(inname+'.tif')
        os.rename(inname+'_tmp.tif', inname+'.tif')
        
    def makeAmp(self, newFile=True, pol=False):
        '''
        Use snap bandMaths to create amplitude band for SLC products
        '''
        
        count = 0
        bands = snappy.jpy.array('org.esa.snap.core.gpf.common.BandMathsOp$BandDescriptor', self.meta.n_bands)
       
        if newFile:
            outname = self.fnameGenerate()[2] 
            inname = os.path.join(self.path, self.fname)
            output = os.path.join(self.tmpDir, outname)
        else:
            inname = self.FileNames[-1]
            output = os.path.splitext(self.FileNames[-1])[0] + '_amp'
        
        while count < self.meta.n_bands:
            if self.meta.n_bands == 1:
                band = str(self.bandNames)
            else:
                band = str(self.bandNames[count])[1:]
            
            target = self.BandMathsOp()
            target.name = 'Amplitude_' + band
            target.type = 'float32'
            target.expression = 'sqrt(pow(i_' + band + ', 2) + pow(q_' + band + ', 2))'
            bands[count] = target
            count += 1
         
        parameters = self.HashMap()
        product = ProductIO.readProduct(inname) 
        parameters.put('targetBands', bands)
        t = GPF.createProduct('BandMaths', parameters, product)
        ProductIO.writeProduct(t, output, 'BEAM-DIMAP')
        count += 1
        
        if not pol:
            self.FileNames.append(output+'.dim')
        return output
        
    
    def defineBands(self, inname):
        self.bandNames = []
        ds = gdal.Open(inname+self.imgExt)
        for band in range(1,ds.RasterCount+1):
            self.bandNames.append(band)
            
    def slantRangeMask(self, mask, input):
        '''
        This function takes a wkt with lat long coordinates, and traslates it into a wkt in line-pixel coordinates
        '''
        input = input+'.dim'
        
        rsat = ProductIO.readProduct(input)
        info = rsat.getSceneGeoCoding()        
        
        geoPosOP = snappy.jpy.get_type('org.esa.snap.core.datamodel.PixelPos')

        splitMask = re.split('([(), ])', mask)
        
        i=0
        while i < len(splitMask):
            if any(char.isdigit() for char in splitMask[i]):
                pixel_pos = info.getPixelPos(GeoPos(float(splitMask[i]), float(splitMask[i+2])), geoPosOP())
                splitMask[i] = pixel_pos.x
                splitMask[i+2] = pixel_pos.y
                i+=2
            i+=1
                
        newMask = ''
        for item in splitMask:
            newMask = newMask + item
            
        return newMask
                    
        
        
                
                    
                    
                            
                