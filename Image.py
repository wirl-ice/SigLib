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
**Modified on** 9 May 13:50:00 2020 **@reason:** Added terrain correction for known elevation **@auther:** Allison Plourde

**Common Parameters of this Module:**

*zipfile* : a valid zipfile name with full path and extension

*zipname* : zipfile name without path or extension

*fname* : image filename with extention but no path

*imgname* : image filename without extention

*granule* : unique name of an image in string format

*path* : path to the image in string format
"""

import os
import sys
import numpy
import subprocess
import math
import logging
import shlex
        
import gc

from configparser import ConfigParser

from osgeo import gdal
from osgeo.gdalconst import *
from osgeo import gdal_array

import Util

class Image(object):
    """
    This is the Img class for each image.  RSAT2, RSAT1, S1
    
    Opens the file specified by fname, passes reference to the metadata class and declares the imgType of interest.

        **Parameters**
            
            *fname*     

            *path*   

            *meta*      : reference to the meta class

            *imgType*   : amp, sigma, beta or gamma

            *imgFormat* : gdal format code gtiff, vrt

            *zipname*  
    """

    def __init__(self, fname, path, meta, imgType, imgFormat, zipname, imgDir, tmpDir, projDir, loghandler = None, eCorr = None, initOnly=False):

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
        self.proj = 'nil' # initialize to nil (then change as appropriate)
        self.projdir = projDir
        self.elevationCorrection = eCorr
            
        # if values might change make a local copy
        try:
            self.polarization = self.meta.polarization
        except:
            self.polarization = 'HH'
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

        if not initOnly:
            self.openDataset(self.fname, self.path)

            if self.imgType == 'amp' and 'Q' in self.meta.beam:  # this would be a quad pol scene...
                self.decomp(format='GTiff')
            elif self.sattype == 'ASF_CEOS':
                self.asfR1Process()
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
        if(self.n_rows == 0 or self.n_cols == 0):
            self.logger.info("Using meta row/col count over gdal...")
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

        chunkSize = 300 # 300 seems to work ok, go lower if RAM is wimpy...

        ############################################## SETUP FOR OUTPUT
        
        if format.lower() == 'gtiff':
            ext = '.tif'
            driver = gdal.GetDriverByName('GTiff')
            options = ['COMPRESS=LZW']
            if self.n_bands > 3:
                    options = ['PHOTOMETRIC=MINISBLACK'] #probs here if you LZW compress
            if self.imgType == 'sigma':
                    options.append('BIGTIFF=YES')
        elif format.lower() == 'hfa':
            ext = '.img'
            driver = gdal.GetDriverByName('HFA')
            
        elif format.lower() == 'vrt':
            self.logger.error('Cannot write a vrt as an original image')
            return
            
        elif format.lower() == 'imgformat':
            ext = self.imgExt
            driver = gdal.GetDriverByName(str(self.imgFormat))
            if self.imgFormat.lower() == 'gtiff':
                options = ['COMPRESS=LZW']
                if self.n_bands > 3:
                    options = ['PHOTOMETRIC=MINISBLACK']
                if self.imgType == 'sigma':
                    options.append('BIGTIFF=YES')
        
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
            n_chunks = int(self.n_rows / chunkSize + 1)
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
                    self.logger.error("GDAL unable to read scene!")

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
            if self.elevationCorrection == "1":
                self.logger.info("Using terrain corrected GCPs with user input elevation = {} m".format(self.elevationCorrection))
                gcp_list = self.correct_known_elevation()
                outds.SetGCPs(gcp_list, self.meta.geoptsGCS)
            else:
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

    #OBSOLETE
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
    
    def projectImg(self, proj, projSRID, format=None, resample='bilinear', clobber=True):
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

        os.chdir(self.tmpDir)

        inname = ''
        for file in self.FileNames:
            if 'a.tif' in file or 's.tif' in file: #handle multiple instances
                inname = file
        if not inname:
            inname = self.FileNames[-1] #last file
            print("Here {}".format(inname))

        outname = os.path.splitext(inname)[0] + '_proj' + ext

        if proj == '':
            command = 'gdalwarp -of ' + imgFormat +  ' -t_srs ' +\
                    'EPSG:' + projSRID +\
                        ' -order 3 -dstnodata 0 -r ' + resample +' '+clobber+ \
                        inname + ' ' + outname
        else:
            command = 'gdalwarp -of ' + imgFormat + ' -t_srs ' + \
                      os.path.join(self.projdir, proj + '.wkt') + \
                      ' -order 3 -dstnodata 0 -r ' + resample + ' ' + clobber + \
                      inname + ' ' + outname

        try:
            ok = subprocess.Popen(command).wait()  # run the other way on linux
        except:
            cmd = shlex.split(command)  #TODO this may be a problem for windows
            ok = subprocess.Popen(cmd).wait()

        if ok == 0:
            if proj == '':
                self.proj = 'EPSG:' + projSRID
            else:
                self.proj = proj
            self.logger.info('Completed image projection')
            
            self.FileNames.append(outname)
        else:
            self.logger.error('Image projection failed')

        return ok

    def asfR1Process(self):

        os.chdir(self.path)
        n_bands, dataType, outname = self.fnameGenerate()
        ext = '.tif'

        # create asf config
        command = "asf_mapready -create {}".format(self.zipname + '.cfg')
        fail = os.system(command)
        print(fail)
        if fail:
            print(
                "Problem with initializing configfile for ASFMapReady, is it installed and configured for the command line?")
            return

        asfConfig = os.path.join(self.path, self.zipname + '.cfg')

        # Remove file header so config can be read by configparser
        with open(asfConfig, 'r') as tmpIn:
            content = tmpIn.read().splitlines(True)
        with open(asfConfig, 'w') as tmpout:
            tmpout.writelines(content[1:])
        tmpIn.close()
        tmpout.close()

        config = ConfigParser()
        config.read(asfConfig)

        # Set initial parameters (in, out, outdir)
        inout = config['General']
        inout['input file'] = self.zipname
        inout['output file'] = outname
        inout['default output dir'] = self.tmpDir

        if self.imgType == 'sigma':
            inout['Calibration'] = '1'

        # Write changes back to config
        with open(asfConfig, 'w') as cfg:
            config.write(cfg)
            cfg.close()
        config = None

        # Regenerate config, same command as above
        os.system(command)

        # Remove file header so config can be read by configparser
        with open(asfConfig, 'r') as tmpIn:
            content = tmpIn.read().splitlines(True)
        with open(asfConfig, 'w') as tmpout:
            tmpout.writelines(content[1:])
        tmpIn.close()
        tmpout.close()

        config = ConfigParser()
        config.read(asfConfig)

        if self.imgType == 'sigma':
            mode = config['Import']
            mode['radiometry'] = 'sigma_image'
            cal = config['Calibration']
            cal['radiometry'] = 'sigma'
            out = config['Export']
            out['byte conversion'] = 'none'

        # Set projection details
        geocode = config['Geocoding']
        geocode['projection'] = 'geographic'
        geocode['datum'] = 'wgs84'

        with open(asfConfig, 'w') as cfg:
            config.write(cfg)
            cfg.close()
        config = None

        command = 'asf_mapready {}'.format(self.zipname + '.cfg')
        os.system(command)

        self.FileNames.append(outname + ext)


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
        if self.imgType == "sigma":
            bands = self.n_bands
            dataType = GDT_Float32
            self.bandNames = None
        if self.imgType == "noise":
            bands = 1
            dataType = GDT_Float32
            self.bandNames['noise']
        if self.imgType == "theta":
            bands = 1
            self.bandNames = ['theta']
            dataType = GDT_Float32
        if self.imgType == "phase":
            bands = self.n_bands
            dataType = GDT_Float32
            self.bandNames = None

        if self.sattype == 'SEN-1':
                self.bandNames = self.polarization
                
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
        outname = os.path.splitext(inname)[0] +'_'+str(subscene) +ext
        
        sep = ' '
        crop = str(llur[0][0]) +sep+ str(llur[0][1]) +sep+ str(llur[1][0]) +sep+ str(llur[1][1])

        if 'EPSG:' in self.proj:
            cmd = 'gdalwarp -of ' + imgFormat + ' -te ' + crop + ' -t_srs ' +\
                    self.proj +\
                ' -r near -order 1 -dstnodata 0 ' +\
                inname + ' ' + outname
        else:
            cmd = 'gdalwarp -of ' + imgFormat + ' -te ' + crop + ' -t_srs ' + \
                  os.path.join(self.projdir, self.proj + '.wkt') + \
                  ' -r near -order 1 -dstnodata 0 ' + \
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
        outname = os.path.splitext(inname)[0] +'_'+str(subscene) +ext

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


    def maskImg(self, mask, vectdir, side, inname=None):
        """
        Masks all bands with gdal_rasterize using the 'layer'

        side = 'inside' burns 0 inside the vector, 'outside' burns outside the vector

        Note: make sure that the vector shapefile is in the same proj as img (Use reprojSHP from ingestutil)

        **Parameters**
            
            *mask*    : a shapefile used to mask the image(s) in question

            *vectdir* : directory where the mask shapefile is

            *side*    : 'inside' or 'outside' depending on desired mask result
            
        """
        if inname == None:
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
        
        cmd = '''gdal_translate -of {} -co "COMPRESS=LZW" -a_nodata 0 {} {}'''.format(self.imgFormat, inname, outname)
            
        command = shlex.split(cmd)
        ok = subprocess.Popen(command).wait()

        if self.imgFormat == 'GTiff' and ok != 0:
            self.logger.info('Normal write failed, attempting BigTiff write')

            cmd = '''gdal_translate -of {} -co "COMPRESS=LZW" -co "BIGTIFF=YES" -a_nodata 0 {} {}'''.format(self.imgFormat, inname, outname)
            command = shlex.split(cmd)
            ok = subprocess.Popen(command).wait()

        if ok == 0:
            self.FileNames.append(outname)          ###
            self.logger.debug('Completed export to tiff ' + outname)
        else:
            self.logger.error('Image export failed')

    def getImgStats(self, save_stats = False):
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
        
        if save_stats:
            statfile = inname[:-4] + "_stats.csv"
            numpy.savetxt(statfile, stats, delimiter = ",")
            
        return stats

    def applyStretch(self, stats, procedure='std', sd=3, bitDepth=8, sep=False, inst = ''):
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
        if inst:
            os.rename(os.path.splitext(self.FileNames[1])[0] + '_temp_stretch.tif', os.path.splitext(self.FileNames[1])[0] + '_inst'+str(inst)+'.tif') 
            os.remove(self.FileNames[-1])
            self.FileNames[len(self.FileNames)-1] = os.path.splitext(self.FileNames[1])[0] + '_inst'+str(inst)+'.tif'                 
        else:
            os.rename(os.path.splitext(self.FileNames[1])[0] + '_temp_stretch.tif', os.path.splitext(self.FileNames[1])[0] + '_final.tif') 
            os.remove(self.FileNames[-1])
            self.FileNames[len(self.FileNames)-1] = os.path.splitext(self.FileNames[1])[0] + '_final.tif'                 

        
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


    def getBandData(self, band, inname=None):
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
        if inname == None:
            inname = self.FileNames[-1]
            
        gdal.AllRegister() # for all purposes
        ds = gdal.Open(inname, GA_ReadOnly)
        n_cols = ds.RasterXSize
        n_lines = ds.RasterYSize
        bandobj = ds.GetRasterBand(band)
        # read in all data (better not be too big for memory!)
        imgData = gdal_array.BandReadAsArray(bandobj, 0, 0, n_cols, n_lines )

        bandobj = None
        ds = None
        return imgData

    def cleanFiles(self, levels=['crop']):
        """
        Removes intermediate files that have been written within the workflow.

        Input a list of items to delete: raw, nil, proj, crop
        
        **Parameters**
            
            *levels* : a list of different types of files to delete
        """
        deleteMe = []
        if 'crop' in levels and len(self.FileNames) > 3:
            for file in self.FileNames:
                if 'crop' in file:
                    deleteMe.append(file)
        if 'proj' in levels and len(self.FileNames) > 2:
            for file in self.FileNames:
                if 'proj' in file:
                    deleteMe.append(file)
        if 'nil' in levels and len(self.FileNames) > 1:
            deleteMe.append(self.tifname)
            print(self.tifname)
            #for file in self.FileNames:
                #if 'nil' in file:   #this needs to be tested
                    #deleteMe.append(glob.glob(file))
                    
        for filename in deleteMe:
            if filename != []:
                try:
                    os.remove(filename)
                    self.FileNames.remove(filename)
                except:
                    self.FileNames.remove(filename)
                    

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

        elif self.sattype == 'SEN-1':
            datachunk = numpy.float32(datachunk)**2 # convert to float, prevent integer overflow
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
            driver = gdal.GetDriverByName(str(self.imgFormat))
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
        n_chunks = int(self.n_rows / chunkSize + 1)
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
            if self.elevationCorrection:
                self.logger.info("Using terrain corrected GCPs with user input elevation = {} m".format(self.elevationCorrection))
                gcp_list = self.correct_known_elevation()
                outds.SetGCPs(gcp_list, self.meta.geoptsGCS)
            else:
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
        
    def compress(self):
        '''
        Use GDAL to LZW compress an image
        '''
        
        inname = os.path.splitext(self.FileNames[-1])[0]
        
        command = "gdal_translate -of GTiff -co COMPRESS=LZW -a_nodata 0 " + inname + '.tif ' + inname + '_tmp.tif'
        os.system(command)
        
        os.remove(inname+'.tif')
        os.rename(inname+'_tmp.tif', inname+'.tif')

    def correct_known_elevation(self):
        '''
        Transforms image GCPs based on a known elevation (rather than the default average)
        '''

        height_correction = float(self.elevationCorrection)

        self.meta.tie_points['longitude'] = numpy.radians(self.meta.tie_points['longitude'])
        self.meta.tie_points['latitude'] = numpy.radians(self.meta.tie_points['latitude'])

        #Get M by N matrix size and position of tie points
        tie_point_lines = numpy.unique(self.meta.tie_points['line'])
        tie_point_pixels = numpy.unique(self.meta.tie_points['pixel'])
        M = len(tie_point_pixels)
        N = len(tie_point_lines)

            
        def m_by_n_array(arr1, arr2=None):
            if arr2:
                x = numpy.empty((N,M), dtype=(float, 2))
            else:
                x = numpy.empty((N,M), dtype=float)
            line = 0
            for i in range(N):
                for j in range(M):
                    if arr2:
                        x[i][j] = (arr1[(i+j)+line], arr2[(i+j)+line])
                    else:
                        x[i][j] = arr1[(i+j)+line]
                line += M-1
            return x

        pixels = m_by_n_array(self.meta.tie_points['pixel'])
        lines = m_by_n_array(self.meta.tie_points['line'])
        lng_matrix = m_by_n_array(self.meta.tie_points['longitude'])
        lat_matrix = m_by_n_array(self.meta.tie_points['latitude'])

        #Transform tie-points from geographic to cartesian space
        xuv, yuv, zuv = Util.geographic_to_cartesian(lat_matrix, lng_matrix, self.meta.ellip_maj, self.meta.ellip_min)
            
        #Step 1: determine near range and far range pixels based on time ordering
        if self.meta.order_Rg == 'Increasing':
                p_near = 0
        elif self.meta.order_Rg == 'Decreasing':
                p_near = self.meta.n_cols - 1
        p_far = self.meta.n_cols - 1 - p_near

        #Step 2: interpolate near and far range pixel 
        #(interpolation is uneccesary since coordinates lie on tie points)
        P_near = int(p_near/(self.meta.n_cols-1)*(M-1))
        P_far = int(p_far/(self.meta.n_cols-1)*(M-1))
        #x_near, y_near, z_near = xuv[:,P_near], yuv[:,P_near], zuv[:,P_near]
        #x_far, y_far, z_far = xuv[:,P_far], yuv[:,P_far], zuv[:,P_far]

        #Step 3: Calculate slant range at the tie points and near and far range pixels
        if self.meta.productType == 'SLC':
            R_near = self.meta.near_range
            if self.meta.order_Rg == 'Increasing':
                R = R_near + pixels*self.meta.pixelSpacing
            elif self.meta.order_Rg == 'Decreasing':
                R = R_near + (self.meta.n_cols-1-pixels)*self.meta.pixelSpacing
            R_far = R_near + (self.meta.n_cols)*self.meta.pixelSpacing
        else:
            if self.meta.order_Rg == 'Increasing':
                    D = pixels*self.meta.pixelSpacing-self.meta.gr0
            elif self.meta.order_Rg == 'Decreasing':
                    D = (self.meta.n_cols-1-pixels)*self.meta.pixelSpacing-self.meta.gr0
            D_far = (self.meta.n_cols-1)*self.meta.pixelSpacing-self.meta.gr0

            R = 0
            for i in range(len(self.meta.gsr)):
                    R += float(self.meta.gsr[i])*D**i
            R_far = 0
            for i in range(len(self.meta.gsr)):
                    R_far += float(self.meta.gsr[i])*D_far**i

        #Step 4: Calculate the distance of the satellite to center of ellipse, h_sat
        #This can be performed with the following calculations or by extracting the 
        #satellite altitude from the metadata
        #r_near = numpy.sqrt(x_near**2+y_near**2+z_near**2)
        #r_far = numpy.sqrt(x_far**2+y_far**2+z_far**2)
        #d = numpy.sqrt((x_far-x_near)**2+(y_far-y_near)**2+(z_far-z_near)**2)
            
        #eta = numpy.arccos((d**2+r_far**2-r_near**2)/(2*d*r_far))
        #gamma = numpy.arccos((d**2+R_far**2-R_near**2)/(2*d*R_far))
        #h_sat = numpy.sqrt(r_far**2 + R_far**2 - (2*r_far*R_far*numpy.cos(eta+gamma)))
        
        #Step 5: Calculate radius of ellipsoid at target
        r = numpy.sqrt(xuv**2+yuv**2+zuv**2)
        h_sat = self.meta.sat_alt + r

        #Step 6: calculate the angles subtended at the centre of the ellipsoid 
        #by the satellite and target with and without the height offset
        delta_h = height_correction-self.meta.h_proc
        eta1 = numpy.arccos((h_sat**2+r**2-R**2)/(2*h_sat*r))
        eta2 = numpy.arccos((h_sat**2+(r+delta_h)**2-R**2)/(2*h_sat*(r+delta_h)))
            
        #Step 7: Calculate the range offset
        if self.meta.productType == 'SLC':
            delta_R = numpy.sqrt(h_sat**2+r**2-2*h_sat*r*numpy.cos(eta2))-R
        else:
            delta_D = (eta2-eta1)*r

        #Step 8: Calculate the corrected pixel position
        if self.meta.productType == 'SLC':
            if self.meta.order_Rg == 'Increasing':
                    p_corr = pixels + delta_R/self.meta.pixelSpacing
            elif self.meta.order_Rg == 'Decreasing':
                    p_corr = pixels - delta_R/self.meta.pixelSpacing
        else:
            if self.meta.order_Rg == 'Increasing':
                    p_corr = pixels + delta_D/self.meta.pixelSpacing
            elif self.meta.order_Rg == 'Decreasing':
                    p_corr = pixels - delta_D/self.meta.pixelSpacing

        #Step 9: Bilinear interpolate the corrected coordinate
        #Normalize image coordinates, p,l, to size of tie-point grid
        P_corr = p_corr/(self.meta.n_cols-1)*(M-1)
        P = pixels/(self.meta.n_cols-1)*(M-1)
        L = lines/(self.meta.n_rows-1)*(N-1)

        x_corr, y_corr, z_corr = Util.interpolate_biquadratic(P_corr, P, L, xuv, yuv, zuv)

        #Step 10: Convert back to lat lng and reset GCPs
        lat_corr, lng_corr = Util.cartesian_to_geographic(x_corr, y_corr, z_corr, self.meta.ellip_maj, self.meta.ellip_min)

        gcp_list = []
        for l in range(len(pixels)):
            for p in range(len(pixels[l])):
                if lat_corr[l][p] != 0:
                    gcp_list.append(gdal.GCP(lng_corr[l][p], lat_corr[l][p], height_correction, pixels[l][p], lines[l][p]))

        return gcp_list

            
