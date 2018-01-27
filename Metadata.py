"""
**Metadata.py**

**Created on** Jan 1, 2009 **@author:** Derek Mueller

This module creates an instance of class Meta and contains functions to
query raw data files for metadata which is standardized and packaged for
later use, output to file, upload to database, etc.

        *This source code to extract metadata from CEOS-format RADARSAT-1 
        data was developed by Defence Research and Development Canada
        [Used with permission]*
        
"""

import os
import binascii
from xml.dom import minidom
import datetime
import math
import numpy
from scipy import interpolate
import pdb

import subprocess
import shlex
import struct

from osgeo import gdal
from osgeo import osr
from osgeo.gdalconst import *

import Util

D2R = math.pi / 180.0
R2D = 180.0 / math.pi

# RSAT1 - CDPF
# as determined from RSAT-1 Data Products Spec. manual (RSI-GS-026)
# ASF _ CEOS
# as determined from their documentation
# the records are indicated by the integer representation of the fields
    # first record subtype, record type code, second record type code and
    # third record type code, separated by hyphens (in an 11 char. string)
    # the nmemonic, record 'header', data type, field length and start pos are
    # given, pluse the field repet value
    # If field_repeat is missed, it has no repeat
# remember the offset is zero-based (so subtract one from value in appendix B)

# codes for data type, A = ASCII string, AI = ASCII int, AF = ASCII float


rsat_cdpf_fields = [
        ('18-10-18-20','scene_id','A',16,21),
        ('18-10-18-20','inp_sctim','A',32,69), #start of scn, center otherwise
        ('18-10-18-20','asc-des','A',16,101),
        ('18-10-18-20','ellip_des','A',16,165),
        ('18-10-18-20','ellip_maj','AF',16,181),
        ('18-10-18-20','ellip_min','AF',16,197),
        ('18-10-18-20','terrain_h','A',16,309),
        ('18-10-18-20','scene_len','AF',16,341),
        ('18-10-18-20','scene_wid','AF',16,357),
        ('18-10-18-20','mission_id','A',16,397),
        ('18-10-18-20','sensor_id','A',32,413),
        ('18-10-18-20','orbit_num','A',8,445),
        ('18-10-18-20','plat_lat','AF',8,453),
        ('18-10-18-20','plat_head','AF',8,469),
        ('18-10-18-20','wave_length','AF',16,501),
        ('18-10-18-20','fac_id','A',16,1047),
        ('18-10-18-20','prod_type','A',32,1111),
        ('18-10-18-20','n_azilok','AF',16,1175),
        ('18-10-18-20','n_rnglok','AF',16,1191),
        ('18-10-18-20','rng_res','AF',16,1351),
        ('18-10-18-20','azi_res','AF',16,1367),
        ('18-10-18-20','time_dir_pix','A',8,1527),
        ('18-10-18-20','time_dir_lin','A',8,1535),
        ('18-10-18-20','line_spacing','AF',16,1687),
        ('18-10-18-20','pix_spacing','AF',16,1703),
        ('18-10-18-20','clock_ang','AF',8,477),
        ('18-120-18-20','act_img_start','A',21,150),
        ('18-120-18-20','n_beams','AI',4,928),
        ('18-120-18-20','beam_type1','A',3,932),
        ('18-120-18-20','beam_type2','A',3,976),
        ('18-120-18-20','beam_type3','A',3,1020),
        ('18-120-18-20','beam_type4','A',3,1064),
        ('18-120-18-20','dopcen_inc','AF',16,2689),
        ('18-120-18-20','n_dopcen','AI',4,2705),
        ('18-120-18-20','eph_orb_data','AR',16,4649), #just want the 1st param
        ('18-120-18-20','n_srgr','AI',4,4883),
        ('18-120-18-20','srgr_coef','AF',16,4908,6), # just take the first set
        ('18-120-18-20','angle_first','A',16,7334), # coerce later
        ('18-120-18-20','angle_last','A',16,7350), # coerce later
        ('18-120-18-20','centre_lat','A',22,7385), # coerce later
        ('18-120-18-20','centre_lon','A',22,7407), # coerce later
        ('18-120-18-20','state_time','A',21,7480),
        ('18-50-18-20','table_des','A',24,37),
        ('18-50-18-20','n_samp','AI',8,61),
        ('18-50-18-20','samp_inc','AI',4,85),
        ('18-50-18-20','lookup_tab','A',8192,89), # 'AF',16,89,('AI',8,61)
        ('18-50-18-20','noise_scale','AF',16,8285),
        ('18-50-18-20','offset','A',16,8317),
        ('50-11-18-20','lat_first','BI',4,133),
        ('50-11-18-20','lat_mid','BI',4,137),
        ('50-11-18-20','lat_last','BI',4,141),
        ('50-11-18-20','long_first','BI',4,145),
        ('50-11-18-20','long_mid','BI',4,149),
        ('50-11-18-20','long_last','BI',4,153)
        #('63-192-18-18','nbit','AI',4,217) #only in img file but data header in ldr too..
        ]

rsat_cdpf_records = {
    '18-10-18-20' : 'dataset_sum_rec',
    '18-120-18-20' : 'proc_parm_rec',
    '18-20-18-20' : 'map_proj_rec',
    '18-30-18-20' : 'pos_data_rec',
    '18-40-18-20' : 'att_data_rec',
    '18-50-18-20' : 'radi_data_rec',
    '18-51-18-20' : 'radi_comp_rec',
    '18-60-18-20' : 'qual_sum_rec',
    '18-63-18-18' : 'text_rec',
    '18-70-18-20' : 'sdr_hist_rec',
    '192-192-18-18' : 'vol_desc_rec',
    '192-192-63-18' : 'null_vol_rec',
    '219-192-18-18' : 'file_pntr_rec',
    '50-10-18-20' : 'sdr_data_rec',
    '50-11-18-20' : 'pdr_data_rec',
    '63-192-18-18' : 'sar_desc_rec',
    }

asf_ceos_fields = [
    ('10-10-18-20','scene_id','A',16,21),
    ('10-10-18-20','inp_sctim','A',32,69), #time @start of scansar, center otherwise
    ('10-10-18-20','asc-des','A',16,101),
    ('10-10-18-20','ellip_des','A',16,165),
    ('10-10-18-20','ellip_maj','AF',16,181),
    ('10-10-18-20','ellip_min','AF',16,197),
    ('10-10-18-20','terrain_h','A',16,309),
    ('10-10-18-20','scene_len','AF',16,341),
    ('10-10-18-20','scene_wid','AF',16,357),
    ('10-10-18-20','mission_id','A',16,397),
    ('10-10-18-20','sensor_id','A',32,413),
    ('10-10-18-20','orbit_num','A',8,445),
    ('10-10-18-20','plat_lat','AF',8,453),
    ('10-10-18-20','plat_head','AF',8,469),
    ('10-10-18-20','wave_length','AF',16,501),
    ('10-10-18-20','fac_id','A',16,1047),
    ('10-10-18-20','prod_type','A',32,1111),
    ('10-10-18-20','n_azilok','AF',16,1175),
    ('10-10-18-20','n_rnglok','AF',16,1191),
    ('10-10-18-20','rng_res','AF',16,1351),
    ('10-10-18-20','azi_res','AF',16,1367),
    ('10-10-18-20','time_dir_pix','A',8,1527),
    ('10-10-18-20','time_dir_lin','A',8,1535),
    ('10-10-18-20','line_spacing','AF',16,1687),
    ('10-10-18-20','pix_spacing','AF',16,1703),
    ('10-10-18-20','clock_ang','AF',8,477),
    ('10-10-18-20','n_beams','AI',2,1767),
    ('10-10-18-20','beam_type1','A',4,1769),
    ('10-10-18-20','beam_type2','A',3,1775),
    ('10-10-18-20','beam_type3','A',3,1777),
    ('10-10-18-20','beam_type4','A',3,1781),
    ('10-10-18-20','centre_lat','A',16,117),
    ('10-10-18-20','centre_lon','A',16,133),
    ('10-10-18-20','state_time','A',32,69),
    ('10-210-18-61','near_start_lat','AF',16,157),
    ('10-210-18-61','near_start_lon','AF',16,174),
    ('10-210-18-61','near_end_lat','AF',16,191),
    ('10-210-18-61','near_end_lon','AF',16,208),
    ('10-210-18-61','far_start_lat','AF',16,225),
    ('10-210-18-61','far_start_lon','AF',16,242),
    ('10-210-18-61','far_end_lat','AF',16,259),
    ('10-210-18-61','far_end_lon','AF',16,276),
    ('10-50-18-20','table_des','A',24,37),
    ('10-50-18-20','n_samp','AI',8,61),
    ('10-50-18-20','noise_factor','AF',16,85),
    ('10-50-18-20','linear_conv_fact','AF',16,101),
    ('10-50-18-20','offset_conv_fact','AF',16,117),
    ('10-50-18-20','lookup_tab','AF',16,137,256)# coerce later
    ]

asf_ceos_records = {
    '10-210-18-61' : 'fac_data_rec',
    '10-80-18-20' : 'rng_spec_rec',
    '63-192-31-18' : 'fdr_rec',
    '10-20-18-20' : 'map_proj_rec',
    '10-30-18-20' : 'pos_data_rec',
    '10-40-18-20' : 'att_data_rec',
    '10-10-18-20' : 'dataset_sum_rec',
    '10-60-18-20' : 'qual_sum_rec',
    '10-70-18-20' : 'sdr_hist_rec'
    }

# define global variables
global gll
global glr
global gul
global gur

class Metadata(object):
    '''
    This is the metadata class for each image RSAT2, RSAT1 (ASF and CDPF)

    '''

    def __init__(self, granule, imgname, path, zipfile, sattype):
        """
            This initializes the class based on input

            :arg granule: unique name of the image
            :type granule: String
            :arg imgname: name of the file to open, representing the image
            :type imgname: String
            :arg path: path to the image
            :type path: String
            :arg sattype: type of data
            :type sattype: String

            :ret: An instance of Meta
        """
        self.path = path
        self.granule = granule  #filename and extension of the file containing
                                      # all original data for this scene
        self.location = zipfile  # Is this the same as granule and path?  
        self.dimgname = None
        self.imgname = imgname
        self.sattype = sattype
        self.notes = []
        self.dimgname = imgname   # update dimgname after accessing meta
        self.status = "ok"


        # now... image, describe thyself!
        result = self.getgdalmeta()     #result = None if test ok, otherwise its an error

        if (result == "gdalerror"):
            self.status = "gdalerror"
            return

        if self.sattype == 'ASF_CEOS' or self.sattype == 'CDPF':
            self.getCEOSmetafile()
            ceos_meta = self.get_ceos_metadata()

            if self.sattype == 'CDPF':
                result = self.clean_metaCDPF(ceos_meta)

                if (result == "gdalerror"):
                    self.status = "gdalerror"
                    return

                if self.getCornerPoints() == 'gcperror':
                    self.status = "gcperror"
                    return
            else:
                self.clean_metaASF(ceos_meta)
                pass

        elif self.sattype == 'RS2':
            self.getCornerPoints()
            self.getRS2metadata()
        else:
            print
            pass # trap error here?
            
            #TODO - test these for a variety of images Desc, Asc, R1 and 2
            #find the conditions where they seem to work (ie what side matches sat_heading in R1)
            #choose one way forward and modify tblMetadata to suit
        ul, ur, lr, ll = Util.wktpoly2pts(self.geom)
        self.az_ulll = Util.az(ul,ll)
        self.az_urlr = Util.az(ur,lr)
        self.az_llul = Util.az(ll,ul)
        self.az_lrur = Util.az(lr,ur)

    def getgdalmeta(self):
        """
            Open file with gdal and get metadata

            :ret: gdal_meta
        """
        if self.sattype == "ASF_CEOS":
            imgname = self.imgname + '.D'
        if self.sattype == "CDPF":
            imgname = self.imgname + '.img'
        if self.sattype == "RS2":
            imgname = 'product.xml'

	### register all drivers at once ... works for reading data but not for creating data sets
        gdal.AllRegister() # for all purposes
        fname = os.path.join(self.path, imgname)

	### now that the driver has been registered, use the stand-alone Open method to return a Dataset object
        ds = gdal.Open(fname, GA_ReadOnly)

        #TODO, more info here

        if(ds == None):
            print "Error with gdal.open"
            self.status = "gdalerror"
            return "gdalerror"


        ### getting image dimensions
        self.n_rows = ds.RasterYSize
        self.n_cols = ds.RasterXSize
        self.n_bands = ds.RasterCount

        print 'Img has:      ' + str(self.n_cols) + ' columns, ' + \
        str(self.n_rows) + ' rows & ' + str(self.n_bands) + ' band(s)'      ###


        #If the n_rows is not zero extract the GCPs through the dataset
        if(self.n_rows > 0):
            self.geopts = ds.GetGCPs()              ### Getting the 15 GCPs
            self.n_geopts = ds.GetGCPCount()

        #Otherwise extract them manually through the .img file
        else:
            print "No GCPs in gdal dataset, manually extracting from the image."
            self.getCEOSmetafile()
            ### Take every nth line (specify nth) and save GCPs
            self.geopts = self.extractGCPs(50)          ### In this case nth = 50

        self.getCEOSmetafile()
        if self.sattype == "CDPF":
            self.geopts = self.extractGCPs(50)


        self.geoptsGCS = ds.GetGCPProjection()
        proj = ds.GetProjectionRef()

        if proj == '':
            print 'Img has:      ' + str(self.n_geopts) + ' GCPs'
        else:
            print 'Projection is:    ' + proj

        # All gdal_meta
        gdal_meta = ds.GetMetadata_List()
        #print gdal_meta

        ds = None

        return gdal_meta


    def getCornerPoints(self):
        """
        Given a set of geopts, calculate the corner coords to the nearest 1/2
        pixel. Assumes that the corners are among the GCPs (not randomly placed)
        """

        x = numpy.zeros((len(self.geopts), 1), dtype=numpy.float64)
        y = numpy.zeros((len(self.geopts), 1), dtype=numpy.float64)
        z = numpy.zeros((len(self.geopts), 1), dtype=numpy.float64)
        row = numpy.zeros((len(self.geopts), 1), dtype=numpy.float64)
        col = numpy.zeros((len(self.geopts), 1), dtype=numpy.float64)

    # won't work with homemade geopt
#        for i, geopt in enumerate(self.geopts):
#            x[i] = geopt.GCPX
#            y[i] = geopt.GCPY
#            z[i] = geopt.GCPZ
#            row[i]= geopt.GCPLine
#            col[i] = geopt.GCPPixel

        for i, geopt in enumerate(self.geopts):
            x[i] = gdal.GDAL_GCP_GCPX_get(geopt)
            y[i] = gdal.GDAL_GCP_GCPY_get(geopt)
            z[i] = gdal.GDAL_GCP_GCPZ_get(geopt)
            row[i]= gdal.GDAL_GCP_GCPLine_get(geopt)
            col[i] = gdal.GDAL_GCP_GCPPixel_get(geopt)

        #Todo -- Check for no GCPs
        if(len(row) == 0):
            print "No rows found in "
            #sys.exit(0)
            return "gdalerror"

        # check to see if the gcps cover the corners
        if self.sattype == 'RS2': # these ones are not in the middle of the pixel
            offset = 0.5
        else:
            offset = 0

        assert row.max()+0.5 == self.n_rows-offset + 0.0
        assert row.min()+0.5 == 1-offset + 0.0
        assert col.max()+0.5 == self.n_cols-offset + 0.0
        assert col.min()+0.5 == 1-offset + 0.0


        uppermost = row == row.min()
        lowermost = row == row.max()
        leftmost = col == col.min()
        rightmost = col == col.max()

        sep = ' '
        ul = str(x[uppermost & leftmost][0]) +sep+ str(y[uppermost & leftmost][0])
        ur = str(x[uppermost & rightmost][0]) +sep+ str(y[uppermost & rightmost][0])
        lr = str(x[lowermost & rightmost][0]) +sep+ str(y[lowermost & rightmost][0])
        ll = str(x[lowermost & leftmost][0]) +sep+ str(y[lowermost & leftmost][0])

        ########################
        gul = ul
        gur = ur
        glr = lr
        gll = ll
        ########################
        '''
        #TODO - determine once and for all if this is correct or not - for now disabled...
        #If Rsat 1 and Decending flip the corner points
        if  self.sattype != 'RS2' and self.passDirection == "Descending":        ###
            print "Pass direction is descending flipping corners and GCPs"
            """FLIP GCP AROUND THE Y-AXIS"""
            ulX = ul[:ul.find(' ',0,len(ul))]
            urX = ur[:ur.find(' ',0,len(ur))]

            #Find the x-coordiate of the centre of the image
            centre = (float(urX) + float(ulX))/2

            for gcp in self.geopts:
                #For each point flip about the Y axis but moving the X coordinate according to newX = center + (center - oldX)
                gcp.GCPX = centre + (centre - gcp.GCPX)

            print "ul: ", ul
            print "ur: ", ur
            print "ll: ", ll
            print "lr: ", lr

            #Flip the corner points
            ul, ur = ur, ul
            ll, lr = lr, ll

            print "ul: ", ul
            print "ur: ", ur
            print "ll: ", ll
            print "lr: ", lr
        '''

        #make sure we have the right spatial coord syst.
        wgs84_srs = osr.SpatialReference()
        wgs84_srs.SetWellKnownGeogCS( "WGS84" )

        gcp_srs = osr.SpatialReference()
        gcp_srs.ImportFromWkt(self.geoptsGCS)

        #If the GCP projection could not be extracted compare the major and minorellip -- are within 1 of expected
        if(self.geoptsGCS == ''):
            if abs(self.ellip_maj - 6378.137) < 1 and abs(self.ellip_min - 6356.7523142) < 1:
                self.geoptsSRID = 4326
            else:
                print 'ERROR: The image GCPs were in an unexpected GCS'

                return 'gcperror'
        elif not wgs84_srs.IsSame(gcp_srs):
            # ok that's not cool, but maybe the projections are the same anyhow...
             #...let's test...
            trans=osr.CoordinateTransformation(gcp_srs, wgs84_srs)
            test = 0.0
            counter = 0
            test_reproj = trans.TransformPoint(test,test,test)
            for coord in test_reproj:
                if coord == test:
                    counter += 1
            if counter == 3:  #then tranformation is *1 (no transform)
                self.geoptsSRID = 4326 # the SRID for WGS84
            else:
                print 'ERROR: The image GCPs were in an unexpected GCS'
                return 'gcperror'
                # at this stage, you could transform the corner coords to wgs84
                # and then continue... (note that this was done -below)
                #self.notes.append('The GCPs had to be transformed to WGS84 GCS)
        else:
            self.geoptsSRID = 4326 # the SRID for WGS84

        self.geom = 'POLYGON(('+ul+', '+ur+', '+lr+', '+ll+', '+ul+'))'

    def getMoreGCPs(self, n_gcps):
        """
        If you have a CDPF RSat1 image, gdal only has 15 GCPs
        Perhaps you want more?  If so, use this function.
        It will grab all the GCPs available (3 on each line) and
        subselect n_gcps of these to return.

        The GCPs will not necessarily be on the 'bottom corners' since the gcps
        will be spaced evenly to get n_gcps (or more if not divisible by 3)
        If you want corners the only way to guarentee this is to set n_gcps = 6
        """

        assert self.sattype == 'CDPF', 'You can only use this function with CDPF R1 data'

        #cycle through record to 50-11-18-20
        fp = open(os.path.join(self.path, self.imgname + '.img'), mode='rb') # must read binary!
        # Get file size
        fp.seek(0,2)
        file_size = fp.tell()

        first = []
        mid = []
        last = []

        # Search the file
        record_offset = 0

        while record_offset < file_size:

            fp.seek(record_offset,0)
            header_data = fp.read(12)

            # CEOS data is Big Endian, here is the code to read it:
                # header one long int, four unsigned bytes, one long int
            seq, sub1, type, sub2, sub3, record_length = struct.unpack('>lBBBBl', header_data)

            #print 'rec_seq ' + str(seq)
            record_key = str(sub1) +'-'+ str(type) +'-'+ str(sub2) +'-'+ str(sub3)

            if record_key == '50-11-18-20':
                #print 'line_number ' + str(struct.unpack('>l', fp.read(4))[0])

                fp.seek(record_offset+132,0) # go to start of coord data
                coords_bin = fp.read(24)
                coords_int = struct.unpack('>llllll', coords_bin)

                first.append((coords_int[3]/1e6, coords_int[0]/1e6))
                mid.append((coords_int[4]/1e6, coords_int[1]/1e6))
                last.append((coords_int[5]/1e6, coords_int[2]/1e6))

            record_offset = record_offset + record_length


        #ok so now you have the coords, but we don't want them all

        # make sure the n_gcps are a multiple of 3 (if not round up!)
        if n_gcps % 3 !=0:
            n_gcps = n_gcps + 3- n_gcps % 3

        # determine the pixel/line of each gcp
        interval = int( (self.n_rows-1) / (n_gcps/3.0 - 1) ) # gives the interval
        lines = range(0, self.n_rows, interval)
        pixels = [0.5, self.n_cols/2, self.n_cols - 0.5]
        gcps = []

        for i, line in enumerate(lines):
            for j, pixel in enumerate(pixels):
                img = (pixel, line+0.5)
                if pixel == 0.5:
                    coord = first[line]
                elif pixel == self.n_cols/2:
                    coord = mid[line]
                elif pixel == self.n_cols -0.5:
                    coord = last[line]
                ##print img, coord, str(i)+'_'+str(j)
                # put into a GCP
                gcp = gdal.GCP()
                gcp.Id = str(i)+'_'+str(j)
                gcp.Info = 'info'
                gcp.GCPX = coord[0]
                gcp.GCPY = coord[1]
                gcp.GCPZ = 0.0
                gcp.GCPPixel = img[0]
                gcp.GCPLine = img[1]

                gcps.append(gcp)
                #gdal.GDAL_GCP_GCPX_get(gcp) # find out what made it into the gcp

        self.geopts = tuple(gcps)
        self.n_geopts = n_gcps
        return tuple(gcps)

    def getCEOSmetafile(self):
        """
        Get the filenames for metadata
        """

        if self.sattype == "ASF_CEOS":
            self.metafile = [os.path.join(self.path, self.imgname) + '.L']
        if self.sattype == "CDPF":
            fname = os.path.join(self.path, self.imgname)
            leader = fname + ".led"
            trailer = fname + ".trl"
            self.image = fname + ".img"
            self.metafile = [leader,trailer] # image hangs get_ceos_meta
            #self.allfiles = [leader,trailer,image];
            #return [leader,trailer]

    def saveMetaFile(self, dir=''):
        """
        Makes a text file with the metadata
        
        """
        #TODO: MAKE THIS AN XML FILE
        meta = self.createMetaDict()
        fout = open(os.path.join(dir, self.dimgname+".met"), 'w')

        for field, value in sorted(meta.iteritems()):
                    fout.write(field +'\t'+ str(value))
                    fout.write('\n')

        fout.close()
#        doc = xml.dom.minidom.Document()
#        dimgname = doc.createElementNS(self.dimgname, "dimgname")

    
    def createMetaDict(self):   
        """
        Creates a dictionary of all the metadata fields for an image
        this can be written to file or sent to database

        Note that the long boring metadata fields are not included
        """

        #make a dictionary of all the meta.attributes
        metaDict = {}
        for attr, value in self.__dict__.iteritems():
            ###tblmetadata is filled with this dict

            if attr != 'geopts' and attr != 'noise' and attr != 'calgain' and \
                        attr != 'theta':
                try:
                    if type(value) == type(self.calgain):
                        metaDict[attr] = value.tolist()
                    else:
                        metaDict[attr] = value
                except AttributeError:
                    metaDict[attr] = value
        return metaDict


    def get_ceos_metadata(self, *file_names):
        """
        Take file names as input and return a dictionary of metadata
        file_names is a list or strings or a string (with one filename)
        
        This source code to extract metadata from CEOS-format RADARSAT-1 
        data was developed by Defence Research and Development Canada
        [Used with Permission]
        """

        # assert - only ASF_CEOS or CDPF
        if self.sattype == 'ASF_CEOS':
            rsat_fields = asf_ceos_fields
            rsat_records = asf_ceos_records

        if self.sattype == 'CDPF':
            rsat_fields = rsat_cdpf_fields
            rsat_records = rsat_cdpf_records

        if len(file_names) == 0:
            file_names = self.metafile

#        file_names.append =''

#        if len(file_names) == 1:
#                file_names = [file_names[0]
#        else:
#                file_names = (file_names,)
        # Build record index

        record_index = {}
        for field in rsat_fields:
            if record_index.has_key(field[0]):
                record_index[field[0]].append(field)
            else:
                record_index[field[0]] = [field]


        # Extract from files
        result = {}
        for file_name in file_names:
            ###print
            #print 'Looking in:   ',file_name,"..."   ###

            fp = open(file_name)


            # Get file size
            fp.seek(0,2)
            file_size = fp.tell()

            # Search the file
            record_offset = 0

            while record_offset < file_size:

                fp.seek(record_offset,0)
                header_data = fp.read(12)
                #print 'rec_seq ' + str(byte2int(header_data[0:4]))

                #after reading 12 bytes, get the record identifier
                record_key = str(byte2int(header_data[4])) + '-' + \
                    str(byte2int(header_data[5])) + '-' + \
                    str(byte2int(header_data[6])) + '-' + \
                    str(byte2int(header_data[7]))

                # read the record length from CEOS data
                record_length = byte2int(header_data[8:12])
                if rsat_records.has_key(record_key):
                    record_name = rsat_records[record_key]
                else:
                    record_name = "Unknown Record"


                #print "Record Header: " + record_key + ", Record Name: " \
                #    + record_name + " Sequence "+  str(byte2int(header_data[0:4]))+\
                #    ", Length: " + str(record_length) + " bytes"


                # if we are looking for this record, then get data in record
                if record_index.has_key(record_key):

                    #print "looking at record key: ", record_key

                    # read in the data in native format
                    record_data = get_data_block(fp, record_offset, record_length)
                    #


                    #print "record data is ", record_data

                    # iterate through fields that are found in this record
                    for field in record_index[record_key]:
                        # determine if the field has repeat values (not a single value)
                        if len(field)>5:
                            field_result = [] #result will be a list
                            # the repeat values info could be in a tuple
                            if type(field[5]) == type(()):
                                #then get the field type, size and offest
                                count = get_field_value(record_data,field[5][0],field[5][1],field[5][2]-1)
                            # the repeat value info is an integer (how many repeats)
                            else:
                                count = field[5]
                            # get the values x the repeat number
                            for i in range(count):
                                field_result.append(get_field_value(record_data,field[2],field[3],field[4]+i*field[3]-1))

                        # if the values don't repeat - single values
                        else:
                            field_result = get_field_value(record_data,field[2],field[3],field[4]-1)

                        result[field[1]] = field_result  # add data to result dictionary

                record_offset = record_offset + record_length

        #print "the returned result is: ", result

        return result

    def extractGCPs(self, interval):

        file_name = self.image
        #print 'Looking in:   ',file_name,"..."       ###
        fp = open(file_name)


        # Get file size
        fp.seek(0,2)                ### Changes the current file position (records the number of bytes from 0 to 2)
        file_size = fp.tell()

        first = []
        mid = []
        last = []

        line_num =-1;
        n_gcps = 0
        record_offset = 0

        # Search the file
        while record_offset < file_size:

            #Increase the number of lines
            line_num = line_num + 1;

            fp.seek(record_offset,0)
            header_data = fp.read(12)

            # CEOS data is Big Endian, here is the code to read it:
                # header one long int, four unsigned bytes, one long int
            seq, sub1, type, sub2, sub3, record_length = struct.unpack('>lBBBBl', header_data)

            #print 'rec_seq ' + str(seq)         ###
            record_key = str(sub1) +'-'+ str(type) +'-'+ str(sub2) +'-'+ str(sub3)

            if record_key == '50-11-18-20':
                #print 'line_number ' + str(struct.unpack('>l', fp.read(4))[0])     ###

                fp.seek(record_offset+132,0) # go to start of coord data
                coords_bin = fp.read(24)

                coords_int = struct.unpack('>llllll', coords_bin)

                first.append((coords_int[3]/1e6, coords_int[0]/1e6))
                mid.append((coords_int[4]/1e6, coords_int[1]/1e6))
                last.append((coords_int[5]/1e6, coords_int[2]/1e6))


            record_offset = (record_offset) + record_length

        lines = range(0, line_num, interval)        ### Take every nth line (nth = interval)

        # Make sure to extract the pixels from the last line
        if line_num - 1 not in lines:
            lines.append(line_num - 1)

        #print "lines ", lines           ###

        pixels = [0.5, self.n_cols/2, self.n_cols - 0.5]

        gcps = []

        for i, line in enumerate(lines):            ### Iterate lines[]
            for j, pixel in enumerate(pixels):      ### Iterate pixels[]
                img = (pixel, line+0.5)
                if pixel == 0.5:
                    coord = first[line]
                elif pixel == self.n_cols/2:
                    coord = mid[line]
                elif pixel == self.n_cols -0.5:
                    coord = last[line]

                #print img, coord, str(i)+'_'+str(j)

                # Put into a GCP
                gcp = gdal.GCP()
                gcp.Id = str(i)+'_'+str(j)
                gcp.Info = 'info'
                gcp.GCPX = coord[0]
                gcp.GCPY = coord[1]
                gcp.GCPZ = 0.0
                gcp.GCPPixel = img[0]
                gcp.GCPLine = img[1]
                gcps.append(gcp)

                n_gcps = n_gcps + 1

                #gdal.GDAL_GCP_GCPX_get(gcp) # find out what made it into the gcp






        self.geopts = tuple(gcps)
        self.n_geopts = n_gcps
        self.n_rows = line_num

        return tuple(gcps)




    def getRS2metadata(self):
        """
        Open a Radarsat2 file and get all the required metadata
        """

        file = os.path.join(self.path, "product.xml")

        xmldoc = minidom.parse(os.path.join(self.path, file))
        #print xmldoc.toxml()

        # get all the 1:1 data first

        '''Changed to 5 characters'''
        self.beam = xmldoc.getElementsByTagName('beamModeMnemonic')[0].firstChild.data
        self.beam = self.beam + '_____'  # this will pad the beam name
        self.beam = self.beam[0:5] # keep this to 5 chars

        self.beams = xmldoc.getElementsByTagName('beams')[0].firstChild.data
        self.n_beams = len(self.beams.split())
        self.polarization = xmldoc.getElementsByTagName('polarizations')[0].firstChild.data
        self.freqSAR = float(xmldoc.getElementsByTagName('radarCenterFrequency')[0].firstChild.data)
        self.acDateTime = xmldoc.getElementsByTagName('rawDataStartTime')[0].firstChild.data
        self.acDateTime = readdate(self.acDateTime, self.sattype)
        self.acDOY = date2doy(self.acDateTime, float=True)

        self.antennaPointing = xmldoc.getElementsByTagName('antennaPointing')[0].firstChild.data
        self.passDirection = xmldoc.getElementsByTagName('passDirection')[0].firstChild.data
        self.processingFacility = xmldoc.getElementsByTagName('processingFacility')[0].firstChild.data
        self.lutApplied = xmldoc.getElementsByTagName('lutApplied')[0].firstChild.data
        self.looks_Rg = int(xmldoc.getElementsByTagName('numberOfRangeLooks')[0].firstChild.data)
        self.looks_Az = int(xmldoc.getElementsByTagName('numberOfAzimuthLooks')[0].firstChild.data)

        self.bitsPerSample = int(xmldoc.getElementsByTagName('bitsPerSample')[0].firstChild.data)
        self.n_cols = int(xmldoc.getElementsByTagName('numberOfSamplesPerLine')[0].firstChild.data)
        self.n_rows = int(xmldoc.getElementsByTagName('numberOfLines')[0].firstChild.data)
        self.pixelSpacing = float(xmldoc.getElementsByTagName('sampledPixelSpacing')[0].firstChild.data)
        self.lineSpacing = float(xmldoc.getElementsByTagName('sampledLineSpacing')[0].firstChild.data)

        self.theta_near = xmldoc.getElementsByTagName('incidenceAngleNearRange')[0].firstChild.data
        self.theta_far = xmldoc.getElementsByTagName('incidenceAngleFarRange')[0].firstChild.data

        self.orbit = xmldoc.getElementsByTagName('orbitDataFile')[0].firstChild.data
        self.orbit, tmp = self.orbit.split('_')
        self.orbit = int(self.orbit)
        self.sat_heading = None # get this later?
        self.satellite = 'Radarsat-2'
        self.copyright = xmldoc.getElementsByTagName('product')[0].attributes["copyright"].value

        #INCIDENCE ANGLE (THETA)
        # get the ground to slant range inputs (from first set of data only)
        sr2gr = xmldoc.getElementsByTagName('slantRangeToGroundRange')[0]
        gr0 = float(sr2gr.getElementsByTagName('groundRangeOrigin')[0].firstChild.data)
        gsr = sr2gr.getElementsByTagName('groundToSlantRangeCoefficients')[0].firstChild.data
        gsr = gsr.split()
        for i in range(len(gsr)):
                gsr[i] = float(gsr[i])

        self.order_Az = xmldoc.getElementsByTagName('lineTimeOrdering')[0].firstChild.data
        self.order_Rg = xmldoc.getElementsByTagName('pixelTimeOrdering')[0].firstChild.data

        #check that the gsr will be ok for the entire scene (valid for about 2 min)
        start = xmldoc.getElementsByTagName('zeroDopplerTimeFirstLine')[0].firstChild.data
        stop = xmldoc.getElementsByTagName('zeroDopplerTimeLastLine')[0].firstChild.data
        if self.order_Az.lower() == 'decreasing':
            duration = readdate(start,self.sattype)-readdate(stop,self.sattype)
        elif self.order_Az.lower() == 'increasing':
            duration = readdate(stop,self.sattype)-readdate(start,self.sattype)
        else:
            print 'No line order found'
            return

        if duration.seconds > 120:
            self.notes.append('Warning SRGR coefficient issue, image duration is '+ str(duration.seconds))


        slantRange = getSlantRange(gsr, self.pixelSpacing, self.n_cols, self.order_Rg, gr0)

        ellip_maj = float(xmldoc.getElementsByTagName('semiMajorAxis')[0].firstChild.data)
        ellip_min = float(xmldoc.getElementsByTagName('semiMinorAxis')[0].firstChild.data)
        sat_alt = float(xmldoc.getElementsByTagName('satelliteHeight')[0].firstChild.data)
        plat_lat = float(xmldoc.getElementsByTagName('latitudeOffset')[0].firstChild.data)

        radius = getEarthRadius(ellip_maj, ellip_min, plat_lat)

        self.theta = getThetaVector(self.n_cols, slantRange, radius, sat_alt)*R2D # now in degrees

        self.productType = xmldoc.getElementsByTagName('productType')[0].firstChild.data
        if self.productType == 'SLC': # then you might want groundRange
            groundRange = getGroundRange(slantRange, radius, sat_alt)

        #NOISE VECTOR
        noise = xmldoc.getElementsByTagName('referenceNoiseLevel')
        for i in range(len(noise)):
            test = noise[i].attributes["incidenceAngleCorrection"].value
            if test == "Sigma Nought":
                break    # break # preserve i as the index you want
        FirstNoisePixel = int(noise[1].getElementsByTagName('pixelFirstNoiseValue')[0].firstChild.data)
        stepSize = int(noise[1].getElementsByTagName('stepSize')[0].firstChild.data)
        n_noise = int(noise[1].getElementsByTagName('numberOfNoiseLevelValues')[0].firstChild.data)
        noiseLevelValues = noise[1].getElementsByTagName('noiseLevelValues')[0].firstChild.data
        noiseList = noiseLevelValues.split()  #make a list
        noiseList = [float(n) for n in noiseList]
        
        nx = [npixel*stepSize+ FirstNoisePixel for npixel in range(n_noise)] #pixel numbers zero-based
        cubicspline = interpolate.splrep(nx, noiseList, s=0)  #cubic spline, no smoothing
        interpnoise = interpolate.splev(range(self.n_cols), cubicspline)  
        #no extrapolation before first pixel or after last pixel in noiseList
        interpnoise[0:FirstNoisePixel] = noiseList[0]
        interpnoise[nx[-1]:] = noiseList[-1]
        self.noise = Util.getPowerScale(interpnoise)
       
        if self.order_Rg.lower() == 'decreasing':
                self.noise = self.noise[::-1].copy() # REVERSE!!
      
        # done, clear file
        xmldoc.unlink()

        # get lut
        file = 'lutSigma.xml'
        if os.path.isfile(os.path.join(self.path, file)):
        # read in LUT
            xmldoc = minidom.parse(os.path.join(self.path, file))
        else:
            print "lutSigma.xml cannot be found"
            #error handler

        self.caloffset = float(xmldoc.getElementsByTagName('offset')[0].firstChild.data)
        gains = xmldoc.getElementsByTagName('gains')[0].firstChild.data
        gains = gains.split()
        calgain = numpy.zeros(len(gains), numpy.float32)
        for i in range(len(gains)):
            calgain[i] = float(gains[i])
        self.calgain = calgain


        #print xmldoc.toxml()

        if self.order_Rg.lower() == 'decreasing':
                self.calgain = self.calgain[::-1].copy() # REVERSE!!

        xmldoc.unlink()
        self.getDimgname()



    def clean_metaCDPF(self, result):
        #ONLY CDPF
        """
        Takes meta data from origmeta and checks it for completeness, coerces data types
        splits values, if required and puts it all into a standard format
        """

        if result.has_key('act_ing_start'): #try this first
            self.acDateTime = readdate(result['act_ing_start'], self.sattype)
        elif result.has_key('state_time'):
            self.acDateTime = readdate(result['state_time'], self.sattype)
        else:
            print 'No date field retrieved in metadata'
            #error handler

       ### print "result...:", result

        self.satellite = result['sensor_id'][0:6]
        # scen_id describes the product type an not the beam mode
        ###self.beam = result['scene_id'].strip()
        ###self.beam = self.beam[-3:len(result['scene_id'])]

        self.n_bands = 1  # since we have HH
        self.lineSpacing = result['line_spacing']
        self.pixelSpacing = result['pix_spacing']
        assert self.lineSpacing > 0 and type(self.lineSpacing) == type(1.2)
        assert self.pixelSpacing > 0 and type(self.pixelSpacing) == type(1.2)

        self.beams = result['beam_type1']+result['beam_type2']+result['beam_type3']+result['beam_type4']
        self.beams = self.beams.strip()

        # deriving ScanSAR Narrow (SNB) beam mode
        if result['beam_type1'].strip() == 'W2'and result['beam_type2'].strip() == 'S5' and result['beam_type3'].strip() == 'S6':
            self.beam = "SNB"

        # deriving ScanSAR Narrow (SNA) and Wide (SWA, SWB) beam modes
        if result['beam_type1'].strip() == 'W1'and result['beam_type2'].strip() == 'W2':
            if result['beam_type3'].strip() == '' and result['beam_type4'].strip() == '':
                self.beam = "SNA"
            elif result['beam_type3'].strip() == 'W3' and result['beam_type4'].strip() == 'S7':
                self.beam = "SWA"
            elif result['beam_type3'].strip() == 'S5' and result['beam_type4'].strip() == 'S6':
                self.beam = "SWB"

        # deriving Extended Low (EXL) beam mode
        if result['beam_type1'].strip() == 'EL1':
            self.beam = "EXTDL"
        # deriving Extended High (EXH) beam mode
        if 'EH' in result['beam_type1'].strip():
            self.beam = "EXTDH"
        if 'F' in result['beam_type1'].strip():
            self.beam = "FINE"
        if 'W' in result['beam_type1'].strip() and result['beam_type2'].strip() == '':
            self.beam = "WIDE"
        if 'S' in result['beam_type1'].strip() and result['beam_type2'].strip() == '':
            self.beam = "STND"


        self.beam = self.beam + '_____'  # this will pad the beam name
        self.beam = self.beam[0:5] # keep this to 5 chars

        if result.has_key('nbit'):
            self.bitsPerSample = result['nbit']
        else:
            self.bitsPerSample = 8  # A BIG ASSUMPTION HERE!!! COULD BE TROUBLE

        self.copyright = 'Copyright CSA ' + self.acDateTime.strftime('%Y')
        self.freqSAR = 0.29979e9/result['wave_length'] # in Hz
        self.looks_Az = result['n_azilok']
        self.looks_Rg = result['n_rnglok']
        self.n_beams = result['n_beams']
        self.NominalResAz = result['azi_res']
        self.NominalResRg = result['rng_res']
        self.processingFacility = result['fac_id'].strip()
        self.SwathWidthAz = result['scene_len']
        self.SwathWidthRg = result['scene_wid']

        self.acDOY = date2doy(self.acDateTime)
        self.orbit = result['orbit_num'].strip()
        self.polarization = result['sensor_id'].strip()[-2:len(result['sensor_id'])]
        self.sat_heading = result['plat_head']
        if result['clock_ang'] == 90:
            self.antennaPointing = 'Right'
        elif result['clock_ang'] == -90:
            self.antennaPointing = 'Left'
        else:
            self.antennaPointing = None

        self.passDirection = result['asc-des'].capitalize().strip()

        #sometimes eph_orb_data is missing...
        if result['eph_orb_data'] == None: #quick check to see
            result['eph_orb_data'] = 7.167055e6 # in metres
        sat_alt = result['eph_orb_data'] - getEarthRadius(result['ellip_maj'], \
                result['ellip_min'], result['plat_lat'])

        #Save these to check the projections
        self.ellip_maj = result['ellip_maj']
        self.ellip_min = result['ellip_min']


        self.lutApplied = None
        self.order_Az = result["time_dir_lin"]
        self.order_Rg = result["time_dir_pix"]

        #Product type too long at the moment so default to none
        self.productType = None
        self.theta_far = None
        self.theta_near = None
        self.theta = ()
        self.noise = ()
        self.calgain = ()
        self.getDimgname()

    #Too long at the moment
    def getASFProductType(self, ASFName):
        cmd = "metadata " + "-dssr " + ASFName
        command = shlex.split(cmd)

        ok = subprocess.Popen(command, stdout = subprocess.PIPE)


        for line in ok.stdout:
            if " PRODUCT TYPE" in line:
                self.productType = line[15:]


    def getASFMetaCorners(self, ASFName):

        #use ASF Mapready to generate the metadata


        cmd = "metadata " + "-asf_facdr " + ASFName
        command = shlex.split(cmd)

        ok = subprocess.Popen(command, stdout = subprocess.PIPE)


        for line in ok.stdout:
            if "Lat at start of image frame in near swath" in line:
                ulLat = line[45:]
            elif "Long at start of image frame in near swath" in line:
                ulLong = line[46:]
            elif "Lat at end of image frame in near swath" in line:
                urLat = line[43:]
            elif "Long at end of image frame in near swath" in line:
                urLong = line[44:]
            elif "Lat at start of image frame in far swath" in line:
                llLat = line[44:]
            elif "Long at start of image frame in far swath" in line:
                llLong = line[45:]
            elif "Lat at end of image frame in far swath" in line:
                lrLat = line[42:]
            elif "Long at end of image frame in far swath" in line:
                lrLong = line[43:]
            elif "Incidence angle at the center of the image" in line:
                self.theta_near = line[27:]
                self.theta_far = line[27:]

        sep = ' '

        ul = ulLat.strip() + sep + ulLong.strip()
        ur = urLat.strip() + sep + urLong.strip()
        ll = llLat.strip() + sep + llLong.strip()
        lr = lrLat.strip() + sep + lrLong.strip()

        ###

        #If Rsat 1 and Decending flip the corner points
        if self.passDirection == "Descending" and self.sattype != 'RS2':
            print "Pass direction is descending flipping corners and GCPs"
            """FLIP GCP AROUND THE Y-AXIS"""
            ulX = ul[:ul.find(' ',0,len(ul))]
            urX = ur[:ur.find(' ',0,len(ur))]

            #Find the x-coordiate of the centre of the image
            centre = (float(urX) + float(ulX))/2

            for gcp in self.geopts:
                #For each point flip about the Y axis but moving the X coordinate according to newX = center + (center - oldX)
                gcp.GCPX = centre + (centre - gcp.GCPX)

            #Flip the corner points
            ul, ur = ur, ul
            ll, lr = lr, ll


        self.geom = 'POLYGON(('+ul+', '+ur+', '+lr+', '+ll+', '+ul+'))'

        #ONLY ASF
    def clean_metaASF(self, result):
        """
        Takes meta data from origmeta and checks it for completeness, coerces data types
        splits values, if required and puts it all into a standard format

        NOT TESTED!!

        """

        #Get the pass direction
        self.passDirection = result['asc-des'].capitalize().strip()


        self.getASFMetaCorners(self.metafile[0])
        self.getASFProductType(self.metafile[0])

        #Set the 4326 for the moment
        self.geoptsSRID = 4326

        self.lutApplied = None
        self.order_Az = result["time_dir_lin"]
        self.order_Rg = result["time_dir_pix"]

        #Product type too long at the moment so default to none
        self.productType = None




        self.theta_far = None
        self.theta_near = None


        if result.has_key('inp_sctim'):
            self.acDateTime = readdate(result['inp_sctim'], self.sattype)
        else:
            print 'No date field retrieved in metadata'
            #error handler

        #Add the satellite type
        self.satellite = result['sensor_id'][0:6]

        if result['prod_type'] == 'SCANSAR':
            self.beam = 'scn'
        else:
            self.beam = result['beam_type1'].strip().lower()

        self.n_bands = 1  # since we have HH
        self.lineSpacing = result['line_spacing']
        self.pixelSpacing = result['pix_spacing']
        assert self.lineSpacing > 0 and type(self.lineSpacing) == type(1.2)
        assert self.pixelSpacing > 0 and type(self.pixelSpacing) == type(1.2)

        self.beams = result['beam_type1']+result['beam_type2']+result['beam_type3']+result['beam_type4']
        self.beams = self.beams.strip()

        if result.has_key('nbit'):
            self.bitsPerSample = result['nbit']
        else:
            self.bitsPerSample = None

        self.copyright = 'Copyright CSA ' + self.acDateTime.strftime('%Y')
        self.freqSAR = 0.29979e9/result['wave_length'] # in Hz
        self.looks_Az = result['n_azilok']
        self.looks_Rg = result['n_rnglok']
        self.n_beams = result['n_beams']
        self.NominalResAz = result['azi_res']
        self.NominalResRg = result['rng_res']
        self.processingFacility = result['fac_id'].strip()
        self.SwathWidthAz = result['scene_len']
        self.SwathWidthRg = result['scene_wid']

        self.acDOY = date2doy(self.acDateTime)
        self.orbit = result['orbit_num'].strip()
        self.polarization = result['sensor_id'].strip()[-2:len(result['sensor_id'])]
        self.sat_heading = result['plat_head']

        if result['clock_ang'] == 90:
            self.antennaPointing = 'Right'
        elif result['clock_ang'] == -90:
            self.antennaPointing = 'Left'
        else:
            self.antennaPointing = None



        #Get Noise vector

        noise = result['lookup_tab']

        #ASF Noise floor is value in lookup table * the noise_fact

        #To calibrate ASF data - need Linear_conversion_factor * DN^2


        #for i in range(len(noise)):
        #Cannot determine Ground-to-Slant Range here, therefore...
        #Cannot determine incidence angle.

        self.getDimgname()

    def getDimgname(self):
        """
        Create a filename that conforms to my own standard naming convention:
            yyyymmdd_HHmmss_sat_beam_pol...
        """

        sep = "_"
        if 'HH' in self.polarization and 'VV' in self.polarization:
            pol = 'qp'
        elif 'HH' in self.polarization:
            pol = 'hh'
            if 'HV'in self.polarization:
                pol = 'hx'
        elif 'VV' in self.polarization:
            pol = 'vv'
            if 'VH'in self.polarization:
                pol = 'vx'
        else:  #could be just hv or vh... or something I never considered...
            pol = self.polarization.lower()

        if self.sattype == 'RS2':
            sat = "r2"
        if self.sattype == 'ASF_CEOS' or self.sattype == 'CDPF':
            if self.satellite == "ERS-1-":
                sat = 'e1'
            if self.satellite == "RSAT-1":
                sat = "r1"
            if self.satellite == "JERS-1":
                sat = "j1"

        # NB: the time is truncated here NOT rounded to second

        self.dimgname = self.acDateTime.strftime('%Y%m%d_%H%M%S')+ sep + \
             sat + sep + self.beam.lower() + sep + pol
             ###sat + sep + self.beam.lower() + sep + self.beams.lower().replace(" ", "_") + sep + pol


        return self.dimgname

def byte2int(byte):
    """
    Reads a byte and converts to integer
    """
    return int(binascii.b2a_hex(byte),16)

def get_data_block(fp, offset, length):
    """
    gets a block of data from file
    """

    fp.seek(offset,0)  # remember this is absolute pos.
    return  fp.read(length)

def get_field_value(data, field_type, length, offset):
    #data_str = string.strip(data[offset:offset+length])
    data_str = data[offset:offset+length]
    if data_str == '':
        return ''

    if field_type == 'A':
        # values are straight ASCII
        return data_str

    if field_type == 'AI':
        # values are ASCII integers
        return int(data_str)

    if field_type == 'AF':
        # values are ASCII float
        return float(data_str)

    if field_type == "BI":
        #values are binary integers
        return byte2int(data_str)


def readdate(date, sattype):
    """
    Takes a rsat2 formated date 2009-05-31T14:43:17.184550Z
    and converts it to python datetime
    """

    if sattype == 'RS2':
        datepart = [date[0:4], date[5:7], date[8:10], date[11:13], date[14:16], date[17:19], date[20:-1]]
        for i in range(len(datepart)):
            datepart[i] = int(datepart[i])
        return datetime.datetime(datepart[0], datepart[1], datepart[2], datepart[3], datepart[4], datepart[5], datepart[6])

    if sattype == 'CDPF':
        datepart = [date[9:11], date[12:14], date[15:17], date[18:len(date)]]
        for i in range(len(datepart)):
            datepart[i] = int(datepart[i])
        newdate = doy2date(date[0:4], date[5:8])
        return datetime.datetime(newdate.year, newdate.month, newdate.day, datepart[0], datepart[1], datepart[2], datepart[3])

    if sattype == 'ASF_CEOS':
        datepart = [date[0:4], date[4:6], date[6:8], date[8:10], date[10:12], date[12:14], date[14:len(date)]]
        for i in range(len(datepart)):
            datepart[i] = int(datepart[i])
        return datetime.datetime(datepart[0], datepart[1], datepart[2], datepart[3], datepart[4], datepart[5], datepart[6])

def date2doy(date, string=False, float=False):
    """
    Give a python datetime and get an integer or string doy fractional doy returned if float=True
    """

    if float:  # get hod - hour of day
        doy = int(date.strftime('%j'))
        hod = date.hour + date.minute/60.0 +\
              date.second/3600
        doy = doy + hod/24
    else:
        doy = int(date.strftime('%j'))
    if string:
        return str(doy)
    else:
        return doy


def doy2date(year, doy):
    """
    Give a float, integer or string and get a datetime
    """

    if type(doy) == type(''):
        doy = float(doy)
    if type(year) == type(''):
        year = int(year)
    if type(year) == type(2.0):
        year = int(year)
        print 'Result might be invalid : year coerced to integer'
    return datetime.datetime(year, 1, 1) + datetime.timedelta(doy - 1)


def datetime2iso(datetimeobj):
    """
    Return iso string from a python datetime
    """

    return datetimeobj.strftime('%Y-%m-%d %H:%M:%S')


def getEarthRadius( ellip_maj, ellip_min, plat_lat):
    """
    Calculates the earth radius at the latitude of the satellite from the ellipsoid params
    """

    r =  ellip_min * ( \
          math.sqrt( 1 + math.tan( plat_lat*D2R )**2 ) / \
          math.sqrt( ((ellip_min**2) / (ellip_maj**2)) + math.tan( plat_lat*D2R )**2 ))
    return r


def getSlantRange(gsr, pixelSpacing, n_cols, order_Rg, groundRangeOrigin=0.0):
    """
    gsr = ground to slant range coefficients -a list of 6 floats
        pixelSpacing - the img. res., n_cols - how many pixels in range
        ground range orig - for RSat2 (seems to be zero always)

        Valid for SLC as well as SGF
    """

    slantRange = numpy.zeros(n_cols, dtype=numpy.float32)
    for j in range(n_cols):
        slantRange[j] = gsr[0] \
            + gsr[1] * (pixelSpacing * j - groundRangeOrigin)\
            + gsr[2] * (pixelSpacing * j - groundRangeOrigin) ** 2 \
            + gsr[3] * (pixelSpacing * j - groundRangeOrigin) ** 3 \
            + gsr[4] * (pixelSpacing * j - groundRangeOrigin) ** 4 \
            + gsr[5] * (pixelSpacing * j - groundRangeOrigin) ** 5

    if order_Rg.lower() == 'decreasing':  #see Altrix doc (applies to all SAR sats - I think)
            slantRange = slantRange[::-1].copy()

    return slantRange

def getGroundRange(slantRange, radius, sat_alt):
    """
    Finds the ground range from nadir which corresponds to a given slant range
    must be an slc image, must have calculated the slantRange first
    """

    ## is this for the start of pixel, end of pixel or middle?
    ## if this is applied (through resampling) how does it affect the GCPs

    #if not 'SLC' don't continue
    r, R, A = slantRange, radius, sat_alt
    #make sure r is a numpy array or a single value

    numer = A*A + 2*A*R + 2*R*R - r*r
    denom = 2*A*R + 2*R*R
    return R*numpy.arccos(numer/denom)



def getThetaPixel(RS, r, h):
    """
    Calc the incidence angle at a given pixel
    """

    theta = math.acos((h ** 2 - RS ** 2 + 2 * r * h) / (2 * RS * r))
    return theta # in radians

def getThetaVector(n_cols, slantRange, radius, sat_alt):
    """
    Make a vector of incidence angles in range direction
    """

    thetaVector = numpy.zeros(n_cols, dtype=numpy.float32)
    for j in range(len(thetaVector)):
        thetaVector[j] = getThetaPixel(slantRange[j], radius, sat_alt)
    return thetaVector
