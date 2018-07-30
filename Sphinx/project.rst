Overview of SigLib.py and its Dependencies
==========================================

Dependencies
------------

You will need a computer running linux or windows

-  Python 2 (not 3), along with several scientific libraries - numpy,
   pandas, psycopg2, matplotlib, datetime... Recommend you install the
   anaconda package as these contain pretty well everything you will
   need.
-  gdal/ogr libraries
-  PostrgreSQL/PostGIS (could be on another computer)

Nice to haves/future...

-  It is highly recommended that you have access to QGIS or ArcGIS to
   manipulate shapefiles
-  Also, if you want to work with ASF CEOS Files, you will need ASF
   MapReady (some functionality)
-  Eventually, there will be a push to integrate other remote sensing
   tools - SNAP(replaces NEST,PolSARPro), CP Simulator, MADGIC, etc.

Modules
-------

There are several modules that are organized according to core
functionality.

#. **Util.py** - a bunch of utilities for manipulating files,
   shapefiles, etc
#. **Metadata.py** - used to discover and extract metadata from image
   files
#. **Database.py** - used to interface between the PostGIS database for
   storage and retrieval of information
#. **Image.py** - used to manipulate images, project, calibrate, crop,
   etc.
#. **LogConcat.py** - used to combine individual log files into one
   master .txt file, and separate log files containing errors for
   analysis (Mainly for use after large runs in parallel)

**SigLib.py** is the front-end of the software. It calls the modules
listed above and is in turn controlled by a configuration file. To run,
simply edit the \*.cfg file with the paths and inputs you want and then
run siglib.py.

However, you can also code your own script to access the functionality
of the modules if you wish.

Config File
-----------

The **\*.cfg** file is how you interface with siglib. It needs to be
edited properly so that the job you want done will happen! Leave entry
blank if you are not sure. There are several categories of parameters
and these are:

*Directories*

-  scanDir = path to where you want siglib to look for SAR image zip
   files to work with
-  tmpDir = a working directory for extracting zip files to (Basically,
   a folder for temporary files that will only be used during the
   running of the code, then deleted, in scratch folder)
-  projDir = where projection definition piles are found in well-known
   text format (/tank/ice/data/proj)
-  vectDir = where vector layers are found (ROI shapefiles or masking
   layers)
-  dataDir = /tank/path2folder
-  imgDir = a working directory for storing image processing
   intermediate files and final output files, in scratch folder
-  logDir = where logs are placed
-  archDir = where CIS archive data are found
   (/tank/ice/data/vector/CIS\_Archive)
-  errorDir = Where logConcat will send .log files with errors (For
   proper review of bad zips at end of run)

*Database*

-  db = the name of the database you want to connect to
-  create\_tblmetadata = 0 for append, 1 for overwrite/create

*Input*

***Note that these are mutually exclusive options - sum of 'Input'
options must = 1***

-  path = 1 for scan a certain path and operate on all files within; 0
   otherwise
-  query = 1 for scan over the results of a query and operate on all
   files returned; 0 otherwise
-  file = 1 for run process on a certain file, which is passed as a
   command line argument (note this enables parallelized code); 0
   otherwise
-  scanFor = a file pattern to search for (eg. \*.zip) - use when path=1
-  sql = define a custom query here for selecting data to process - use
   when query=1. eg. SELECT location FROM tblmetadata WHERE granule =
   'B0558007.img'

*Process*

-  data2db = 1 when you want to upload metadata to the metadata table in
   the database
-  data2img = 1 when you want to manipulate images (as per specs below)

*IMGMode*

-  proj = basename of wkt projection file (eg. lcc)
-  imgtypes = types of images to process (Amplitude Image - amp, Sigma
   Nought Image - sigma, Incidence Angle image - theta, Noise Floor
   Image - nes2?)
-  crop = nothing for no cropping, or four space-delimited numbers,
   upper-left and lower-right corners (in proj above) that denote a crop
   area: ul\_x ul\_y lr\_x lr\_y
-  maskshp = a polygon shapefile (one feature) to mask image data with
   (eg. /tank/ice/data/vector/CIS\_Vectors/coast\_poly.shp)
-  roiproj = Projection of roi
-  imgformat = File format for output imagery (gdal convention)
-  roi = ROI Shapefile for Discovery or Scientific modes, stored in same
   directory as python files
-  spatialrel = ST\_Contains (Search for images that fully contain the
   roi polygon) or ST\_Intersects (Search for images that merely
   intersect with the roi)

Using a Config in an IDE
------------------------

You can run SigLib inside an integrated development environment (Spyder,
IDLE, etc) or at the command line. In either case you must specify the
configuration file you wish to use:

``python /path_to_script/SigLib.py /path_to_file/config_file.cfg``

Dimgname Convention
-------------------

“The nice thing about standards is that there are so many to chose from”
(A. Tannenbaum), but this gets annoying when you pull data from MDA,
CSA, CIS, PDC, ASF and they all use different file naming conventions.
So I made this problem worse with my own `standard image naming
convention called **dimgname**. All files
processed by SigLib get named as follows, which is good for:

-  sorting on date (that is the most important characteristic of an
   image besides where the image is - and good luck conveying that
   simply in a file name).
-  viewing in a list (because date is first, underscores keep the names
   tidy in a list - you can look down to see the different beams,
   satellites, etc.)
-  extensibility - you can add on to the file name as needed - add a
   subscene or whatever on the end, it will sort and view the same as
   before.
-  extracting metadata from the name (in a program or spreadsheet just
   parse on "\_")

Template: date\_time\_sat\_beam\_data\_proj.ext

Example: 20080630\_225541\_r1\_scwa\_\_hh\_s\_lcc.tif

Table: **dimgname fields**

+------------+---------------------------------------------------------------+--------------------------------------------------------+---------+
| Position   | Meaning                                                       | Example                                                | Chars   |
+============+===============================================================+========================================================+=========+
| \| Date    | year month day                                                | 20080630                                               | 8       |
+------------+---------------------------------------------------------------+--------------------------------------------------------+---------+
| \| Time    | hour min sec                                                  | 225541                                                 | 6       |
+------------+---------------------------------------------------------------+--------------------------------------------------------+---------+
| \| Sat     | satellite/platform/sensor                                     | r1,r2,e1,en                                            | 2       |
+------------+---------------------------------------------------------------+--------------------------------------------------------+---------+
| \| Beam    | Beam for SAR, band combo for optical                          | st1\_\_,scwa\_,fqw20\_,134\_\_                         | 5       |
+------------+---------------------------------------------------------------+--------------------------------------------------------+---------+
| \| Band    | pol for SAR, meaning of beam for optical (tc = true colour)   | hh, hx, vx, vv, hv, qp                                 | 2       |
+------------+---------------------------------------------------------------+--------------------------------------------------------+---------+
| \| Data    | what is represented (implies a datatype to some extent        | a= amplitude, s=sigma, t=incidence,n=NESZ, o=optical   | 1       |
+------------+---------------------------------------------------------------+--------------------------------------------------------+---------+
| \| Proj    | projection                                                    | nil, utm, lcc, aea                                     | 3       |
+------------+---------------------------------------------------------------+--------------------------------------------------------+---------+
| \| Ext     | file extension                                                | tif, rrd, aux, img                                     | 3       |
+------------+---------------------------------------------------------------+--------------------------------------------------------+---------+

ROI.shp format
--------------

The ROI.shp or Region Of Interest shapefile is what you need to extract
data. Basically it denotes *where* and *when* you want information. It
has to have certain fields to work properly. There are two basic
formats, based on whether you are using the **Discovery** or
**Scientific** mode. If you are interested in 1) finding out what
scenes/images might be available to cover an area or 2) generating
images over a given area then use the *Discovery* format. If you have
examined the images already and have digitized polygons of areas that
you want to analyze (find statistics), then make sure those polygons are
stored in a shapefile using the *Scientific* format. In either case you
must have the fields that are required for *Both* formats in the table
below. You can add whatever other fields you wish and some suggestions
are listed below as *Optional*.

The two fields which are required for both Discovery or Scientific mode
use may be confusing, so here are some further details with examples.

-  OBJ - this is a unique identifier for a given area or object
   (polygon) that you are interested in getting data for.
-  INSTID - A way to track OBJ that is repeatedly observed over time
   (moving ice island, a lake during fall every year for 5 years). [If
   it doesn't repeat just put '0']

Example workflow:
-----------------

You could be interested in lake freeze-up in the Yukon, drifting ice
islands, or soil moisture in southern Ontario farm fields. First you
will want to find out what data are available, retrieve zip files and
generate imagery to look at. In this case use the *Discovery* format.
Each lake, region that ice islands drift through or agricultural area
that you want to study would be given a unique OBJ. If you have only one
time period in mind for each, then INSTID would be '0' in all cases. If
however, you want to look at each lake during several autumns, ice
islands as they drift or farm fields after rain events, then each OBJ
will have several rows in your shapefile with a different FROMDATE and
TODATE. Then for each new row with the same OBJ, you must modify the
INSTID such that a string that is composed of OBJ+INSTID is unique
across your shapefile. This is what is done internally by SigLib and a
new field is generated called INST (in the PostGIS database). Note that
the FROMDATE and TODATE will typically be different for each OBJ+INSTID
combination.

If you know what imagery is available already, or if you have digitized
specific areas corresponding where you want to quantify backscatter (or
image noise, incidence angle, etc), then you should use the *Scientific*
format. In this case, the principles are the same as in the *Discovery*
mode but your concept of what an OBJ might be, will be different.
Depending on the study goals, you may want backscatter from the entire
lake, in which case your OBJ would be the same as in *Discovery* mode,
however, the INSTID must be modified such that there is a unique
OBJ+INSTID for each image (or image acquisition time) you want to
retrieve data for. The scientific OBJ should change when you are hand
digitizing a specific subsample from each OBJ from the *Discovery* mode.
For example:

-  within each agricultural area you may want to digitize particular
   fields;
-  instead of vast areas to look for ice islands you have actually
   digitized each one at a precise location and time

Build your *Scientific* ROI shapefile with the field IMGREF for each
unique OBJ+INSTID instead of the FROMDATE and TODATE. By placing the
dimgname of each image you want to look at in the IMGREF field, SigLib
can pull out the date and time and populate the DATEFROM and DATETO
fields automatically. Hint: the INSTID could be IMGREF if you wished
(since there is no way an OBJ would be in the same image twice).

Once you complete your ROI.shp you can name it whatever you like (just
don't put spaces in the filename, since that causes problems).

Table: **ROI.shp fields**

+---------------+------------+-------------------------------------------------------------------------------------------------------+------------------------------------------------+--------------+
| Field         | Var. Type  | Description                                                                                           | Example                                        | ROI Format   |
+===============+============+=======================================================================================================+================================================+==============+
| \| OBJ        | String     | A unique identifier for each polygon object you are interested in                                     | 00001, 00002                                   | Both         |
+---------------+------------+-------------------------------------------------------------------------------------------------------+------------------------------------------------+--------------+
| \| INSTID     | String     | An iterator for each new row of the same OBJ                                                          | 0,1,2,3,4                                      | Both         |
+---------------+------------+-------------------------------------------------------------------------------------------------------+------------------------------------------------+--------------+
| \| FROMDATE   | String     | ISO Date-time denoting the start of the time period of interest                                       | 2002-04-15 00:00:00                            | Discovery    |
+---------------+------------+-------------------------------------------------------------------------------------------------------+------------------------------------------------+--------------+
| \| TODATE     | String     | ISO Date-time denoting the end of the time period of interest                                         | 2002-09-15 23:59:59                            | Discovery    |
+---------------+------------+-------------------------------------------------------------------------------------------------------+------------------------------------------------+--------------+
| \| IMGREF     | String     | dimgname of a specific image known to contain the OBJ polygon (Spaces are underscores)                | 20020715 135903 r1 scwa  hh s lcc.tif          | Scientific   |
+---------------+------------+-------------------------------------------------------------------------------------------------------+------------------------------------------------+--------------+
| \| Name       | String     | A name for the OBJ is nice to have                                                                    | Ward Hunt, Milne, Ayles                        | Optional     |
+---------------+------------+-------------------------------------------------------------------------------------------------------+------------------------------------------------+--------------+
| \| Area       | Float      | You can calculate the Area of each polygon and put it here (choose whatever units you want)           | 23.42452                                       | Optional     |
+---------------+------------+-------------------------------------------------------------------------------------------------------+------------------------------------------------+--------------+
| \| Notes      | String     | Comment field to explain the OBJ                                                                      | Georeferencing may be slightly off here?       | Optional     |
+---------------+------------+-------------------------------------------------------------------------------------------------------+------------------------------------------------+--------------

-  See folder ROISamples for example ROIs - Discovery and Scientific
   mode

TODO
====

\*# capture stdout and stderr from spawned processes

\*# Make sure there is process/output testing and error trapping at
every major step.

\*# Develop a test suite of imagery for the project - R2 and R1 images
that are in different beam modes, orbit directions, even bad images to
test siglib. (imagery with no EULA so it can be shared)

-  version control (github? bitbucket?) - both software and version
   identification and tracking changes for users
-  Continue documentation

   #. overarching documentation important too
   #. UML diagram for visual
   #. example scripts/configs
   #. example ROI.shp

-  add local? [Not sure exactly what this is]
-  investigate compatibility with python 3

SigLib.py
---------

-  add 'modes' to this - so that siglib can do what is described above.
-  add qryDatabase stuff or at least some of it (part of discovery mode)
-  update config.cfg accordingly

Metadata.py
-----------

-  get look direction for RSAT2, test against RSAT1

Image.py
--------

-  test Pauli decomp and write in a switch for this - so users can
   choose?
-  test image crop and mask - in both modes

Util.py
-------

-  deltree needs work (or can it be removed?)

Sphinx
------

-  Run this to put wiki info in. Must save info as .wik file: pandoc -s
   -S -f mediawiki intro.wik -t rst -o intro.rst
-  Using Siglib!