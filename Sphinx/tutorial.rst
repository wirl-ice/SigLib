Using SigLib
============

Welcome to the tutorial sections of the SigLib documentation! This section 
gives a brief overview of how to use the Metadata, 
Util, Database, and Image functions via SigLib and its config file,
or in a custom way via qryDatabase.  

Basic SigLib Setup
------------------

Before SigLib and its dependencies can be used for the first time, some 
basic setup must first be completed. In the downloaded SigLib file, there 
are five Python files (*.py), a config file (*.cfg), and an extras folder 
containing some odds and ends (including this very document you are reading!). 
 
A number of folders must be created and refered to the 
config file. Please see the config section of the documentation above for 
the required folders. These directories are used to keep the various input, 
temporary, and output files organized. Once created, the full path to each file 
must be added to the config file alongside the directory it is set to represent. 
The config file contains example path listings.
 
For SigLib.py to recognise and use the config file properly, your
Python IDE must be set up for running via the command line. The following
instructions are given for the Spyder Python IDE; the setup for other IDEs may vary. 

1.Go to Run -> Configuration per file... (Ctrl + F6) 
2.Under General Settings, check the box labeled *Command Line Options:*
3.In the box to the right, put the full path to the config
file, including the config file itself and its extension.
4.Press the OK button to save the setting and close the
window

	
Example #1: Basic Radarsat2 Image Calibration using SigLib
---------------------------------------------------------

In this example we will be using SigLib to produce Tiff 
images from Amplitude Radarsat2 image files.
Before any work beings in Python, the config file must be configured for this
type of job, see the figure below for the required settings. 
Place a few Radarsat2 zip files in your scanDir, then open your IDE configured for 
command line running, and run SigLib. 

What will happen is as follows: The zipfile will be extracted to the temp 
directory via Util.py. The metadata will then be extracted and saved to the output 
directory, via Metadata.py. Image.py will create an initial Tiff image via GDAL or SNAPPY,  
and saved to the output directory. The image will then be reprojected 
and stretched into a byte-scaled Tiff file. 
All intermediate files will then be cleaned and Siglib will move onto the next zipfile,
until all the files in the scanDir are converted.

.. figure:: basicCFG.png
	:scale: 50%

	A basic config file for this task


Example #2: Discover Radarsat metadata and upload to a geodatabase
------------------------------------------------------------------

This example will be the first introduction to Database.py and PGAdmin. 
In this example we will be uploading the metadata of Radarsat scenes to a
geodatabase for later reference (and for use in later examples). This process
will be done using the parallel library on linux. See https://www.gnu.org/software/parallel
for documentation and downloads for the parallel library. **NOTE:** This example only
works on *linux* machines, how the results of this example can be replicated
on other machines will be explained afterwards.

This job will be done via the data2db process of SigLib, as seen in the
config. Also, since we are running this example in parallel, the input
must be **File** not **Path**. 

In this case, we need the metadata table in our geodatabase to already exist. If this table has not been created yet, run SigLib with "create_tblmetadata" equal to 1, with all modes under **Process** equal to 0 before continuing with the rest of this example.

A review of the settings needed for this particular example can be 
seen in the figure below.

.. figure:: uploadMeta.png
	:scale: 50%

	Config file settings for discovering and uploading metadata.


To start the parallel job:

1. Open a terminal
2. cd into the directory containing all your radarsat images (They can be in multiple
directories, just make sure they are below the one you cd into, or they will
not be found)
3. Type in the terminal: 
**find . -name '*.zip' -type f | parallel -j 16 --nice 15 --progress python /path/to/SigLib.py/ /path/to/config.cfg/** 
Where -j is the number of cores to use, and --nice is how nice the process will be to 
other processes (I.E. A lower --nice level gives this job a higher priority over
other processes). The first directory is the location of your verison of SigLib.py,
the second is the location of the associated config file. 

**NOTE:** ALWAYS test parallel on a small batch before doing a major run, to make
sure everything is running correctly. 

Once started, Parallel will begin to step though your selected
directory looking for .zip files. Once one is found, it will pass it to one of the
16 availible (or however many cores you set) openings of Siglib.py. SigLib will
unzip the file via Util.py, grab the metadata via Metadata.py, then connect to your
desired database, and upload this retrieved metadata to the relational table
*tblmetadata* (which will have to be created by running createTblMetadata() in 
Database.py before parallelizing) via Database.py. This will repeat until parallel has 
fully stepped through your selected directory. 

Most SigLib process can be parallelized, as long as the correct config parameters
are set, and the above steps on starting a parallel job are followed.

The same results for this example can be achieved for non-linux machines by
putting all the zip files containing metadata for upload into your scanDir,
and using the same settings as in the above figure, except in the *Input* section,
**File** must be set to 0, and **Path** must be set to 1.


Example #3: Scientific Mode!
----------------------------

In this example, we will dive into the depths of SigLibs' Scientific Mode!
Scientific Mode (as described in an earlier section of this documentation) is a way of 
taking normal radarsat images and converting them to a new image type (sigma0, beta0, and gamma0) followed by cropping and masking them into small 
pieces via a scientific ROI. The ROI should contain a series of polygons representing regions of interest for different scenes. For example, the polygons could be individual farmers fields, or individual icebergs. The ROI created must be uploaded to the geodatabase for querying by SigLib. To upload the ROI specified in the config, set 'uploadROI' equal to 1, as seen below in the example config. **NOTE:** This config setting **MUST** be 0 if running in parallel, or else the ROI will constantly be overwritten. This case also requires a database with SAR image footprints, like the one made in the previous example! 

.. figure:: scientific.png
	:scale: 50%

	Config file settings for scientific mode. Note that we are uploading an ROI in this example. The first time scientific is run with a new ROI, this setting will be nessesary, otherwise it can be set equal 		to 0

Once begun, this mode takes a SAR image in the scanDir, and calibrates it to the selected image type. Once completed, database.py is used to query the ROI against the image footprint to find which polygons in the ROI are within the scene being processed. Each of these hits is then processed one at a time, beginning with a bounding-box crop around the instance, followed by a mask using the ROI polygon (both queried via Database.py). At this point, each instance is projected and turned into its own TIFF file for delivery, or the image data for the instances is uploaded to a database table made to store data from this run. 


Conclusion
----------

This is the conclusion to the *Using SigLib* section of this documentation. For 
additional help in using SigLib.py and its dependencies, please refer to the next section
of this documentation, *SigLib API*. This section gives and overview, the parameters, 
and the outputs, of each function in the main five modules.

	
	


	
	
	
	
	
