Using SigLib
============

Welcome to the tutorial sections of the SigLib documentation! This section 
gives a brief overview of how to use the Metadata, 
Util, Database, and Image functions via SigLib and its config file. These
example assume the preseeding documentation has been read in detail. For examples
on how to use the Query mode, please see the proceeding section. 

Basic SigLib Setup
------------------

Before SigLib and its dependencies can be used for the first time, some 
basic setup must first be completed. A number of folders must be created and referenced to 
in the config file. Please see the config section of the documentation above for 
the required folders. The full path to each of these folders must be .
 
For SigLib.py to recognise and use the config file properly, your
Python IDE must be set up for running via the command line. The following
instructions are given for the Spyder Python IDE; the setup for other IDEs may vary. 

1.Go to Run -> Configuration per file... (Ctrl + F6) 
2.Under General Settings, check the box labeled *Command Line Options:*
3.In the box to the right, put the full path to the config
file, including the config file itself and its extension.

If SigLib is being run via the command line, simply supply the path to the config file
after the path to the python script. 

	
Example #1: Basic Radarsat2 Image Calibration using SigLib (Qualitative Mode)
-----------------------------------------------------------------------------

Qualitative mode can be utilized to create projected, byte-scale amplitude imagery from Radarsat-1,
Radarsat-2, and Sentinel-1 products. The basic config settings needed to run this mode in its default 
configuration can be found in the figure below. 

.. figure:: basicCFG.png
	:scale: 50%

	A basic config file for this task

In its default settings, full size imagery will be produced, however,
a simple crop or mask can be conducted in this mode if desired. If cropping is desired,
the upper-left and lower-right coordinates of the crop need to be specified in the *crop*
config option in the image projection specified (IE crop = ULX ULY LRX LRY). For masking, 
simply place a shapefile of the desired mask into the vect directory, and specify the filename
(minus the extension) in the *mask* config option. If a mask is provided, a crop zone does not 
need to be provided, as the image will automatically be cropped to the bounding area of the mask
before masking. 

Example #2: Accumulating Image Metadata in a local geodatabase (and intro to parallel processing)
-------------------------------------------------------------------------------------------------

The metaUpload function of SigLib is used to collect and upload metadata for any SAR imagery found locally
to a geodatabase. A database table of local imagery can be useful as an archive for any accumulated products, 
and is also potentially nessesary for using SigLib's Quantitative mode. This example will also demonstrate
how to run a SigLib mode in parallel using the Linux library GNU Parallel (https://www.gnu.org/software/parallel).
GNU Parallel allows the user to run a common command in tandom on different computer cores, thus allowing for processing
time of repetative task to decrease dramatically.   

The basic config settings needed to run this job in parallel are presented in the figure below.

A review of the settings needed for this particular example can be 
seen in the figure below.

.. figure:: uploadMeta.png
	:scale: 50%

	Config file settings for discovering and uploading metadata.
	
In the case of the above figure, it is assumed that metadata uploading is being done for the first time,
thus the *create_tblmetadata* option is set to 1. This will create a geodatabase table with the name provided in 
*metatable_name* with all the required metadata fields. If a metadata table has already been created, make sure 
return *create_tblmetadata* to 0 in order to not overwrite it.

The main parameter that differentiates parallel processing from standard processing is the *file* option in the 
**Input** section. This option configures SigLib to process SAR files via commandline input instead of via directory scan 
or csv/txt list. This means that SigLib is configured to process only a single file per run of the script, which is required
for parallel processing to prevent the same file from processing multiple times by multiple instances. 

To start the parallel job:

1. Open a terminal
2. cd into the directory containing all your radarsat images (They can be in multiple
directories, just make sure they are below the one you cd into, or they will
not be found)
3. Type in the terminal: 
**find . -name '*.zip' -type f | parallel -j 16 --nice 15 --progress python /path/to/SigLib.py/ /path/to/config.cfg/** 
Where -j is the number of cores to use, and --nice is how nice the process will be to 
other processes). 

The find command is looking for zipped SAR files in a directory we specify, then piping what's found
to the parallel command one at a time. The parallel command manages our 16 cores, each of which runs an instance of SigLib 
processing a single zipfile from the find command. When processing of a file is completed by a core, and the script ends, 
parallel automatically starts a new instance of SigLib on that core with a zipfile from the find command. This is repeated until 
there is no further input from find. 

**NOTE:** ALWAYS test parallel on a small batch before doing a major run, to make
sure everything is running correctly. 

Remember: this SigLib mode can be run without parallel by simply using the *path* option instead of the *file* option, and specifying 
a scan directory and filetype. 

If you are on a Windows machine, and wish to run SigLib in parallel, it is possible to use the Multiprocessing Library in Python to call
instances of SigLib. An example on how to do this will not be covered however. 


Example #3: Quantitative Mode
-----------------------------

Quanitative Mode (as described in an earlier section of this documentation) takes SAR scenes, extracts image statistics for study region(s)
specified via a Region of Interest geodatabase table, and accumulates the results into a database table. The ROI should contain a series 
of polygons representing regions of interest in specific timeframes. The ROI must be uploaded to the geodatabase for querying by SigLib,
which can be done via the *uploadROI* config setting, which uploads the ROI shapefile specified in the **MISC** section from the vect directory.
If the ROI is already uploaded, leave the *uploadROI* option at 0, but still specify the name of the ROI and its projection in the **MISC** section,
the software will automatically look in the geodatabase for this table.
**NOTE:** This config setting **MUST** be 0 if running in parallel, or else the ROI will constantly be overwritten. 
This case also requires a database table with SAR image metadata to reference, like the one made in the previous example, or those created via Query Mode.
This is needed to determine which polygons in the ROI overlap an image being processed, thus, any image being processed in this mode must have its metadata
in the metatable. 

.. figure:: Quantitative.png
	:scale: 50%

	Example config file settings for Quanitative Mode. 




	
	


	
	
	
	
	
