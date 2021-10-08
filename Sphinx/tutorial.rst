Using SigLib
============

This section gives a brief overview of how to use the Metadata, 
Util, Database, and Image functions via SigLib and its config file. These
example assume the preseeding documentation has been read in detail, 
and proper setup has been performed as per the **Setup** section of this document. 
For examples on how to use the Query mode, please see the proceeding section. 

	
Example #1: Basic Radarsat2 Image Calibration using SigLib (Qualitative Mode)
-----------------------------------------------------------------------------

Qualitative mode can be utilized to create projected, byte-scale amplitude imagery from Radarsat-1,
Radarsat-2, and Sentinel-1 products. The basic config settings needed to run this mode in its default 
configuration can be found in the figure below. 

.. figure:: basicCFG.png
	:scale: 60%

	A basic config file for this task

In its default settings, full size imagery will be produced, however,
a simple crop or mask can be conducted in this mode if desired. If cropping is desired,
the upper-left and lower-right coordinates of the crop need to be specified in the *crop*
config option, the coordinates themselves in the image projection specified (IE crop = ULX ULY LRX LRY). For masking, 
place a shapefile of the desired mask into the vect directory, and specify the filename
(minus the extension) in the *mask* config option. If a mask is provided, a crop zone does not 
need to be provided, as the image will automatically be cropped to the bounding area of the mask
before masking. 

This config is using the *scanFor* option to find '.zip' files within the specified *SCANDIR* to process. The
*scanFor* option can also be either '.csv' or '.txt' to look for these files within the *SCANDIR*. '.csv' or '.txt'
files should contain a list of image filepaths for SigLib to process, which is a preferred option to '.zip' if the zipfiles
are scattered around your computer. 

Example #2: Accumulating Image Metadata in a local geodatabase (and intro to parallel processing)
-------------------------------------------------------------------------------------------------

The metaUpload function of SigLib is used to collect and upload metadata for any SAR imagery found locally
to a geodatabase. A database table of local imagery can be useful as an archive for any accumulated products, 
and is also potentially nessesary for using SigLib's Quantitative mode. This example will also demonstrate
how to run a SigLib mode in parallel using the Linux library GNU Parallel (https://www.gnu.org/software/parallel).
GNU Parallel allows the user to run a common command in tandom on different computer cores, thus allowing for processing
time of repetative tasks to decrease dramatically.   

The basic config settings needed to run this job in parallel are presented in the figure below.

A review of the settings needed for this particular example can be 
seen in the figure below.

.. figure:: uploadMeta.png
	:scale: 60%

	Config file settings for discovering and uploading metadata.
	
In the case of the above figure, it is assumed that the metadata table has already been created,
thus the *create_tblmetadata* option is set to 0. A metadata table created by SigLib is nessesary
for this process, as it contains all the required metadata fields. If you have not done so, create tblmetadata
as specified in **Setup** before continuing with this example!

The main parameter that differentiates parallel processing from standard processing is the *file* option in the 
**Input** section. This option configures SigLib to process SAR files via commandline input instead of via directory scan 
or csv/txt list. This means that SigLib is configured to process only a single file per run of the script, which is required
for parallel processing to prevent the same file from being processed multiple times by multiple instances. 

To start the parallel job:

1. Open a terminal
2. cd into the directory containing all your images (They can be in multiple
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
instances of SigLib. An example on how to do this will not be covered. 


Example #3: Quantitative Mode
-----------------------------

Quantitative mode is used for a quantitative analysis of the radar signature or backscatter (also known as the normalized radar cross section (nrcs) or sigma nought).
It can extract the sigma nought values within imagery that coincides with specific regions of interest (ROIs) and derive statistics on this SAR signature. 
These statistics can be stored in a database table or CSV file for further analysis. The ROI should contain a series 
of polygons representing regions of interest in specific timeframes, and be uploaded to the database as a new table. This can be accomplished by running SigLib with
the *uploadROI* option set to 1, and the name of an ROI shapefile located in the *VECTDIR* specified in *roi*. As per the creation of tblmetadata in the previous
example, uploading the roi should be done before continuing with this example. 

**NOTE:** This config setting **MUST** be 0 if running in parallel, or else the ROI will constantly be overwritten. 

This mode also requires a database table with SAR image metadata to reference, like the one made in the previous example.
This is needed to determine which polygons in the ROI overlap an image being processed, thus, any image being processed in this mode must have its metadata
in the metatable. 

An example config file for this mode is depicted in the figure below.

.. figure:: Quantitative.png
	:scale: 60%

	Example config file settings for Quanitative Mode. 

For this example, the *uploadResults* parameter is set to 1, meaning the image statistics for each ROI polygon will be uploaded to a results table in the database. 
If uploading results, you must create the results table called 'tblbanddata'. This can be done by running the "CreateResultsTable.sql" script located in 
the Extras folder in PGAdmin. Changing this parameter to 0 will instead output cropped and masked image data for each ROI polygon to the *IMGDIR*. Functionality
to export image statistics as a csv is a work in progress. 

