Tutorial on how to run SigLib/Query.py or standalone script query_imgs.py.
===========================================================================

To be able to run the SigLib on query mode, the first step consists of changing the config file (config.cfg) with query = 1 under Process section. After that simply type: 

``python SigLib.py config.cfg``

On the other hand, the standalone script can be run as follows:

``python query_imgs.py  config.cfg``

If the scripts successfully run, the following menu should appear on the screen:

``Available Query Methods:``

``1: tblmetadata: Query``

``2: tblmetadata: Download``

``3: CIS Archive (WIRL users only)``

``4: EODMS: Query``

``5: EODMS: Order``

``6: EODMS: Download``

``7: Copernicus: Query``

``8: Copernicus: Download``

``9: Execute Raw Sql Query``

``0: Exit``

``Please select the desired query method (0,1,2,3,4,5,6,7,8,9):``

Setup the config file to run the query mode
============================================

The first step to run the query mode is to update the config file (config.cfg) as follows. Under the section *MISC*, update the parameters *roi*, *roiprojSRID*, and *spatialrel* with the name of the ROI shapefile (as it is found in the database), the projection, and the spatial relation (either *ST_Intersects* or *ST_Overlaps*). 

In addition, under the section *Database*, the parameter *table* should contain the name of the local table to be queried (this only applies when querying local tables). Finally, an additional parameter should be added to the config file, *outDir* under the section *Directories*. This parameter should contain the directory where the images will be downloaded. Notice that such directory should exists on disk. 


Example 1. Querying a ROI shapefile on a local table
====================================================

We consider for this example that the ROI shapefile is already on the local database. For details about how to upload a ROI on the local database please consult **Section X?**.


Let us suppose we would like to query a ROI shapefile called ArcticBay with roiprojSRID=4326 and ST_Intersects as spatialrel. The local table that we want to query is tblmetadata. After updating the config file as explained in the initial section of this tutorial, the config file should look as follows:

``[Database]``

…

``table = tblmetadata``

…

``[MISC]``

…

``roi = ArcticBay``

``roiprojSRID = 4326``

…

``spatialrel = ST_Intersects``

After that, we run ``python query_imgs.py  config.cfg`` or ``python SigLib.py  config.cfg``, and we select from the menu option ``1: tblmetadata: Query``. 

We expect the following output, if there were no errors when executing the query:

``ROI start date: 2010-05-01  end date: 2015-12-31``

``Query completed. Write query results to:``

``1: CSV file``

``2: Local table``

``3: Both``

``Enter your option (1,2,3):``

With this menu, the user can select either to output the results to a CSV file or to a local table. 

Suppose we choose option 1: CSV file, the program will output a standard filename. The user can accept that name or type one of its own. Notice that only the filename with extension should be provided (no path). 

``Filename will be: ArcticBay_tblmetadata_24-08-2021_13-44.csv``

``Press [Enter] to accept this name or Introduce a new filename (add .csv extension):``

Suppose for example, we keep the provided name. Afterwards, the program will ask if the images should be downloaded:

``Results saved to /home/outDir/ArcticBay_tblmetadata_24-08-2021_13-44.csv``

``Download 2 images to output directory [Y/N]? N``

Notice that if choosing *Y* all the images in the query result will be downloaded to the output directory (outDir). 





