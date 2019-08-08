Overview of the Project
=======================

Intro
-----

SigLib stands for Signature Library and is a suite of tools to query,
manipulate and process remote sensing imagery (primarily Synthetic
Aperture Radar (SAR) imagery) and store the data in a geodatabse. It
uses open source libraries and can be run on Windows or Linux.

There are 4 main *modes* that it can run in (or combinations of these)

#. A data **Discovery Mode** where remote sensing scenes are discovered
   by ingesting a copy of the Canadian Ice Service archive (or other
   geodatabase containing metadata, with tweaks), or by crawling through
   a hard drive and extracting metadata from zipped SAR scenes, or by
   querying a table in a local database that contains geospatial
   metadata. Queries take a Region Of Interest -- **ROI shapefile** with
   a specific format as input. The region of interest delineates the
   spatial and temporal search boundaries. The required attribute fields
   and formats for the ROI are elaborated upon in a section below.
#. An **Exploratory Mode** where remote sensing scenes are made ready
   for viewing. This includes opening zip files, converting imagery
   (including Single Look Complex), geographical projection, cropping,
   masking, image stretching, renaming, and pyramid generation. The user
   must supply the name of a single zip file that contains the SAR
   imagery, a directory where a batch of zip files to be prepared
   resides, or a query that selects a list of zip files to be processed
   (functionality to come).
#. A **Scientific Mode** where remote sensing scenes can be converted to
   either calibrated (sigma0), noise level, or incidence angle images.
   Image data (from each band) can be subsampled by way of an **ROI
   shapefile** that references every image and specific polygon you want
   to analyze. These polygons represent sampling regions that you know
   about (a priori) or they are hand digitized from Exploratory mode
   images. Data can be stored in a table in a geodatabase for further
   processing.
#. An **Analysis Mode** where data that was stored in the geodatabase is
   retrieved and plotted [Note, this is essentially depreciated since it
   hasn't been used for over 5 years]

These modes are brought together to work in harmony by **SigLib.py** the
recommended way to interact with the software. This program reads in a
configuration file that provides all the parameters required to do
various jobs. However, this is only one way to go... Anyone can call the
modules identified above from a custom made python script to do what
they wish, using the SigLib API

In addition, there are different ways to process *input* through
SigLib.py that can be changed for these modes. You can input based on a
recursive **scan** of a directory for files that match a pattern; you
can input one **file** at a time (useful for parallelization, when many
processes are spawned by gnu parallel) and; you can input an SQL
**query** and run the resulting matching files through SigLib (note that
query input is not yet enabled, but it wouldn't take long).

Acknowledgements
----------------

This software was conceived and advanced initially by Derek Mueller
(while he was a Visiting Fellow at the Canadian Ice Service). Some code
was derived from from Defence Research and Development Canada (DRDC). I
benefited from discussions with Ron Saper, Angela Cheng and my salary
was provided via a CSA GRIP project (PI Roger De Abreu).

At Carleton this code was modified further and others have worked to
improve it since the early days at CIS: Cindy Lopes (workstudy student &
computer programmer) 2012, Sougal Bouh-Ali (workstudy student & computer
programmer) 2013-2016, and Cameron Fitzpatrick 2018. Ron Saper, Anna
Crawford and Greg Lewis-Paley helped out as well (indirectly).

