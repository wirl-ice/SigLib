TODO
====

-  Test Discover meta!
-  add proper logging to replace print statements AND standardize error
   trapping and handling

   #. capture stdout and stderr from spawned processes
   #. Make sure there is process/output testing and error trapping at
      every major step.
   #. Need a way to isolate a reliable summary of bad images at the end.
      Make sure this works in both dir scan and file input
   #. Develop a test suite of imagery for the project - R2 and R1 images
      that are in different beam modes, orbit directions, even bad
      images to test siglib. (imagery with no EULA so it can be shared)

-  version control (github? bitbucket?) - both software and version
   identification and tracking changes for users
-  Continue documentation

   #. every function should have complete
      comments/parameters/options/return for sphinx (standard format)
   #. overarching documentation important too
   #. UML diagram for visual
   #. example scripts/configs
   #. example ROI.shp
   #. run Sphinx - put all this wiki info in there...

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

Database.py
-----------

-  test now that I replaced srid 914 with 4326
-  qryfromlocal... [not too sure what this is?]

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

-  pandoc -s -S -f mediawiki intro.wik -t rst -o intro.rst
