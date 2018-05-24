# -*- coding: utf-8 -*-
"""
**LogConcat.py**

Created on Fri May 18 11:08:28 2018 **@author:**  Cameron Fitzpatrick

This module takes all log files in the specified log directory and combines them into one master .txt file.
All individual log files are then deleted along with all but one config

Potential TODO's: - Make more than one master log file and sort log files via config, date, etc
                  - Put log files of failed images into a different place (After a parallelized run mainly) to get clear results fast!
"""

import os
import sys
import ConfigParser
from time import localtime, strftime

stored_cfg = False
zipname = ""

cfg = os.path.expanduser((sys.argv[1]))
config = ConfigParser.RawConfigParser()
config.read(cfg)
cfg = os.path.basename(cfg)[:-4]

logDir = os.path.abspath(os.path.expanduser(config.get("Directories","logDir")))
zipsDir = os.path.abspath(os.path.expanduser(config.get("Directories", "scanDir")))

ans = raw_input("Confirm concatenation of log files? All individual log files will be combined into a master .txt and deleted, all but one config file will be deleted. y/n")

if ans == 'y':
    master_log = os.path.join(logDir, ("_" + strftime('%Y%m%d_%H%M%S', localtime()) + " Master_Log.txt"))
    fileA = open(master_log, "w+")

    for files in os.listdir(logDir):
        if files.endswith(".cfg") and stored_cfg == False:
            stored_cfg = True
            continue
        
        elif files.endswith(".cfg") and stored_cfg == True:
            file_to_remove = os.path.join(logDir, files)
            os.remove(file_to_remove)
        
        elif files.endswith(".txt"):
            continue
        
        elif files.endswith(".log"):
            file_to_open = os.path.join(logDir, files)
            fileB = open(file_to_open, 'r')
            
            for line in fileB:
                if "Zipname: " in line:
                    zipname = line
                if "- ERROR -" in line:
                    pass
                    '''
                    TODO: Move this log (close it first) to new "error" directory (Make and config)
                    
                    for zips in os.listdir(zipsdir):
                        if zipname in zips:
                            #Now, grap this zipfile and move it to new "error" directory (Make and config)
                    '''
                               
            for line in fileB:
                fileA.write(line)
                
            fileB.close()
            file_to_remove = os.path.join(logDir,files)
            os.remove(file_to_remove)

    fileA.close()
    print "Concatenation complete"
    
else:
    print "Process cancelled"          
