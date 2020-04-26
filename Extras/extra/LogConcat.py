# -*- coding: utf-8 -*-
"""
**LogConcat.py**

Created on Fri May 18 11:08:28 2018 **@author:**  Cameron Fitzpatrick

This module takes all log files in the specified log directory and combines them into one master .txt file.
All individual log files are then deleted along with all but one config

Potential TODO's: - Make more than one master log file and sort log files via config, date, etc
"""

import os
import sys
import ConfigParser
from time import localtime, strftime

stored_cfg = False
error_found = 0
num_images = 0
total_time = 0

cfg = os.path.expanduser((sys.argv[1]))     #Get config info
config = ConfigParser.RawConfigParser()
config.read(cfg)
cfg = os.path.basename(cfg)[:-4]

logDir = os.path.abspath(os.path.expanduser(config.get("Directories","logDir")))         #Get directories from config file
zipsDir = os.path.abspath(os.path.expanduser(config.get("Directories", "scanDir")))
errorDir = os.path.abspath(os.path.expanduser(config.get("Directories", "errorDir")))

ans = raw_input("Confirm concatenation of log files? All individual log files will be combined into a master .txt and deleted, all but one config file will be deleted. y/n")

if ans == 'y':
    master_log = os.path.join(errorDir, ("_" + strftime('%Y%m%d_%H%M%S', localtime()) + " Master_Log.txt"))  #Creates a master .txt file to write logs to
    fileA = open(master_log, "w+")

    for files in os.listdir(errorDir):    #Goes through files in the logs directory one by one
        if files.endswith(".cfg") and stored_cfg == False:    #Stores a single .cfg file, deletes the rest
            stored_cfg = True
            continue
        
        elif files.endswith(".cfg") and stored_cfg == True:
            file_to_remove = os.path.join(logDir, files)
            os.remove(file_to_remove)
        
        elif files.endswith(".txt"):   #skips any master .txt files
            continue            
            
        elif files.endswith(".log"):      
            error_found = 0
            file_to_open = os.path.join(errorDir, files)
            fileB = open(file_to_open, 'r')
            
            for line in fileB:
                if "SigLib Run w/ config" in line:
                    num_images += 1                   
                    
                
                if "- ERROR -" in line:   #A duplicate was attempted to be sent! Move to error directory for analysis
                    error_found = 1
                    fileB.close()
                    os.rename(os.path.join(logDir, files),os.path.join(errorDir, files))
                    break
                   
            if error_found == 0:   #If no error messages, write to master log file, then delete
                fileB.close()
                file_to_open = os.path.join(errorDir, files)
                fileB = open(file_to_open, 'r') 
                
                for line2 in fileB:
                    fileA.write(line2)
                
                fileB.close()
                file_to_remove = os.path.join(errorDir,files)   #delete log file
                os.remove(file_to_remove)
            
    fileA.write("Total Images Processed: " + str(num_images))
    fileA.close()
    print("Concatenation complete\n \
            Check error directory, logs containing errors have been moved!")
    
else:
    print("Process cancelled")        
