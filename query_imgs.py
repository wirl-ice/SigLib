import geopandas
import psycopg2
import psycopg2.extras
import os
import sys
from configparser import RawConfigParser
from datetime import datetime, timedelta
import pandas as pd
import shutil
import csv
import requests
from osgeo import ogr, osr
from eodms_api_client import EodmsAPI
import json
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt
import getpass
from ftplib import FTP
import re
from Database import Database
from Query import Query


def read_config():

    cfg = os.path.expanduser((sys.argv[1]))
    config = RawConfigParser()  # Needs to be tested for python2 compatibility 
    config.read(cfg)
    vectDir = str(os.path.abspath(os.path.expanduser(config.get("Directories", "vectDir"))))
    outDir = str(os.path.abspath(os.path.expanduser(config.get("Directories", "outDir"))))
    scanDir = str(os.path.abspath(os.path.expanduser(config.get("Directories", "scanDir"))))
    uploadROI = str(config.get("Database", "uploadROI"))


    dbName = str(config.get("Database", "db"))
    dbHost = str(config.get("Database", "host"))
    table_to_query = str(config.get("Database", "table"))

    roi = str(config.get('MISC', "roi"))
    roiProjSRID = str(config.get('MISC', "roiProjSRID"))
    spatialrel = str(config.get('MISC', "spatialrel"))

    return vectDir, scanDir, outDir, dbName, dbHost, table_to_query, roi, roiProjSRID, spatialrel, uploadROI



def query_menu(db, roiDir, scanDir, outputDir, table_to_query, roi, roiSRID, spatialrel):


    query_methods = {'1': 'metadata', '2': 'download_metadata', '3': 'cis', '4': 'EODMS', '5': 'ORDER_EODMS',
                     '6': 'DOWNLOAD_EODMS', '7': 'SENTINEL', '8': 'DOWNLOAD_SENTINEL', '9': 'RAW_SQL', '0': 'EXIT'}
    print("Available Query Methods:\n")
    print("1: {}: Query".format(table_to_query))
    print("2: {}: Download".format(table_to_query))
    print("3: CIS Archive (WIRL users only)")
    print("4: EODMS: Query")
    print("5: EODMS: Order")
    print("6: EODMS: Download")
    print("7: Copernicus: Query")
    print("8: Copernicus: Download")
    print("9: Execute Raw Sql Query")
    print("0: Exit")
    try:
        ans = input("Please select the desired query method (0,1,2,3,4,5,6,7,8,9):\t")
        method = query_methods[ans]
    except:
        print ('That is not a valid option.')
        return

    Query(db, roi, roiSRID, roiDir, scanDir, table_to_query, spatialrel, outputDir, method)


    return


def run():
    vectDir, scanDir, outDir, dbName, dbHost, table_to_query, roi, roiProjSRID, spatialrel, uploadROI = read_config()
    db = Database(table_to_query, dbName, host=dbHost)

    if uploadROI == "1":
        db.updateROI(roi, roiProjSRID, vectDir)  # Refer to this function in documentation before running to confirm convension
        ans = input("Create image references from {}? [Y/N]\t".format(table_to_query))
        if ans.lower() == 'y':
            db.findInstances(roi)

    query_menu(db, vectDir, scanDir, outDir, table_to_query, roi, roiProjSRID, spatialrel)
    
    
if __name__ == "__main__":
    run()
   
    
