#!/usr/local/bin/python
# -*- coding: UTF-8 -*-
'''
Script to open zipfiles and replace the EULAs with the one specified
Works on a single file or on an entire directory
Can also rename the zip file to match the unzip folder name

If you have gnu parallel, you can run this in parallel by doing this: 
find *.zip -maxdepth 0 -type f | parallel -j 15 --nice 15 --progress python /tank/SCRATCH/dmueller/EULA_replace/replace_eula.py {} /tank/SCRATCH/dmueller/EULA_replace/Licences/LI-11525-6RS2EULA_SingleUser_V1-5_24APR2009.pdf -r

'''
import os
import sys
import zipfile
import glob
import shutil
import getopt
import pdb

__author__="muellerd"
__date__ ="$Sep 2, 2009 8:14:19 PM$"

# Generic functions
def deltree(dirname):
    
    if os.path.exists(dirname):
        for root,dirs,files in os.walk(dirname):
            for d in dirs:
                deltree(os.path.join(root,d))
            for f in files:
                os.remove(os.path.join(root,f))
        os.rmdir(dirname)

def getZipRoot(zipname):
    '''looks into a zipfile and determines if the contents will unzip into
    their own subdirectory (ZipRoot).  If not, unpack to a ZipRoot based on the
    zipfile name.  Also return the ZipRoot for zipfiles that have one.
    '''
    #determine if the zipfile contains directory structure, some don't
    z = zipfile.ZipFile(zipname)
    testdir = z.namelist()[0]
    if '/' not in testdir: # this means the files are not in a directory
        unpackdir, ext = os.path.splitext(zipname)
        ziproot = unpackdir
    else:  # the files will unzip into a new directory
        unpackdir = os.getcwd()
        ziproot, f = os.path.split(testdir)
        ziproot = os.path.join(unpackdir, ziproot)
    return unpackdir, ziproot

def make_zipfile(output_filename, source_dir):
    '''See: stackoverflow.com/questions/1855095/how-to-create-a-zip-archive-of-a-directory-in-python
    Note in 2.7.3 the shutil.make_archive doesn't handle relpath base_names correctly
    Code below does the job - don't need an empty directory though - commented out
    '''
    relroot = os.path.abspath(os.path.join(source_dir, ".."))
    with zipfile.ZipFile(output_filename, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as zip:
        for root, dirs, files in os.walk(source_dir):
            # zip.write(root, os.path.relpath(root, relroot))  # add directory (needed for empty dirs)
            for file in files:
                filename = os.path.join(root, file)
                if os.path.isfile(filename): # regular files only
                    arcname = os.path.join(os.path.relpath(root, relroot), file)
                    zip.write(filename, arcname)
    zip.close()

def checkEULA(zipname, criteria):    
    '''
    Open a zipfile and look to see if the pdf files matches any of the criteria specified
    return a list of file names 
    '''
    eula_pdf = []
    z = zipfile.ZipFile(zipname)
    unpackdir, ziproot = getZipRoot(zipname)
    for file in z.namelist():
        if '.pdf' in file:
            for criterion in criteria:
                if criterion in file:
                    eula_pdf.append(os.path.basename(file))
                    
    z.close()
    return list(set(eula_pdf))

def syntax():
    print('''SYNTAX: replace_eula.py directory/zipfile eula_name -r  
        
        Please supply a [directory -OR- zipfile] AND a [EULA file name]...
        
            *directory*: - all zipfiles will be worked on
            *Zipfile*: - only that one file will be worked on
            *eula_name*: - the name of the eula file to insert 
            
            Optional:
            *-r*:  - will rename the zipfile after the unzipped folder name
            
            Tips: 
            The EULA filename should not have any spaces (or quote it)
            Put the EULA file in the directory along with the zip files
            
            Example:
            python replace_eula.py ./SAR_staging SingleUserEULA.pdf

    ''')
    sys.exit(1)

#OK HERE IS THE SCRIPT
#print sys.argv

opts, args = getopt.gnu_getopt(sys.argv[1:], "-r")
optdict = dict(opts)

zipdata = None
datadir = None

if len(sys.argv) >= 3:
    if os.path.isfile(sys.argv[1]):
        zipdata = sys.argv[1]
    if os.path.isdir(sys.argv[1]):
        datadir = sys.argv[1]
    if zipdata is None and datadir is None:
        syntax()  
    eula = sys.argv[2]
    eula = os.path.abspath(os.path.expanduser(eula))
    if not os.path.isfile(eula):
        syntax()
else:
    syntax()
renamezip = ("-r" in optdict)

       
if datadir is not None:
    print('''This script may overwrite all zipfiles in the specified directory
        Do not do this with your only copy of the data....
        Type 'Y' to continue or any other key to exit''')
    ans = sys.stdin.readline()
    if ans.strip().lower() != 'y':
        print('Exiting the program')
        sys.exit(0)

    datadir = os.path.abspath(datadir)
    os.chdir(os.path.join(datadir))

    #make list of all the zip files (data)
    ziplist = glob.glob('*.zip')
    ziplist = [os.path.join(datadir,z) for z in ziplist]

if zipdata is not None:
    ziplist = [os.path.abspath(zipdata)]


####### Setup a for loop to go to a directory and unzip files
for zipname in ziplist:
    print('Opening file: ', zipname)
    #   is there an offending EULA?
    eula_match = checkEULA(zipname, ['EULA_Gov', 'Multi-User'])

    if len(eula_match) > 0:
        z = zipfile.ZipFile(zipname)
        os.chdir(os.path.split(zipname)[0])
        unpackdir, ziproot = getZipRoot(zipname)
        z.extractall(path=unpackdir)
        z.close()
        
        #Delete all EULA files in the zip
        for root, dirs, files in os.walk(ziproot):
            for file in files:
                if ('EULA' in file or 'DFAIT' in file) and '.pdf' in file:
                    os.remove(os.path.join(root, file))
                    shutil.copy(os.path.abspath(eula), os.path.join(root, os.path.basename(eula)))
                            
        #make new zipfile
        os.chdir(unpackdir)
        if renamezip:
            finalname = os.path.basename(ziproot) + '.zip'
        else: 
            finalname = os.path.split(zipname)[1]            
        make_zipfile('temp_'+finalname, ziproot)
        #pdb.set_trace()
        os.remove(zipname) #delete the original zip file
        print('Removed original file ', zipname)
        os.rename('temp_'+finalname,finalname)
        deltree(ziproot)

print('\n....Finished job!')
