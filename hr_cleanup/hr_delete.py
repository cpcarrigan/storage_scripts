#!/usr/bin/python3

import re
import requests
import os
import logging
import argparse
import sys
sys.path.append("..") # Adds higher directory to python modules path.
from sf import utils

# Overview: Take the generated files, parse, and delete the objects:

# sample file entries:
# 92536169,3677266552,http://s1.sf-cdn.com/v1/snapfish/migration_36516/hr_0004_233_285_0004233285546.jpg
# 92536169,3677275633,http://s1.sf-cdn.com/v1/snapfish/migration_36516/hr_0004_233_295_0004233295234.jpg

# todo:
# handle the most generic format handed off by DBOPS - no tweaking ingest file
# for thumbnail files: make call against TNL service, pull location, if you get valid URL, transform to storage, and then delete
# deal w/ amazon images, cleversafe images
# log anything that cannot be processed
# log separately items to reprocess.


parser = argparse.ArgumentParser()
parser.add_argument("filename", help="Pass a filename to parse for URLs to delete")
args = parser.parse_args()
logging.basicConfig(filename=args.filename + '.log',level=logging.WARN,format='%(asctime)s.%(msecs)03d - %(levelname)s: %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logging.info("File to import: " + args.filename)

files_object = utils.ObjectStorage()

f_pattern = re.compile(r"http://.*.sf-cdn.com/.*\.jpg$")

with open(args.filename, 'r') as f:
  for line in f:
    ele = line.strip().split(',')
    # logging.warning(f"looking at: '{line.rstrip()}'")
    matched = f_pattern.match(ele[2])
    if matched:
      # use a regex to fix a problem, get two problems
      url = ele[2]
      logging.info("Raw URL to delete: '" + url + "'")
      url = files_object.clean_up_url(url)
      logging.info("Cleaned URL to delete: '" + url + "'")
      status = files_object.delete_object_file(url)
      logging.warning("Deletion status: " + str(status)  + " for '" + url + "'")
