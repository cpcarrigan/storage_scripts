#!/usr/bin/python3

import re
import requests
import os
import logging
import argparse
import sf_utils

# Overview: Take the DBA generated files, parse, and delete the objects:

# sample file entries:
# LOWRES,http://images2.snapfish.com/ldm70000/snapfish_uploads3469/lr/1254/313/401/UPLOAD//0373744800019lr.jpg
# HIRES,http://images2.snapfish.com/ldm70000/snapfish_uploads9964/hr/0373/745/433//0373745433019.jpg
# THUMBNAIL,SNAPFISH/q9oR7ng5ibKN7EgeVqPPzA/a/7H7q-BxLAhqBX_RDkFX6FA/d/qhac9fJtVzxe-Z2ExgA-lg/time/dkhjkgTDtxEhXrxi9x3neQ/h/s7/m/s7/l/cs
# LOWRES,http://images2.snapfish.com/ldm70000/snapfish_uploads9729/lr/1254/313/401/UPLOAD//0373745433019lr.jpg
# HIRES,http://swiftbuckets7.sf-cdn.com/v1/uass/hires_7092/0a25fee8-ad55-4ee6-b4b6-4677fa74afe2.jpg
# THUMBNAIL,SNAPFISH/ZK5zSF6AvLlmcFsRLs7nhefnZWqfchaTRMgQwD_L7Po/a/tlhQobR4QzS-t4fsm5AmlA/d/qhac9fJtVzxe-Z2ExgA-lg/time/fuAsTgbtQoFbd-GVEdBz6Q/h/s7/m/s7/l/cs
# HIRES,https://swiftbuckets7.sf-cdn.com/v1/uass/hires_1252/2a923a30-b427-44c2-b417-60c5d4657e86.jpg
# THUMBNAIL,SNAPFISH/uKB-bhWtsf7xB1LT2XOrlefnZWqfchaTRMgQwD_L7Po/a/tlhQobR4QzS-t4fsm5AmlA/d/qhac9fJtVzxe-Z2ExgA-lg/time/opdz2WAPc-QPUSOkXFt5zQ/h/s7/m/s7/l/cs
# HIRES,https://swiftbuckets7.sf-cdn.com/v1/uass/hires_7955/550496cf-dc79-4c56-89fb-f57ffda9c076.jpg

# todo:
# handle the most generic format handed off by DBOPS - no tweaking ingest file
# for thumbnail files: make call against TNL service, pull location, if you get valid URL, transform to storage, and then delete
# deal w/ amazon images, cleversafe images
# log anything that cannot be processed
# log separately items to reprocess.

logging.basicConfig(filename='deletion_gdpr.log',level=logging.WARN,format='%(asctime)s - %(levelname)s: %(message)s', datefmt='%d-%b-%y %H:%M:%S')

parser = argparse.ArgumentParser()
parser.add_argument("filename", help="Pass a filename to parse for URLs to delete")
args = parser.parse_args()
logging.info("File to import: " + args.filename)

files_object = sf_utils.ObjectStorage()

f_pattern = re.compile(r"^(LOWRES,|HIRES,|THUMBNAIL,)")

# f_pattern = re.compile(r"(http://.*.sf-cdn.com/.*(\.jpg|\.mp4|\.mpg))(\?.*)$")
with open(args.filename, 'r') as f:
  for line in f:
    logging.warning("looking at: \'" + line.rstrip() + "\'")
    matched = f_pattern.match(line)
    if matched:
      # use a regex to fix a problem, get two problems
      url_type, url = line.split(',')
      if url_type == 'THUMBNAIL':
        url = files_object.get_referer(url)
      if url:
        url = url.rstrip()
        logging.info("Raw URL to delete: '" + url + "'")
        url = files_object.clean_up_url(url)
        logging.info("Cleaned URL to delete: '" + url + "'")
        status = files_object.delete_object_file(url)
        logging.warning("Deletion status: " + str(status)  + " for '" + url + "'")
