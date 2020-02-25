#!/usr/bin/python

import re
import requests
import os
import logging
import argparse
import sf_utils

# Overview: Take a file full of cos URLs and delete them

logging.basicConfig(filename='deletion.log',format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

parser = argparse.ArgumentParser()
parser.add_argument("filename", help="Pass a filename to parse for URLs to delete")
args = parser.parse_args()
logging.info("File to import: " + args.filename)

files_object = sf_utils.ObjectStorage()
with open(args.filename, 'r') as f: 
  for line in f:
    line = line.rstrip()
    logging.warning("Raw URL to delete: '" + line + "'")
    url = files_object.clean_up_url(line)
    logging.info("Cleaned URL to delete: '" + url + "'")
    status = files_object.delete_object_file(url)
    logging.warning("Deletion status: " + str(status)  + " for '" + url + "'")