#!/usr/bin/python

import re
import requests
import os
import logging

import argparse

# Overview: Take a list of TNL calls from DBOps, request a response, and save out the Location if it is a 302.
# Most calls will be a 404.
# basic script to validate tnl, and log any valid images

logging.basicConfig(filename='tnl.log',format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

parser = argparse.ArgumentParser()
parser.add_argument("filename", help="Pass a filename to parse for URLs to delete")
args = parser.parse_args()
logging.info("File to import: " + args.filename)

tnl_f = open("tnl-302.txt","w+")

# http://tnl.snapfish.com/assetrenderer/v2/thumbnail/SNAPFISH/vHqf1CBhOSTaDQac3L-gbw/a/W5spIVofLj66CVTE0Y_fvQ/d/gaK2vSO3rPGPZ1Xhsiv48g
# tnl service to call:
tnl = 'http://tnl.snapfish.com/assetrenderer/v2/thumbnail/'

tnl_sess = requests.session()

with open(args.filename, 'rU') as f:
  for line in f:
    url = tnl + line.strip()
    tnl_r = tnl_sess.head(url)

    # line.replace('')
    logging.info(line.strip())
    
    if tnl_r.status_code == 302:
      # grab the 'Location: header, put in log
      logging.warn(str(tnl_r.status_code) + ' status Location: ' + tnl_r.headers['Location'] + " for: " + url)
      tnl_f.write(tnl_r.headers['Location'] + '\n')

    elif tnl_r.status_code == 404:
      logging.debug(str(tnl_r.status_code) + ' status, ignore for: ' + url)
      # do nothing
    else:
      logging.error(str(tnl_r.status_code) + ' status, not handled for: ' + url)

tnl_f.close()