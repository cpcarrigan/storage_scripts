#!/usr/bin/python

import argparse
import configparser
import logging
import os
import re
import requests

# Overview: Take a file full of URLs and delete them.

# todo:
# deal with cname issue - ie swiftbuckets7
# handle the most generic format handed off by DBOPS - no tweaking ingest file
# for thumbnail files: make call against TNL service, pull location, if you get valid URL, transform to storage, and then delete
# deal w/ amazon images, cleversafe images

logging.basicConfig(filename='deletion.log',format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

parser = argparse.ArgumentParser()
parser.add_argument("filename", help="Pass a filename to parse for URLs to delete")
args = parser.parse_args()
logging.info("File to import: " + args.filename)

config = configparser.ConfigParser()
config.read("../conf/setup.ini")
# tenant_user = config[cluster + '-' + tenant]['user']
# tenant_pass = config[cluster + '-' + tenant]['password']

# http://swiftbuckets.sf-cdn.com/v1/encoding/migrationHosting_5002/lr_0159_191_555_0159191555012lr.jpg

# dict of lists, store user, tenant, auth token, password, requests session
#      {'cluster-tenant': ['cluster', 'tenant', 'user-str', 'password', 'session-obj','auth-token']
sess = {'swiftbuckets-uass':     ['swiftbuckets', 'uass', config['swiftbuckets-uass']['user'], config['swiftbuckets-uass']['password'], '', ''],
        'swiftbuckets-snapfish': ['swiftbuckets', 'snapfish', config['swiftbuckets-snapfish']['user'], config['swiftbuckets-snapfish']['password'], '', ''],
        'swiftbuckets-encoding': ['swiftbuckets', 'encoding', config['swiftbuckets-encoding']['user'], config['swiftbuckets-encoding']['password'], '', ''],
        's1-snapfish':           ['s1', 'snapfish', config['s1-snapfish']['user'], config['s1-snapfish']['password'], '', ''],
        's1-uass':               ['s1', 'uass', config['s1-uass']['user'], config['s1-uass']['password'], '', ''],
        's2-snapfish':           ['s2', 'snapfish', config['s2-snapfish']['user'], config['s2-snapfish']['password'], '', ''],
        's2-uass':               ['s2', 'uass', config['s2-uass']['user'], config['s2-uass']['password'], '', '']
        }

def createSession(cl_te='all'):
  logging.info("Creating session for " + cl_te)
  if cl_te == 'all':
    for key in sess:
      logging.debug(sess[key][0])
      sess[key][4] = requests.session()
      temp_resp = sess[key][4].get('http://'+ sess[key][0] + '.sf-cdn.com/auth/v1.0', 
        headers={"X-Auth-User": sess[key][2], "X-Auth-Key": sess[key][3] })
      sess[key][5] = temp_resp.headers["X-Auth-Token"]
      logging.debug(sess[key])
  else:
    sess[cl_te][4] = requests.session()
    temp_resp = sess[cl_te][4].get('http://'+ sess[cl_te][0] + '.sf-cdn.com/auth/v1.0', 
      headers={"X-Auth-User": sess[cl_te][2], "X-Auth-Key": sess[cl_te][3] })
    sess[cl_te][5] = temp_resp.headers["X-Auth-Token"]
  logging.info("Done creating session for " + cl_te)

createSession()

url_pattern = re.compile(r"http://([^.]+).sf-cdn.com/v1/([^/]+)/([^/]+)/(.*)$")

with open(args.filename, 'rU') as f:
  for line in f:
    # use a regex to fix a problem, get two problems
    line.replace('https://','http://')
    line.replace('swiftbuckets1.sf-cdn.com','s1.sf-cdn.com')
    line.replace('swiftbuckets3.sf-cdn.com','s1.sf-cdn.com')
    line.replace('swiftbuckets4.sf-cdn.com','s2.sf-cdn.com')
    line.replace('swiftbuckets7.sf-cdn.com','s1.sf-cdn.com')
    # line.replace('')
    logging.info(line.strip())
    m = url_pattern.match(line.strip())
    if m:
      cluster = m.group(1)
      logging.info("cluster: " + cluster)
      tenant = m.group(2)
      logging.info("tenant: " + tenant)
      container = m.group(3)
      logging.info("container: " + container)
      obj = m.group(4)
      logging.info("object: " + obj)
      cl_te = cluster + '-' + tenant
      try:
        logging.debug(sess[cl_te])
        r = sess[cl_te][4].delete(line.strip(), headers={'X-Auth-Token': sess[cl_te][5]})
        logging.debug(r)
        r.raise_for_status()
      except requests.HTTPError:
        logging.error("ERROR status code: " + str(r.status_code) + " for URL: " + line.strip())
        if r.status_code == 401:
          # login again
          createSession(cl_te)
          r = sess[cl_te][4].delete(line.strip(), headers={'X-Auth-Token': sess[cl_te][5]})
      except requests.exceptions.ConnectionError:
        # set up the connection again
        createSession(cl_te)
        r = sess[cl_te][4].delete(line.strip(), headers={'X-Auth-Token': sess[cl_te][5]})
      except KeyError:
        logging.error("No key for: " + cl_te)
    else:
      logging.error('URL does not match expected pattern: ' + line.strip())
