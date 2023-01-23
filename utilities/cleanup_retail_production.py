#!/usr/bin/python3

import configparser
from datetime import datetime, timedelta
import logging
import os
import re
import sys
import time
from sys import argv


# Purpose: get stats from a list of objects inside the same cluster, tenant,
# and containers
# How to invoke: ./get_object_stats.py cluster_short_name tenant
# file_list_of_containers
# Example: ./get_object_stats.py s1 snapfish
# s1-snapfish_uploads-container-list
# Output: cluster-tenant-object-stats.csv aka s1-snapfish-object-stats.csv

config = configparser.ConfigParser()
config.read("../conf/setup.ini")

os.environ['ST_USER'] = config['swiftbuckets-retail']['user']
os.environ['ST_KEY'] = config['swiftbuckets-retail']['password']
os.environ['ST_AUTH'] = 'https://swiftbuckets.sf-cdn.com/auth/v1.0'
os.environ['ST_AUTH_VERSION'] = '1.0'

from swiftclient.service import SwiftService, SwiftError

# env_var = os.environ
# print(env_var)

logging.basicConfig(level=logging.ERROR)
logging.getLogger("requests").setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)

container = 'production'
minimum_size = 10
cutoff_date = datetime.now() - timedelta(days=180)
print(cutoff_date)
print("done with date")

pattern = '^\d+.xml$'

batch_size = 100
to_delete = []

import psutil

def findProcessIdByName(processName):
  '''
  Get a list of all the PIDs of a all the running process whose name contains
  the given string processName
  '''
  listOfProcessObjects = []
  # Iterate over the all the running process
  for proc in psutil.process_iter():
    try:
      pinfo = proc.as_dict(attrs=['pid', 'name', 'create_time'])
      # Check if process name contains the given name string.
      if processName.lower() in pinfo['name'].lower() :
        listOfProcessObjects.append(pinfo)
    except (psutil.NoSuchProcess, psutil.AccessDenied , psutil.ZombieProcess) :
      pass
  return listOfProcessObjects;

listOfProcessIds = findProcessIdByName('cleanup_retail_')
# exit if the program is already running
if len(listOfProcessIds)> 1:
  sys.exit()
  
# print(listOfProcessIds)

_opts = {'object_dd_threads': 20, 'container_threads': 20}
with SwiftService() as swift:
  try:
    list_parts_gen = swift.list(container=container)
    for page in list_parts_gen:
      if page["success"]:
        for item in page["listing"]:

          # 'last_modified': '2014-12-11T12:02:38.774540'
          last_modified = datetime.strptime(item["last_modified"], '%Y-%m-%dT%H:%M:%S.%f')
          # print(last_modified)
          i_size = int(item["bytes"])
          i_name = item["name"]
          i_etag = item["hash"]
          result = re.match(pattern, i_name)
          #if result:
          if (last_modified < cutoff_date) and result:
            # print(
            #   "Will delete: %s [size: %s] [etag: %s] - [last_modified: %s]" %
            #   (i_name, i_size, i_etag, last_modified)
            # )
            to_delete.append(i_name)
          # else:
          #   print(
          #     "Keep: %s [size: %s] [etag: %s] - [last_modified: %s]" %
          #     (i_name, i_size, i_etag, last_modified)
          #   )
          if len(to_delete) > batch_size:
            # print("about to delete")
            # print(to_delete)
            try:
              delete_gen = swift.delete(container=container, objects=to_delete)
              for element in delete_gen:
                tic = time.perf_counter()
                if element["success"]:
                  logger.error("Successful deletion of :", element)
                  # print(element)
                  # print(element['response_dict'][0]['response_dicts'])
                  logger.error("status: ")
                  logger.error(element['response_dict']['response_dicts']['status'])
                  # print("full response_dict: ")
                  print(element['response_dict'])
                  # print(element['response_dict']['response_dicts'][1]['status'])
                else:
                  # print(element)
                  logger.error("Failed deletion of :", element)
                  print(element['response_dict'])
                  print(element['response_dict']['response_dicts'][1]['status'])
                  logger.error("Failure deletion of :", element)
                  # print(element)
                toc = time.perf_counter()
                print(f"Deleted in {toc - tic:0.4f} seconds")
            except SwiftError as d:
              logger.error(d.value)
            to_delete.clear()
                      
      else:
        raise page["error"]

  except SwiftError as e:
    logger.error(e.value)
