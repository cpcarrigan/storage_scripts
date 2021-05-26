#!/usr/bin/python3

import logging
import re
from datetime import datetime, timedelta
import configparser
import os

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

env_var = os.environ
print(env_var)

logging.basicConfig(level=logging.ERROR)
logging.getLogger("requests").setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)

from swiftclient.service import SwiftService, SwiftError

container = 'production'
minimum_size = 1024
cutoff_date = datetime.now() - timedelta(days=180)
print(cutoff_date)
print("done with date")

pattern = '^\d+.xml$'

batch_size = 1000
to_delete = []

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
            print(
              "Will delete: %s [size: %s] [etag: %s] - [last_modified: %s]" %
              (i_name, i_size, i_etag, last_modified)
            )
            to_delete.append(i_name)
          # else:
          #   print(
          #     "Keep: %s [size: %s] [etag: %s] - [last_modified: %s]" %
          #     (i_name, i_size, i_etag, last_modified)
          #   )
          if len(to_delete) > batch_size:
            # print("about to delete")
            print(to_delete)
            try:
              delete_gen = swift.delete(container=container, objects=to_delete)
              for element in delete_gen:
                if element["success"]:
                  logger.error("Successful deletion of :", element)
                  print(element)
                else:
                  logger.error("Failure deletion of :", element)
                  print(element)
            except SwiftError as d:
              logger.error(d.value)
            to_delete.clear()
                      
      else:
        raise page["error"]

  except SwiftError as e:
    logger.error(e.value)
