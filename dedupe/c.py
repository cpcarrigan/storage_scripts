#!/usr/bin/env python3

import datetime
# import boto3
# import os
import argparse
import logging
import sys
from dateutil.parser import parse as parsedate
import time
from multiprocessing import Pool
# import queue


sys.path.append("..") # Adds higher directory to python modules path.
from sf import utils

parser = argparse.ArgumentParser()
parser.add_argument("file", help=f"Pass a file with list of prefixes to dedupe.")
args = parser.parse_args()
start_tic = time.perf_counter()
logging.basicConfig(filename='a.log',level=logging.INFO,format='%(asctime)s.%(msecs)03d - %(levelname)s: %(message)s', datefmt='%d-%b-%y %H:%M:%S')

bucket = 'snapfish-prod-media-raw-snapfish'

data_file = open(args.file,'r')

to_del = []
  
# prefix_count = 0

def send_off_dedupe(prefix):
  return utils.dedupe_s3_object('snapfish-prod-media-raw-snapfish', prefix)

def main():
  prefix_count = 0
  number_of_task = 10
  number_of_processes = 24
  batch_size = 48
  processes = []

  # for i in range(number_of_task):
  #   tasks_to_accomplish.put("Task no " + str(i))
  prefix_list = []
  try:
    with open(args.file, 'r') as data_file:

      for line in data_file:
        prefix_list.append(line.rstrip())
        prefix_count += 1
        # tasks_to_accomplish.put(prefix)
        print(f"prefix_count: {prefix_count} / modulo: {prefix_count % batch_size}")
        # submit when batch size hits
        if prefix_count % batch_size == 0:
          start = prefix_count - batch_size
          # with Pool(processes=number_of_processes) as pool:
          with Pool() as pool:
            result = pool.map_async(send_off_dedupe, prefix_list[start:prefix_count])
            print(result.get(timeout=5))
            # for result in pool.map_async(send_off_dedupe, prefix_list[start:prefix_count]):
              # print(f"Results: {result}")
              # to_del.extend(result)
  finally:
    # got to catch the case where it ends evenly, otherwise this is to pick up
    # any remaining prefixes outside of the batch
    if prefix_count % batch_size == 0:
      print("nothing to process")
    else:
      start = 0
      stop = 0
      if prefix_count < batch_size:
        start = 0
        stop = len(prefix_list)
      else:
        start = prefix_count - batch_size
        stop = len(prefix_list)

      # with Pool(processes=number_of_processes) as pool:
      with Pool() as pool:
        result = pool.map_async(send_off_dedupe, prefix_list[start:stop])
        print(result.get(timeout=5))
        # for result in pool.map_async(send_off_dedupe, prefix_list[start:stop]):
        #   print(f"Results: {result}")
        #   to_del.extend(result)
      
  # print(to_del)
  #for list in to_del:
  for deletable in to_del:
    logging.warning(f"Delete version {deletable['VersionId']} of object {deletable['Prefix']} in bucket {deletable['Bucket']}")
  logging.warning(f"################################################")
  finish_tic = time.perf_counter()
  print(f"Run took: {finish_tic - start_tic:0.4f} seconds for {prefix_count} prefixes ({(finish_tic - start_tic)/prefix_count:0.4f} prefix/s)")
  # print(f"Run took: {finish_tic - start_tic:0.4f} seconds")

  return True

# logging.warning(f"Will delete the following version(s): {to_del}\n")

if __name__ == "__main__":
  main()


"""
$ aws s3api list-object-versions --bucket snapfish-prod-media-raw-snapfish --prefix 1f5eff/40758720050-1004851833784070.jpg
{
    "Versions": [
       {
          "ETag": "\"da223358500f6c4574afde9cc5ba3c8b\"",
          "Size": 3223319,
          "StorageClass": "STANDARD_IA",
          "Key": "1f5eff/40758720050-1004851833784070.jpg",
          "VersionId": "8m2R1xp.fhJpKieFDbf2Y4Qkk4PlZJ6c",
          "IsLatest": true,
          "LastModified": "2021-12-04T00:03:34.000Z"
       },
       {
          "ETag": "\"da223358500f6c4574afde9cc5ba3c8b\"",
          "Size": 3223319,
          "StorageClass": "STANDARD_IA",
          "Key": "1f5eff/40758720050-1004851833784070.jpg",
          "VersionId": "HYj13tDAz9Haq4jj8BFQMP1CDxWODkEk",
          "IsLatest": false,
          "LastModified": "2021-11-27T21:34:56.000Z"
       }
    ]
 }
"""
