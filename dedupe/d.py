#!/usr/bin/env python3

from datetime import datetime
# import boto3
import os
# import argparse
import logging
import random
import sys
from dateutil.parser import parse as parsedate
import time
from multiprocessing import Pool
# import queue


sys.path.append("/home/ccarrigan/git/storage_scripts") # Adds higher directory to python modules path.
from sf import utils

# parser = argparse.ArgumentParser()
# parser.add_argument("file", help=f"Pass a file with list of prefixes to dedupe.")
# args = parser.parse_args()
start_tic = time.perf_counter()
logging.basicConfig(filename='a.log',level=logging.INFO,format='%(asctime)s.%(msecs)03d - %(levelname)s: %(message)s', datefmt='%d-%b-%y %H:%M:%S')

bucket = 'snapfish-prod-media-raw-snapfish'
data_active = '/fast1/dedupe/fetch-active'
data_done = '/fast1/dedupe/fetch-done'
data_ready = '/fast1/dedupe/fetch-test'
data_results = '/fast1/dedupe/fetch-results'
log_dir = '/fast1/dedupe/fetch-log'
STOP = '/fast1/dedupe/STOP'

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
  ready_file = ''
  prefix_list = []


  while not os.path.exists(STOP):
    try:
      ready_file = random.choice(os.listdir(data_ready))
      print(ready_file)
    except IndexError:
      print("Exiting because no work files available.")
      logging.critical("Exiting because no work files available.")
      sys.exit()
    except Exception as e:
      print(f"Exiting. Failed to get a file to work on. Error occurred: {e}.")
      logging.critical(f"Exiting. Failed to get a file to work on. Error occurred: {e}.")
      sys.exit()

    results_file = open(f"{data_results}/{ready_file}", 'w')

    os.rename(f"{data_ready}/{ready_file}", f"{data_active}/{ready_file}")
    dt_epoch = datetime.timestamp(datetime.now())
    os.utime(f"{data_active}/{ready_file}", (dt_epoch, dt_epoch))

    logging.basicConfig(filename=f'{log_dir}/{ready_file}.log',level=logging.WARNING,format='%(asctime)s.%(msecs)03d - %(levelname)s: %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logging.info(f"File to import: {data_active}/{ready_file}")

    try:
      logging.debug(f"About to open file: {data_active}/{ready_file}")
      with open(f"{data_active}/{ready_file}", 'r') as work_file:
        logging.debug(f"Opened file: {data_active}/{ready_file}")
        for line in work_file:
          logging.debug(f"Working on line: '{line.rstrip}'")
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
              result_dict = result.get(timeout=5)
              for ele_a in result_dict:
                for ele_b in ele_a:
                  results_file.write(f"{ele_b['Bucket']}|{ele_b['Prefix']}|{ele_b['VersionId']}\n")

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
          result_dict = result.get(timeout=5)
          for ele_a in result_dict:
            for ele_b in ele_a:
              results_file.write(f"{ele_b['Bucket']}|{ele_b['Prefix']}|{ele_b['VersionId']}\n")

          # for result in pool.map_async(send_off_dedupe, prefix_list[start:stop]):
          #   print(f"Results: {result}")
          #   to_del.extend(result)
      
    # print(to_del)
    # for list in to_del:
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
