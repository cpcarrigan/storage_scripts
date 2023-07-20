#!/usr/bin/env python3

from dateutil.parser import *
from datetime import datetime
import os
import logging
from logging.handlers import RotatingFileHandler
import random
import sys
import time
from multiprocessing import Pool, TimeoutError

import os
os.environ["AWS_SHARED_CONFIG_FILE"] = "/opt/dedupe/.aws/config"
os.environ["AWS_SHARED_CREDENTIALS_FILE"] = "/opt/dedupe/.aws/credentials"

import sfutils

# logging.basicConfig(filename='/var/log/dedupe/loggy.log',level=logging.WARNING,format='%(asctime)s.%(msecs)03d - %(levelname)s: %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logging.basicConfig( handlers=[RotatingFileHandler('/var/log/dedupe/dedupe.log', maxBytes=250000000, backupCount=5)],
                     level=logging.WARNING,
                     format='%(asctime)s.%(msecs)03d - %(levelname)s: %(message)s',
                     datefmt='%d-%b-%y %H:%M:%S' )

bucket = 'snapfish-prod-media-raw-snapfish'
data_active = '/opt/dedupe/fetch-active'
data_done = '/opt/dedupe/fetch-done'
data_ready = '/opt/dedupe/fetch-ready'
data_results = '/opt/dedupe/fetch-results'
data_retry = '/opt/dedupe/fetch-retry'
# log_dir = '/opt/dedupe/fetch-log'
STOP = '/opt/dedupe/STOP'

def send_off_dedupe(prefix):
  return sfutils.dedupe_s3_object(bucket, prefix)

def main():
  batch_size = 48
  ready_file = ''

  while not os.path.exists(STOP):
    prefix_list = []
    start_tic = time.perf_counter()
    prefix_count = 0

    try:
      ready_file = random.choice(os.listdir(data_ready))
      logging.warning(f"Chose {ready_file} file and starting work")
    except IndexError:
      print("Exiting because no work files available.")
      logging.critical("Exiting because no work files available.")
      sys.exit()
    except Exception as e:
      print(f"Exiting. Failed to get a file to work on. Error occurred: {e}.")
      logging.critical(f"Exiting. Failed to get a file to work on. Error occurred: {e}.")
      sys.exit()

    results_file = open(f"{data_results}/{ready_file}", 'w')
    retry_file = open(f"{data_retry}/{ready_file}", 'w')

    os.rename(f"{data_ready}/{ready_file}", f"{data_active}/{ready_file}")
    dt_epoch = datetime.timestamp(datetime.now())
    os.utime(f"{data_active}/{ready_file}", (dt_epoch, dt_epoch))

    # logging.basicConfig(filename=f'{log_dir}/{ready_file}.log',level=logging.WARNING,format='%(asctime)s.%(msecs)03d - %(levelname)s: %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logging.info(f"File to import: {data_active}/{ready_file}")

    logging.debug(f"About to open file: {data_active}/{ready_file}")
    with open(f"{data_active}/{ready_file}", 'r') as work_file:
      logging.debug(f"Opened file: {data_active}/{ready_file}")
      try:
        for line in work_file:
          logging.debug(f"Working on line: '{line.rstrip()}'")
          prefix_list.append(line.rstrip())
          prefix_count += 1
          # tasks_to_accomplish.put(prefix)
          logging.debug(f"prefix_count: {prefix_count} / modulo: {prefix_count % batch_size}")

          # submit when batch size hits
          if prefix_count % batch_size == 0:
            start = prefix_count - batch_size
            pl = Pool()
            results = [pl.apply_async(send_off_dedupe, (_xx,)) for _xx in prefix_list[start:prefix_count]]
            rets = []
            timed_out_results = []

            for res in results:
              try:
                rets.append(res.get(5))
              except TimeoutError:
                timed_out_results.append(res)

            if rets is not None:
              for ele_a in rets:
                for ele_b in ele_a:
                  results_file.write(f"{ele_b['Bucket']}|{ele_b['Prefix']}|{ele_b['VersionId']}\n")
            if timed_out_results is not None:
              results_file.write(f"{timed_out_results}\n")
              
            pl.close()
            pl.join()


      finally:
        # got to catch the case where it ends evenly, otherwise this is to pick up
        # any remaining prefixes outside of the batch
        if prefix_count % batch_size == 0:
            logging.debug(f"No remaining lines in file: {data_active}/{ready_file}, so nothing to process.")
        else:
          start = 0
          stop = 0
          if prefix_count < batch_size:
            start = 0
            stop = len(prefix_list)
          else:
            start = prefix_count - batch_size
            stop = len(prefix_list)

          pl = Pool()
          results = [pl.apply_async(send_off_dedupe, (_xx,)) for _xx in prefix_list[start:stop]]
          rets = []
          timed_out_results = []
          for res in results:
            try:
              rets.append(res.get(5))
            except TimeoutError:
              timed_out_results.append(res)

          for ele_a in rets:
            if rets is not None:
              for ele_b in ele_a:
                results_file.write(f"{ele_b['Bucket']}|{ele_b['Prefix']}|{ele_b['VersionId']}\n")

          # not seeing any matches for this, wtf?
          if timed_out_results is not None:
            retry_file.write(f"{timed_out_results}\n")

          pl.close()
          pl.join()
      
    # move {data_active}/{ready_file} to {data_done}/{ready_file}YP
    work_file.close()
    results_file.close()
    retry_file.close()
    os.rename(f"{data_active}/{ready_file}", f"{data_done}/{ready_file}")

    finish_tic = time.perf_counter()
    logging.warning(f"Run took: {finish_tic - start_tic:0.4f} seconds for {prefix_count} prefixes ({(finish_tic - start_tic)/prefix_count:0.4f} prefix/s)")
    print(f"Run took: {finish_tic - start_tic:0.4f} seconds for {prefix_count} prefixes ({(finish_tic - start_tic)/prefix_count:0.4f} prefix/s)")

  return True

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
