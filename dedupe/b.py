#!/usr/bin/env python3

import datetime
# import boto3
# import os
import argparse
import logging
import sys
from dateutil.parser import parse as parsedate
import time
from multiprocessing import Lock, Process, Queue, current_process
import queue


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

def do_job(tasks_to_accomplish, tasks_that_are_done):
  while True:
    to_del = []
    try:
      '''
        try to get task from the queue. get_nowait() function will 
        raise queue.Empty exception if the queue is empty. 
        queue(False) function would do the same task also.
      '''
      task = tasks_to_accomplish.get_nowait()
      to_del = utils.dedupe_s3_object('snapfish-prod-media-raw-snapfish',task)
      
    except queue.Empty:

      break
    else:
      '''
        if no exception has been raised, add the task completion 
        message to task_that_are_done queue
      '''
      print(task)
      # tasks_that_are_done.put(to_del task + ' is done by ' + current_process().name)
      print(f"{to_del} task is done by {current_process().name}")
      tasks_that_are_done.put(to_del)
      # time.sleep(.5)
  return True


def main():
  prefix_count = 0
  number_of_task = 10
  number_of_processes = 24
  tasks_to_accomplish = Queue()
  tasks_that_are_done = Queue()
  processes = []

  # for i in range(number_of_task):
  #   tasks_to_accomplish.put("Task no " + str(i))
  for line in data_file:
    prefix = line.rstrip()
    prefix_count += 1
    tasks_to_accomplish.put(prefix)

  # creating processes
  for w in range(number_of_processes):
    p = Process(target=do_job, args=(tasks_to_accomplish, tasks_that_are_done))
    processes.append(p)
    p.start()

  # completing process
  for p in processes:
    p.join()

  # print the output
  while not tasks_that_are_done.empty():
    print(tasks_that_are_done.get())

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
  #setup()
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
