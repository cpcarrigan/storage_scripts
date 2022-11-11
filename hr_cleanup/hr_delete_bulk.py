#!/usr/bin/python3

from datetime import datetime
import re
import requests
import os
import json
import logging
import argparse
import sys
import glob
import random

sys.path.append("/home/ccarrigan/git/storage_scripts") # Adds higher directory to python modules path.
from sf import utils

# Overview: Take the generated files, parse, and delete the objects:

# sample file entries:
# assetid,account,hr_url
# 92536169,3677266552,http://s1.sf-cdn.com/v1/snapfish/migration_36516/hr_0004_233_285_0004233285546.jpg
# 92536169,3677275633,http://s1.sf-cdn.com/v1/snapfish/migration_36516/hr_0004_233_295_0004233295234.jpg

# todo:

# log anything that cannot be processed
# log separately items to reprocess.


STOP = '/home/ccarrigan/s1-deletion/work/STOP'

while not os.path.exists(STOP):
    try:
        #     print(random.choice(os.listdir('work/ready')))
        data_file = random.choice(os.listdir('work/ready'))
        print(data_file)
    except IndexError:
        print("Exiting because no work files available.")
        logging.critical("Exiting because no work files available.")
        sys.exit()
    except Exception as e:
        print(f"Exiting. Failed to get a file to work on. Error occurred: {e}.")
        logging.critical(f"Exiting. Failed to get a file to work on. Error occurred: {e}.")
        sys.exit()

    os.rename(f"work/ready/{data_file}", f"work/active/{data_file}")
    dt_epoch = datetime.timestamp(datetime.now())
    os.utime(f"work/active/{data_file}", (dt_epoch, dt_epoch))

    s = requests.Session()

    logging.basicConfig(filename=f'work/log/{data_file}.log',level=logging.WARNING,format='%(asctime)s.%(msecs)03d - %(levelname)s: %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logging.info(f"File to import: work/active/{data_file}")

    files_object = utils.ObjectStorage()

    # f_pattern = re.compile(r"http://([^.]+).sf-cdn.com/v1/([^/]+)/pg$")
    f_pattern = re.compile(r"http://([^.]+).sf-cdn.com/v1/([^/]+)/([^/]+)/(.*)$")

    with open(f"work/active/{data_file}", 'r') as f:
    # with open(args.filename, 'r') as f:
        bulk_list = []
        count = 1000
        status = False

        acct = ''
        cluster = ''
        new_acct = False
        for line in f:
            ele = line.strip().split(',')
            logging.info(f"looking at: '{line.rstrip()}'")
            # 09-Nov-22 22:03:15.423 - WARNING: looking at: '2448034026,241339706026,http://s1.sf-cdn.com/v1/snapfish/migrationHosting_7426/hr_0273_943_350_0273943350026.jpg'
            matched = f_pattern.match(ele[2])
            if matched:
                # use a regex to fix a problem, get two problems
                url = ele[2]
                logging.debug("Raw URL to delete: '" + url + "'")
                url = files_object.clean_up_url(url)
                logging.debug("Cleaned URL to delete: '" + url + "'")
                if acct == '' and cluster == '':
                    acct = matched.group(2)
                    cluster = matched.group(1)
                if acct != matched.group(2) or cluster != matched.group(1):
                    new_acct = True

                # need to add logic so that if the hostname or the account changes,
                # submit the batch without waiting for the full set of data
                if len(bulk_list) < count and new_acct == False:
                    bulk_list.append(url)
                else:
                    status = files_object.delete_openstack_batch(bulk_list)
                    logging.warning(f"Deletion status: {status} for bulk list of {len(bulk_list)} urls, first url in list: {bulk_list[0]}")
                    bulk_list = []
                    status = False

    f.close()

    os.rename(f"work/active/{data_file}", f"work/done/{data_file}")

else:
    print(f"Stop file ({STOP}) exists. Exiting")
    logging.critical(f"Stop file ({STOP}) exists. Exiting")
    sys.exit()
