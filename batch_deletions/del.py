#!/usr/bin/env python3

import boto3

from dateutil.parser import *
from datetime import datetime
import os
import logging
from logging.handlers import RotatingFileHandler
# import random
import sys
import time

import os
os.environ["AWS_SHARED_CONFIG_FILE"] = ".aws/config"
os.environ["AWS_SHARED_CREDENTIALS_FILE"] = ".aws/credentials"

import sfutils

logging.basicConfig( handlers=[RotatingFileHandler('del.log', maxBytes=250000000, backupCount=5)],
                     level=logging.WARNING,
                     format='%(asctime)s.%(msecs)03d - %(levelname)s: %(message)s',
                     datefmt='%d-%b-%y %H:%M:%S' )

batch = 1000

bucket = ''
del_file = ''

def main():
    s3_client = boto3.client('s3')

    if len(sys.argv) == 3:
        bucket = sys.argv[1]
        del_file = sys.argv[2]
    else:
        print('usage: ./del.py bucket_name delete_file.txt\nThe delete file should be a list of objects in the bucket, 1 per line. eg: 100061/158714409060-3142591617070.jpg')


    last_line = ''
    start_tic = time.perf_counter()
    with open(del_file, 'rb') as f:
        try:  # catch OSError in case of a one line file 
            f.seek(-2, os.SEEK_END)
            while f.read(1) != b'\n':
                f.seek(-2, os.SEEK_CUR)
        except OSError:
            f.seek(0)
    
        last_line = f.readline().decode()
    
    tcount = 0
    
    with open(del_file) as work_file:
        to_del = []
        count = 0
        
        bstart_tic = time.perf_counter()
        for line in work_file:
            # format of lines:
            # a69f79/4066701-202548520016.jpg
            logging.debug(f"Line: {line}")
            try:
                # skey = line.rstrip('\n').split('|')
                skey = line.rstrip('\n')
            except ValueError:
                next
            to_del.append({ 'Key': skey })
            count += 1
            tcount += 1
    
            if count % batch == 0:
                logging.debug(f"Will delete the following batch: {to_del}")
                logging.warning(f"Deleting batch ending with key: {skey}")
        
                response = s3_client.delete_objects(Bucket=bucket, Delete={'Objects': to_del})
                logging.info(response)
                bfinish_tic = time.perf_counter()
                logging.warning(f"Batch took: {bfinish_tic - bstart_tic:0.4f} seconds for {count} records ({(bfinish_tic - bstart_tic)/count:0.4f} seconds per record)")
                print(f"Batch took: {bfinish_tic - bstart_tic:0.4f} seconds for {count} records ({(bfinish_tic - bstart_tic)/count:0.4f} seconds per record)")
                count = 0
                to_del = []
                bstart_tic = time.perf_counter()
    
            elif line == last_line:
                logging.debug(f"Will delete the following batch: {to_del}")
                logging.warning(f"Deleting last batch ending with key: {skey}")

                response = s3_client.delete_objects(Bucket=bucket, Delete={'Objects': to_del})
                logging.info(response)
                bfinish_tic = time.perf_counter()
                logging.warning(f"Batch took: {bfinish_tic - bstart_tic:0.4f} seconds for {count} records ({(bfinish_tic - bstart_tic)/count:0.4f} seconds per record)")
                print(f"Batch took: {bfinish_tic - bstart_tic:0.4f} seconds for {count} records ({(bfinish_tic - bstart_tic)/count:0.4f} seconds per record)")
                count = 0
                to_del = []
                bstart_tic = time.perf_counter()
    
    # move {data_active}/{ready_file} to {data_done}/{ready_file}YP
    work_file.close()

    finish_tic = time.perf_counter()
    logging.warning(f"Done with: {del_file}")
    logging.warning(f"Run took: {finish_tic - start_tic:0.4f} seconds for {tcount} records ({(finish_tic - start_tic)/tcount:0.4f} seconds per record)")
    print(f"Run took: {finish_tic - start_tic:0.4f} seconds for {tcount} records ({(finish_tic - start_tic)/tcount:0.4f} seconds per record)")

if __name__ == "__main__":
    main()
