#!/usr/bin/python3

import gzip
import logging
import os
import random
import requests
import sys
import time

from PIL import Image
from io import BytesIO

jpg_f_limit = '64000'

if (len(sys.argv) < 2):
  print("instructions on use:")
  print("./get_img_details.py s1")
  exit()

bucket = sys.argv[1]

# grabs a single file, no path
try:
  data_file = random.choice(os.listdir(f"work/{bucket}/ready/"))
except IndexError:
  print("Exiting because no work files available.")
  exit()
except:
  print("Failed to get a file to work on.")

# data_file = sys.argv[1]

os.rename(f"work/{bucket}/ready/{data_file}", f"work/{bucket}/active/{data_file}")

data_handle = gzip.open(f"work/{bucket}/active/{data_file}", 'rt')

# chop the last 4 characters, append new string, whack previous '-done.csv.gz' file
output_file = data_file[0:len(data_file)-7] + '-done.csv.gz'
# print(output_file)
output_handle = gzip.open(f"work/{bucket}/done/{output_file}", 'wt')

s = requests.Session()

for line in data_handle:

  # sample lines:
  # 0,751447346011,5221243011,SFO,SB,MIGRATED,StorageAPIOLRValid,http://swiftbuckets.sf-cdn.com/v1/uass/olowres_635/uLPYvi0HzQtsEjYBVBjAQtlvAF71gZmshc0MpJJFgwo.jpg
  # 3,764679844007,30331600,SFO,SB,MIGRATED,AssetLLRValid,http://swiftbuckets.sf-cdn.com/v1/snapfish/snapfish_uploads14862/lr/0030/331/600/UPLOAD/0881777762007lr.jpg

  # print(line)
  tic = time.perf_counter()
  ele = line.strip().split(',')

  url = ele[7]
  r_headers = {'Range': 'bytes=0-' + jpg_f_limit }

  # print(url)

  get_t1 = time.perf_counter()
  r_get = s.get(url, headers=r_headers)
  get_time = r_get.elapsed.total_seconds()
  get_t2 = time.perf_counter()
  get_time2 = get_t2 - get_t1


  if r_get.status_code == 206:
    head_bytes = 0
    head_time = 0
    head_time2 = 0
    if r_get.headers['Content-Length'] == str(int(jpg_f_limit) + 1):
      head_t1 = time.perf_counter()
      r_head = s.head(url)
      head_t2 = time.perf_counter()
      head_bytes = int(r_head.headers['Content-Length'])
      head_time = r_head.elapsed.total_seconds()
      head_time2 = head_t2 - head_t1
    
    get_bytes = int(r_get.headers['Content-Length'])

    img_t1 = time.perf_counter()
    img = Image.open(BytesIO(r_get.content))
    w, h = img.size
    img_t2 = time.perf_counter()

    f_bytes = max(head_bytes, get_bytes)

    toc = time.perf_counter()
    perf_total = toc - tic

    # print('\n#################################')
    # print(f'Asset ID:   {ele[1]}')
    # print(f'URL:        {ele[7]}')
    # print(f'width:      {w}')
    # print(f'height:     {h}')
    # print(f'head bytes: {head_bytes}')
    # print(f'get bytes:  {get_bytes}')
    # print(f'final size: {f_bytes}')
    # print('output line: ')
    # print(f'{line.strip()},{w},{h},{f_bytes}')

    logging.warning(f'URL={ele[7]}, get_time={get_time:.3f}, perf_get={get_time2:.3f}, head_time={head_time:.3f}, perf_head={head_time2:.3f}, perf_total={perf_total:.3f}, total_time={(head_time+get_time):.3f}, width={w}, height={h}, bytes={f_bytes}')

    output_handle.write(f'{line.strip()},{w},{h},{f_bytes}\n')

data_handle.close()
output_handle.close()
