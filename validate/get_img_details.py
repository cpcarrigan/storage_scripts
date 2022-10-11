#!/usr/bin/python3

""" Open a csv file, parse it, check for height, width & bytes and then write
new csv to new location. """

import glob
import gzip
from io import BytesIO
import logging
import os
import random
import sys
import time
import warnings

import requests

from PIL import Image, ImageFile

ImageFile.LOAD_TRUNCATED_IMAGES = True
warnings.simplefilter('ignore', Image.DecompressionBombWarning)

JPG_F_LIMIT = '64000'

if len(sys.argv) < 2:
  print("instructions on use:")
  print("./get_img_details.py s1")
  sys.exit()

bucket = sys.argv[1]

STOP = '/fast1/lr-updates/STOP'

while not os.path.exists(STOP):
  try:
    data_file = random.choice(glob.glob(
                f"work/{bucket}/ready/*csv.gz")).strip(f"work/{bucket}/ready/")
  except IndexError:
    print("Exiting because no work files available.")
    sys.exit()
  except Exception as e:
    print(f"Failed to get a file to work on. Error occurred: {e.message}.")
    sys.exit()

  os.rename(f"work/{bucket}/ready/{data_file}", f"work/{bucket}/active/{data_file}")

  work_handle = gzip.open(f"work/{bucket}/active/{data_file}", 'rt')

  output_handle = gzip.open(f"work/{bucket}/done/{data_file}", 'at')
  bad_handle = gzip.open(f"work/{bucket}/bad/{data_file}", 'at')

  s = requests.Session()

  for line in work_handle:
    # sample lines:
    # 751447346011,5221243011,SFO,SB,MIGRATED,StorageAPIOLRValid,http://swiftbuckets.sf-cdn.com/v1/uass/olowres_635/uLPYvi0HzQtsEjYBVBjAQtlvAF71gZmshc0MpJJFgwo.jpg
    # 764679844007,30331600,SFO,SB,MIGRATED,AssetLLRValid,http://swiftbuckets.sf-cdn.com/v1/snapfish/snapfish_uploads14862/lr/0030/331/600/UPLOAD/0881777762007lr.jpg

    tic = time.perf_counter()
    ele = line.strip().split(',')

    if ele[5] == 'StorageAPIHRValid' or ele[5] == 'OppositeDataCenterHRValid' :
      print(f"skipping {line}")
      continue

    url = ele[6]
    r_headers = {'Range': 'bytes=0-' + JPG_F_LIMIT }

    get_t1 = time.perf_counter()
    try:
      r_get = s.get(url, headers=r_headers)
    except requests.exceptions.ConnectionError :
      bad_handle.write(line)
      continue

    get_time = r_get.elapsed.total_seconds()
    get_t2 = time.perf_counter()
    get_time2 = get_t2 - get_t1

    if r_get.status_code == 206:
      head_bytes = 0
      head_time = 0
      head_time2 = 0
      if r_get.headers['Content-Length'] == str(int(JPG_F_LIMIT) + 1):
        head_t1 = time.perf_counter()
        try:
          r_head = s.head(url)
        except ConnectionResetError:
          bad_handle.write(line)
          continue

        head_t2 = time.perf_counter()
        head_bytes = int(r_head.headers['Content-Length'])
        head_time = r_head.elapsed.total_seconds()
        head_time2 = head_t2 - head_t1

      get_bytes = int(r_get.headers['Content-Length'])

      img_t1 = time.perf_counter()

      # do a try / catch on "OSError: Truncated File Read"
      try:
        img = Image.open(BytesIO(r_get.content))

        w, h = img.size
        img_t2 = time.perf_counter()

        f_bytes = max(head_bytes, get_bytes)

        toc = time.perf_counter()
        perf_total = toc - tic

        # print('\n#################################')
        # print(f'Asset ID:   {ele[0]}')
        # print(f'URL:        {ele[6]}')
        # print(f'width:      {w}')
        # print(f'height:     {h}')
        # print(f'head bytes: {head_bytes}')
        # print(f'get bytes:  {get_bytes}')
        # print(f'final size: {f_bytes}')
        # print('output line: ')
        # print(f'{line.strip()},{w},{h},{f_bytes}')

        # logging.warning(f'URL={ele[7]}, get_time={get_time:.3f},
        # perf_get={get_time2:.3f}, head_time={head_time:.3f},
        # perf_head={head_time2:.3f}, perf_total={perf_total:.3f},
        # total_time={(head_time+get_time):.3f}, width={w}, height={h}, bytes={f_bytes}')

        # final output:
        # assetid,accountid,datacenter,storagecluster,migrationstatus,imagesource,imagesourceurl,width,height,bytes
        output_handle.write(f'{line.strip()},{w},{h},{f_bytes}\n')
      except OSError:
        bad_handle.write(line)
        continue
      except Exception as e:
        print(f"Error '{e.message}' occurred. Arguments '{e.args}', message: {e.message}.")
        bad_handle.write(line)
        continue

  bad_handle.close()
  output_handle.close()
  work_handle.close()

  # delete error file if it is empty:
  error_file_is_empty = False
  with gzip.open(f"work/{bucket}/bad/{data_file}", 'rb') as f:
    data = f.read(1)
    if len(data) == 0:
      error_file_is_empty = True

  if error_file_is_empty:
    os.remove(f"work/{bucket}/bad/{data_file}")

  os.remove(f"work/{bucket}/active/{data_file}")
else:
  print(f"Stop file ({STOP}) exists. Exiting")
  sys.exit()
