#!/usr/bin/python

import requests
from PIL import Image
from io import BytesIO

f_data = open('test.txt','r')

for line in f_data:
  ele = line.split()

  # url = 'http://s1.sf-cdn.com/v1/uass/olowres_7677/nfmARON1k_dcYtAUUqfdj9lvAF71gZmshc0MpJJFgwo.jpg'
  url = ele[7]
  r_headers = {'Range': 'bytes=0-64000'}

  r_get = requests.get(url, headers=r_headers)

  if r_get.status_code == 206:
    head_bytes = 0
    if r_get.headers['Content-Length'] == '64001':
      r_head = requests.head(url)
      head_bytes = r_head.headers['Content-Length']

    img = Image.open(BytesIO(r_get.content))
    w, h = img.size

    print('\n#################################')
    print('Asset ID:   ', ele[1])
    print('URL:        ', ele[7])
    print('width:      ', w)
    print('height:     ', h)
    print('head bytes: ', head_bytes)
    print('get bytes:  ', r_get.headers['Content-Length'])
