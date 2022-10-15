#!/usr/bin/python3

import gzip
import os
import sys


gzip_file = sys.argv[1]

print(f"file to mess with/ {gzip_file}")

# take filename from input
# gunzip it - into mem or file?
# strip out the lines with HR
# write a new file, delete the old

gz_old = gzip.open(gzip_file, 'rt')
gz_new = gzip.open(gzip_file + '.tmp', 'wt')


for line in gz_old:
  ele = line.strip().split(',')
  
  if ele[5] == 'OppositeDataCenterHRValid' or \
     ele[5] == 'StorageAPIHRValid':
     continue
  else:
    gz_new.write(line)


gz_new.close()
gz_old.close()

os.rename(gzip_file+'.tmp', gzip_file)
