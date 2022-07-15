#!/usr/bin/python3 
import gzip 
import os 
import pandas as pd 
import sys

# get file name from arg $1, decompress a file, parse it, validate the entries
# split it into workable chunks based on cluster
max_lines = 5000

bucket = { 'esthreecos-ak.sf-cdn.com': 'other',
                'images2.snapfish.com': 'other',
                's1.sf-cdn.com': 's1',
                's2.sf-cdn.com': 's2',
                'swiftbuckets1.sf-cdn.com': 's1',
                'swiftbuckets3.sf-cdn.com': 's1',
                'swiftbuckets7.sf-cdn.com': 's2',
                'swiftbuckets.sf-cdn.com': 'sb'}


if (len(sys.argv) < 2):
  print("instructions on use:")
  print("./sifter.py parquet-ready/parquet-file")
  exit()

df = pd.read_parquet(sys.argv[1])
df.to_csv(sys.argv[1] + '.csv')

csv_file = sys.argv[1] + '.csv'
pq_file = sys.argv[1].strip('parquet-ready/')
bad_file = 'parquet-bad/' + pq_file

csv_obj = open(csv_file, 'r')
bad_obj = open(bad_file, 'w')

# dict of line counter, file counter, file_object
track = { 'other': [0, 0, gzip.open('work/other/ready/' + pq_file + '-0.gz','wt')], 
          's1': [0, 0, gzip.open('work/s1/ready/' + pq_file + '-0.gz','wt')], 
          's2': [0, 0, gzip.open('work/s2/ready/' + pq_file + '-0.gz','wt')], 
          'sb': [0, 0, gzip.open('work/sb/ready/' + pq_file + '-0.gz','wt')], 
          }

for line in csv_obj:
  ele = line.strip().split(',')

  # row,assetid,accountid,datacenter,storagecluster,migrationstatus,imagesource,imagesourceurl
  # 0,751447346011,5221243011,SFO,SB,MIGRATED,StorageAPIOLRValid,http://swiftbuckets.sf-cdn.com/v1/uass/olowres_635/uLPYvi0HzQtsEjYBVBjAQtlvAF71gZmshc0MpJJFgwo.jpg

  # valid if has a number for assetid, accountid, begins w/ 'http://', ends with 'jpg'
  
  if ele[1].isdigit() == True and \
     ele[2].isdigit() == True and \
     ele[7].startswith('http://') and \
     ele[7].endswith('jpg'):

    # ele[7] looks like 'http://swiftbuckets.sf-cdn.com/v1/uass/olowres_635/uLPYvi0HzQtsEjYBVBjAQtlvAF71gZmshc0MpJJFgwo.jpg'
    # split out all elements of the URL field by '/':
    url = ele[7].split('/')
    # print(url)
    # get just hostname from url array
    host = url[2]

    # write line to 'work/bucket/ready/pq_file-file_counter.gz'
    # eg: work/s1/ready/ASDFL123HJ234KH-42.gz
    track[bucket[host]][2].write(line)

    # incr line count by 1
    track[bucket[host]][0] += 1

    # if max_lines exceeded, roll over to a new file
    if track[bucket[host]][0] > max_lines:

      # reset line counter
      track[bucket[host]][0] = 0
      # add one to file counter
      track[bucket[host]][1] += 1
      # close current file
      track[bucket[host]][2].close()
      # open new file
      track[bucket[host]][2] = gzip.open('work/' + bucket[host] + '/ready/' + pq_file + '-' + str(track[bucket[host]][1]) + '.gz', 'wt')

  else:
    bad_obj.write(line)

# close all file objects
for k in track:
  track[k][2].close()

bad_obj.close()
csv_obj.close()

# delete old csv file
os.remove(csv_file)
# move parquet file from ready to done
os.rename(sys.argv[1], sys.argv[1].replace('parquet-ready','parquet-done'))
