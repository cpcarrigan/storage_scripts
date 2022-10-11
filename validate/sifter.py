#!/usr/bin/python3
import gzip
import os
import sys

# get file name from 1st argument, decompress a file, parse it, validate the entries
# split it into workable chunks based on cluster
MAX_LINES = 5000

bucket = { 'esthreecos-ak.sf-cdn.com': 'other',
                'images1.snapfish.com': 'other',
                'images2.snapfish.com': 'other',
                's1.sf-cdn.com': 's1',
                's2.sf-cdn.com': 's2',
                'swiftbuckets1.sf-cdn.com': 's1',
                'swiftbuckets3.sf-cdn.com': 's1',
                'swiftbuckets4.sf-cdn.com': 's2',
                'swiftbuckets7.sf-cdn.com': 's2',
                'swiftbuckets.sf-cdn.com': 'sb'}


if (len(sys.argv) < 2 or len(sys.argv) > 2):
  print("instructions on use:")
  print("./sifter.py data-file.csv.gz")
  sys.exit()

csv_file = sys.argv[1]
bad_file = csv_file

csv_obj = gzip.open('csv-ready/' + csv_file, 'rt')
bad_obj = gzip.open('csv-bad/' + bad_file, 'wt')

# probably want to create it as a 'tmp' file, and then move it. Will make it
# more 'atomic' so that the get_image_stats won't start using it until it is
# ready
#
# dict of array of line counter, file counter, gzip file_object
track = { 'other': [0, 0, gzip.open('work/other/ready/' + csv_file + '.tmp', 'wt')],
          's1':    [0, 0, gzip.open('work/s1/ready/' + csv_file + '.tmp', 'wt')],
          's2':    [0, 0, gzip.open('work/s2/ready/' + csv_file + '.tmp', 'wt')],
          'sb':    [0, 0, gzip.open('work/sb/ready/' + csv_file + '.tmp', 'wt')],
          }

for line in csv_obj:
  ele = line.strip().split(',')

  # 7 elements (native csv):
  # assetid,accountid,datacenter,storagecluster,migrationstatus,imagesource,imagesourceurl
  # 114948587022,658258022,AUS,S1,MIGRATED,TnlRefOLRValid,http://s2.sf-cdn.com/v1/uass/olowres_9964/44F8iFLFL-N44q0e8gz-ENlvAF71gZmshc0MpJJFgwo.jpg

  # valid if has a number for assetid, accountid, imagesource is not
  # OppositeDataCenterHRValid or StorageAPIHRValid, begins w/ 'http://', ends with 'jpg' 
  if ele[0].isdigit() is True and \
     ele[1].isdigit() is True and \
     ele[5] != 'OppositeDataCenterHRValid' and \
     ele[5] != 'StorageAPIHRValid' and \
     ele[6].startswith('http://') and \
     ele[6].endswith('jpg'):

    # ele[6] (the URL field) looks like 'http://swiftbuckets.sf-cdn.com/v1/uass/olowres_635/uLPYvi0HzQtsEjYBVBjAQtlvAF71gZmshc0MpJJFgwo.jpg'
    # split out all elements of the URL field by '/':
    url = ele[6].split('/')
    # print(url)
    # get just hostname (eg, 'swiftbuckets.sf-cdn.com') from url array
    host = url[2]

    # write line to 'work/bucket/ready/csv_file.csv.gz'
    # eg: work/s1/ready/ASDFL123HJ234KH-42.gz
    try:
      track[bucket[host]][2].write(line)

      # incr line count by 1
      track[bucket[host]][0] += 1

      # if MAX_LINES exceeded, roll over to a new file
      if track[bucket[host]][0] > MAX_LINES:

        # reset line counter
        track[bucket[host]][0] = 0
        # close current file
        track[bucket[host]][2].close()

        # move file from tmp to done
        if track[bucket[host]][1] == 0:
          # zeroeth file, mv that, otherwise:
          os.rename('work/' + bucket[host] + '/ready/' + csv_file.strip('.csv.gz') + '.csv.gz.tmp',
                    'work/' + bucket[host] + '/ready/' + csv_file.strip('.csv.gz') + '.csv.gz')
        else:
          os.rename('work/' + bucket[host] + '/ready/' + csv_file.strip('.csv.gz') + '-' + str(track[bucket[host]][1]) + '.csv.gz.tmp',
                    'work/' + bucket[host] + '/ready/' + csv_file.strip('.csv.gz') + '-' + str(track[bucket[host]][1]) + '.csv.gz')


        # add one to file counter
        track[bucket[host]][1] += 1

        # open new file
        track[bucket[host]][2] = gzip.open('work/' + bucket[host] + '/ready/' + csv_file.strip('.csv.gz') + '-' + str(track[bucket[host]][1]) + '.csv.gz.tmp', 'wt')
        # part-00039-7efdd472-1f61-4729-9b27-3d34dcd53850-c000.csv.gz
        # part-00039-7efdd472-1f61-4729-9b27-3d34dcd53850-c000-1.csv.gz
        # part-00039-7efdd472-1f61-4729-9b27-3d34dcd53850-c000-2.csv.gz
        # part-00039-7efdd472-1f61-4729-9b27-3d34dcd53850-c000-3.csv.gz
        # part-00039-7efdd472-1f61-4729-9b27-3d34dcd53850-c000-4.csv.gz
        # part-00039-7efdd472-1f61-4729-9b27-3d34dcd53850-c000-5.csv.gz
        # part-00039-7efdd472-1f61-4729-9b27-3d34dcd53850-c000-6.csv.gz
        # part-00039-7efdd472-1f61-4729-9b27-3d34dcd53850-c000-7.csv.gz
        # part-00039-7efdd472-1f61-4729-9b27-3d34dcd53850-c000-8.csv.gz
        # part-00039-7efdd472-1f61-4729-9b27-3d34dcd53850-c000-9.csv.gz
        # part-00039-7efdd472-1f61-4729-9b27-3d34dcd53850-c000-10.csv.gz
    except KeyError:
      bad_obj.write(line)

  else:
    bad_obj.write(line)

# close all file objects
for k in track:
  track[k][2].close()
  # ugh, assumes not on zeroeth file:
  try:
    os.rename('work/' + k + '/ready/' + csv_file.strip('.csv.gz') + '-' + str(track[k][1]) + '.csv.gz.tmp',
              'work/' + k + '/ready/' + csv_file.strip('.csv.gz') + '-' + str(track[k][1]) + '.csv.gz')
  except FileNotFoundError:
    continue

bad_obj.close()
csv_obj.close()

# delete error file if it is empty:
error_file_is_empty = False
# bad_obj = open('csv-bad/' + bad_file, 'w')
with gzip.open(f"csv-bad/{bad_file}", 'rb') as f:
  data = f.read(1)
  if len(data) == 0:
    error_file_is_empty = True

if error_file_is_empty:
  os.remove(f"csv-bad/{bad_file}")


# move csv file from ready to done
os.rename('csv-ready/' + sys.argv[1], 'csv-done/' +  sys.argv[1])
