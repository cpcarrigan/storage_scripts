#!/usr/bin/python

import sys
import bisect

containerType = ''
tenant = ''
cluster = ''
container = ''
limit = ''
batchSize = ''

if (len(sys.argv) < 3):
  print("instructions on use:")
  print("./find_gaps.py olowres ../data/s1-uass-container.csv")
  print("./find_gaps.py container_type data_file")
  exit()
container = sys.argv[1]
data_file = sys.argv[2]

if container == 'olowres' or container == 'lowres':
  containerType = 'lowres'
  limit = '256'
  batchSize = '4096'
else:
  containerType = 'hires'
  limit = '20'
  batchSize = '1024'

df = open(data_file,"r")
sc = []

for line in df:
  line_arr = line.strip().split(',')
#  print('"'+line_arr[0]+'"')
  if line_arr[0] == 'url':
    continue
  # ['http://s1.sf-cdn.com/v1/uass/lowres_65331', 'uass', 'lowres_65331', '26058', '9911192473', '2copy', 'yes']
  tenant = line_arr[1]
  list1 = line_arr[0].split('/')
#  print(list1)
  host = list1[2].split('.')
#  print(host)
  cluster = host[0]
  if line_arr[6] == 'no' and line_arr[2].startswith(container + '_'):
    # print(line_arr[2])
    blank, nu = line_arr[2].split('_')
    bisect.insort(sc, int(nu))
print(sc)

def print_sc(first, last):
  print('curl -v "http://localhost:8080/migration/command?command=' + containerType \
   + 'Compress&openstackUser=' + tenant \
   + '&dataCenter=' + cluster  \
   + '&container=' + container \
   + '&batchSize=' + batchSize \
   + '&startRange=' + str(first) \
   + '&endRange=' + str(last) \
   + '&threads=2' \
   + '&limit=' + limit + '"')

first = None
last = None
for i in range(len(sc)):
  # print("look at: " + str(sc[i]))
  if first is None:
    first = sc[i]
  if last is None and i+1 < len(sc) and sc[i+1] == first+1:
    # print('insert into last')
    last = sc[i+1]
  elif last is None and i+1 < len(sc) and sc[i+1] != first+1:
    # first not contigous w/ next, but next null
#    print('range of one')
#    print('range: ' + str(first) + ' to ' + str(first))
    print_sc(first, first)
    first = None
    next
  elif i+1 < len(sc) and last+1 == sc[i+1] :
    # print('insert into last')
    last = sc[i+1]
  elif i+1 < len(sc) and last+1 != sc[i+1] :
#    print('first: ' + str(first) + ' last: ' + str(last))
    print_sc(first, last)
    first = None
    last = None
  else:
#    print('first: ' + str(first) + ' last: ' + str(last))
    print_sc(first, last)

print('################')
print('curl -v "http://localhost:8080/migration/command?command=lowresCompress&openstackUser=uass&dataCenter=s2&container=olowres&batchSize=4096&startRange=1&endRange=35001&threads=2&limit=256"')
