#!/usr/bin/python

import configparser
import re
import requests
import sys

# Purpose: delete specific range of containers in a cluster that are empty

# cluster = 'swiftbuckets'
# tenant = 'cvs-ua-qa'
# container_prefix = 'hires_'
# # start = 9999
# end   = 100001

cluster = sys.argv[1]
tenant = sys.argv[2]
container_prefix = sys.argv[3]
start = sys.argv[4]
end = sys.argv[5]


config = configparser.ConfigParser()
config.read("../conf/setup.ini")
tenant_user = config[cluster + '-' + tenant]['user']
tenant_pass = config[cluster + '-' + tenant]['password']

server = 'http://' + cluster + '.sf-cdn.com'

# get cluster/tenant auth token
http_sess = requests.session()
http_resp = http_sess.get(server + "/auth/v1.0", headers={"X-Auth-User":tenant_user, "X-Auth-Key":tenant_pass })
http_auth = http_resp.headers["X-Auth-Token"]
# print(server + "/v1/" + tenant + " auth token: " + http_auth)

# session headers:
# DEBUG:swiftclient:RESP HEADERS: {'Content-Type': 'application/json;
# charset=utf-8', 'X-Container-Object-Count': '536992',
# 'X-Container-Meta-File-Compressed': '1587522505667', 'X-Container-Read':
# '.r:*', 'X-Container-Bytes-Used': '78043854021', 'X-Timestamp':
# '1368477626.92731', 'Accept-Ranges': 'bytes', 'Content-Length': '0',
# 'X-Storage-Policy': 'three-copy', 'X-Trans-Id':
# 'txd7bb14f61e16484f924f8-005ea8d15a', 'X-Openstack-Request-Id':
# 'txd7bb14f61e16484f924f8-005ea8d15a', 'Date': 'Wed, 29 Apr 2020 00:59:06
# GMT'}

# {'Content-Length': '0', 'X-Container-Object-Count': '1', 'Date': 'Thu, 13 Feb 2020 01:56:47 GMT', 'Accept-Ranges': 'bytes', 'X-Trans-Id': 'tx82d93fc08580442c8eae4-005e44acdf', 'X-Storage-Policy': '3copy', 'Last-Modified': 'Wed, 15 Jan 2020 00:39:49 GMT', 'Connection': 'keep-alive', 'X-Timestamp': '1579048788.66572', 'X-Container-Read': '.r:*', 'X-Container-Bytes-Used': '5162463', 'X-Container-Sharding': 'False', 'Content-Type': 'application/json; charset=utf-8', 'X-Openstack-Request-Id': 'tx82d93fc08580442c8eae4-005e44acdf' }

for i in range(start, end):
  container = container_prefix + str(i)
  url = server + '/v1/' + tenant + '/' + container
  http_resp = http_sess.head(url, headers={"X-Auth-Token":http_auth})
  # print(http_resp.headers)
  # print(url + " - " + str(http_resp.status_code))
  if http_resp.status_code == 204 and http_resp.headers['X-Container-Object-Count'] == '0' and http_resp.headers['X-Container-Bytes-Used'] == '0':
    print "Deleting: " + url
    http_resp = http_sess.delete(url, headers={"X-Auth-Token":http_auth})
    # print '####################'
