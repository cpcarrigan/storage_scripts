#!/usr/bin/python

import configparser
import re
import requests
import sys

# Purpose: get stats from a list of containers inside the same tenant and
# cluster
# How to invoke: ./get_container_stats.py cluster_short_name tenant file_list_of_containers
# Example: ./get_container_stats.py s1 snapfish s1-snapfish_uploads-container-list
# Output: cluster-tenant-container-stats.csv aka s1-snapfish-container-stats.csv

if (len(sys.argv) < 5):
  print("instructions on use:")
  print("./reset_container_header.py s1 uass olowres 1 100000")
  print("./reset_container_header.py cluster_name account_name container_name start_range end_range")
  exit()

cluster = sys.argv[1]
tenant = sys.argv[2]
container = sys.argv[3]
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
print(server + "/v1/" + tenant + " auth token: " + http_auth)

# {'Content-Length': '0', 'X-Container-Object-Count': '1', 'Date': 'Thu, 13 Feb 2020 01:56:47 GMT', 'Accept-Ranges': 'bytes', 'X-Trans-Id': 'tx82d93fc08580442c8eae4-005e44acdf', 'X-Storage-Policy': '3copy', 'Last-Modified': 'Wed, 15 Jan 2020 00:39:49 GMT', 'Connection': 'keep-alive', 'X-Timestamp': '1579048788.66572', 'X-Container-Read': '.r:*', 'X-Container-Bytes-Used': '5162463', 'X-Container-Sharding': 'False', 'Content-Type': 'application/json; charset=utf-8', 'X-Openstack-Request-Id': 'tx82d93fc08580442c8eae4-005e44acdf' }

for container_nu in range(int(start),int(end)):
  try:
    url = server + '/v1/' + tenant + '/' + container + '_' + str(container_nu)
    http_resp = http_sess.post(url, headers={"X-Auth-Token":http_auth, "X-Container-Meta-File-Compressed":""})
    # print(http_resp.headers)
    # print(url + " - " + str(http_resp.status_code))

    # what we should send:
    # DEBUG:swiftclient:REQ: curl -i https://s1.sf-cdn.com/v1/uass/olowres_1
    # -X POST -H "Content-Length: 0" -H "X-Container-Meta-File-Compressed: "
    # -H "X-Auth-Token: AUTH_tkad27bb478fdf49ef9a3b5b1379e856f7"

    # refresh the token and try again:
    if http_resp.status_code == 401:
      http_resp = http_sess.get(server + "/auth/v1.0", headers={"X-Auth-User":tenant_user, "X-Auth-Key":tenant_pass })
      http_auth = http_resp.headers["X-Auth-Token"]
      url = server + '/v1/' + tenant + '/' + container + '_' + str(container_nu)
      http_resp = http_sess.post(url, headers={"X-Auth-Token":http_auth, "X-Container-Meta-File-Compressed":""})

    if http_resp.status_code == 204:
#      http_resp = http_sess.head(url, headers={"X-Auth-Token":http_auth})
      print("Fixed container: " + container + '_' + str(container_nu))
    else:
      print("bad response " + url)
  except requests.exceptions.ConnectionError:
    http_resp = http_sess.get(server + "/auth/v1.0", headers={"X-Auth-User":tenant_user, "X-Auth-Key":tenant_pass })
    http_auth = http_resp.headers["X-Auth-Token"]

