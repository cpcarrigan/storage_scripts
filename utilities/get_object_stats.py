#!/usr/bin/python

import configparser
import re
import requests
import sys

# Purpose: get stats from a list of objects inside the same cluster, tenant, and containers
# How to invoke: ./get_object_stats.py cluster_short_name tenant file_list_of_containers
# Example: ./get_object_stats.py s1 snapfish s1-snapfish_uploads-container-list
# Output: cluster-tenant-object-stats.csv aka s1-snapfish-object-stats.csv

cluster = sys.argv[1]
tenant = sys.argv[2]
f = sys.argv[3]

config = configparser.ConfigParser()
config.read("../conf/setup.ini")
tenant_user = config[cluster + '-' + tenant]['user']
tenant_pass = config[cluster + '-' + tenant]['password']

server = 'http://' + cluster + '.sf-cdn.com'

csv_f = "../data/" + cluster + "-" + tenant + "-containers.csv"

# get cluster/tenant auth token
http_sess = requests.session()
http_resp = http_sess.get(server + "/auth/v1.0", headers={"X-Auth-User":tenant_user, "X-Auth-Key":tenant_pass })
http_auth = http_resp.headers["X-Auth-Token"]
# print(server + "/v1/" + tenant + " auth token: " + http_auth)

# object headers:
# {'Content-Length': '50006768', 'Accept-Ranges': 'bytes', 'Last-Modified': 'Fri, 24 Jan 2020 21:03:53 GMT', 'Connection': 'keep-alive', 'Etag': '3e9ae6afe3d9d9ece630a1b28a41f78b', 'X-Timestamp': '1579899832.31398', 'X-Trans-Id': 'tx960a0518cd314343b5113-005e59b5ed', 'Date': 'Sat, 29 Feb 2020 00:53:01 GMT', 'Content-Type': 'application/octet-stream', 'X-Openstack-Request-Id': 'tx960a0518cd314343b5113-005e59b5ed'}

container_list = open(f,"r")
csv = open(csv_f,"a")
csv.write("url,tenant,container,bytes,last-modified\n")
for container_line in container_list:
  container = container_line.strip()
  url = server + '/v1/' + tenant + '/' + container
  container_resp = http_sess.get(url + "?limit=100", headers={"X-Auth-Token":http_auth})
  print(container_resp.headers)
  print(url + " - " + str(container_resp.status_code))
  for obj in container_resp:
    obj_url = url + '/' + obj.strip()
    obj_head = http_sess.head(obj_url, headers={"X-Auth-Token":http_auth})
    if obj_head.status_code != 404 and int(obj_head.headers['Content-Length']) > 0:
      print(obj_head.headers)
      print(obj_url + " - " + str(obj_head.status_code))
      csv.write(obj_url + "," + tenant + "," + container + "," + obj_head.headers['Content-Length'] + "," + obj_head.headers['X-Timestamp'] + "\n")

container_list.close()
csv.close()
