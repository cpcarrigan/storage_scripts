#!/usr/bin/python

import configparser
import re
import requests
import sys
import glob
from prometheus_client import CollectorRegistry, Gauge, pushadd_to_gateway, push_to_gateway

# Purpose: push stats into grafana from a list of accounts within a cluster
# How to invoke: ./push_account_stats.py

tenant = 'ssmigration'
push_gw = 'prom01.tnt6-zone1.aus2:9091'

# echo "some_metric 3.14" | curl --data-binary @- http://localhost:9091/metrics/job/some_job

# grab the list of accounts to pull for each cluster from the data dir
files = glob.glob("../data/*-account-list.txt")

http_sess = requests.session()

# walk through each cluster's list of accounts
for k in files:
  # get just the filename
  filename = k.split('/')[-1]

  # extract clustername from filename
  cluster = filename.split('-')[0]

  # get user & password for cluster
  config = configparser.ConfigParser()
  config.read("../conf/setup.ini")
  tenant_user = config[cluster + '-' + tenant]['user']
  tenant_pass = config[cluster + '-' + tenant]['password']


  # get auth token for just this cluster
  server = 'http://' + cluster + '.sf-cdn.com'
  http_resp = http_sess.get(server + "/auth/v1.0", headers={"X-Auth-User":tenant_user, "X-Auth-Key":tenant_pass })
  http_auth = http_resp.headers["X-Auth-Token"]
  # print(server + "/v1/" + tenant + " auth token: " + http_auth)

  # get the list of accounts in a cluster to get stats for:
  account_list = open(k,"r")

  # walk through each account in cluster:
  for account_line in account_list:
    account = account_line.strip()
    url = server + '/v1/' + account

    # get account stats:
    http_resp = http_sess.head(url, headers={"X-Auth-Token":http_auth})
    # sample response:
    # {'X-Account-Storage-Policy-Three-Copy-Object-Count': '1124964', 'X-Account-Object-Count': '1124964', 'Connection': 'keep-alive', 'Content-Length': '0', 'X-Account-Storage-Policy-Three-Copy-Bytes-Used': '36202309965826', 'X-Account-Storage-Policy-Three-Copy-Container-Count': '1', 'X-Timestamp': '1366635649.47829', 'X-Trans-Id': 'txbc6b5a50e9ab4c1c88ffe-005e59a294', 'Date': 'Fri, 28 Feb 2020 23:30:28 GMT', 'X-Account-Bytes-Used': '36202309965826', 'X-Account-Container-Count': '1', 'Content-Type': 'text/plain; charset=utf-8', 'Accept-Ranges': 'bytes'}


    # if valid response, push data for object count, object bytes, and container count
    # into pushgateway
    if http_resp.status_code < 400:
      registry = CollectorRegistry()

      gauge_objects = Gauge('object_count', 'Count of objects within a tenant', ["vip","tenant"], registry=registry) 
      gauge_objects.labels(server, account).set(http_resp.headers['X-Account-Object-Count'])

      gauge_bytes = Gauge('bytes_count', 'Count of bytes within a tenant',["vip", "tenant"], registry=registry) 
      gauge_bytes.labels(server, account).set(http_resp.headers['X-Account-Bytes-Used'])

      gauge_containers = Gauge('container_count', 'Count of containers within a tenant',["vip", "tenant"], registry=registry) 
      gauge_containers.labels(server, account).set(http_resp.headers['X-Account-Object-Count'])

      pushadd_to_gateway(push_gw, job=cluster + '-' + account, registry=registry)

    else:
      print("bad response " + url + " resp: " + str(http_resp.status_code))
  
  account_list.close()
