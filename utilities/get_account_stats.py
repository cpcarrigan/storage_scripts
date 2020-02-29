#!/usr/bin/python

import configparser
import re
import requests
import sys

# Purpose: get stats from a list of accounts within a cluster
# How to invoke: ./get_account_stats.py cluster_short_name
# Example: ./get_account_stats.py s1 snapfish s1-snapfish_uploads-container-list
# Output: cluster-tenant-stats.csv aka s1-snapfish-stats.csv

cluster = sys.argv[1]
tenant = 'ssmigration'

config = configparser.ConfigParser()
config.read("../conf/setup.ini")
tenant_user = config[cluster + '-' + tenant]['user']
tenant_pass = config[cluster + '-' + tenant]['password']

server = 'http://' + cluster + '.sf-cdn.com'

csv_f = "../data/" + cluster + "-account-stats.csv"
f = "../data/" + cluster + "-account-list.txt" 

# get cluster/tenant auth token
http_sess = requests.session()
http_resp = http_sess.get(server + "/auth/v1.0", headers={"X-Auth-User":tenant_user, "X-Auth-Key":tenant_pass })
http_auth = http_resp.headers["X-Auth-Token"]
# print(server + "/v1/" + tenant + " auth token: " + http_auth)

# session headers:
# {'X-Account-Storage-Policy-Three-Copy-Object-Count': '1124964', 'X-Account-Object-Count': '1124964', 'Connection': 'keep-alive', 'Content-Length': '0', 'X-Account-Storage-Policy-Three-Copy-Bytes-Used': '36202309965826', 'X-Account-Storage-Policy-Three-Copy-Container-Count': '1', 'X-Timestamp': '1366635649.47829', 'X-Trans-Id': 'txbc6b5a50e9ab4c1c88ffe-005e59a294', 'Date': 'Fri, 28 Feb 2020 23:30:28 GMT', 'X-Account-Bytes-Used': '36202309965826', 'X-Account-Container-Count': '1', 'Content-Type': 'text/plain; charset=utf-8', 'Accept-Ranges': 'bytes'}

account_list = open(f,"r")
csv = open(csv_f,"a")
csv.write("url,account,object-count,bytes,container-count\n")
for account_line in account_list:
  account = account_line.strip()
  url = server + '/v1/' + account
  http_resp = http_sess.head(url, headers={"X-Auth-Token":http_auth})
  # print(http_resp.headers)
  # print(url + " - " + str(http_resp.status_code))
  if http_resp.status_code == 204:
    csv.write(url + "," + account + "," + http_resp.headers['X-Account-Object-Count'] + "," + http_resp.headers['X-Account-Bytes-Used'] + "," + http_resp.headers['X-Account-Container-Count'] + "\n")
  else:
    print("bad response " + url)

account_list.close()
csv.close()
