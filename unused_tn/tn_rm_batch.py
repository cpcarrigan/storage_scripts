#!/usr/bin/python

import configparser
import re
import requests
import sys
import timeit


# Purpose: delete unused jpeg formats beginning with 'tn/' files from containers
# How to invoke: ./tn-rm-batch.py cluster_short_name tenant container_name
# Example: ./tn-rm-batch.py s1 snapfish snapfish_uploads23
# How it works: script will get 10k files in a container that begin with 'tn/',
# then validate they are the right files to delete, bulk delete them and
# run through this process until there are less than 10k tn files in the
# response

cluster = sys.argv[1]
tenant = sys.argv[2]
container = sys.argv[3]

batch_size = 1000

if sys.argv[4]:
    batch_size = int(sys.argv[4])

server = 'http://' + cluster + '.sf-cdn.com'

config = configparser.ConfigParser()
config.read("../conf/setup.ini")
tenant_user = config[cluster + '-' + tenant]['user']
tenant_pass = config[cluster + '-' + tenant]['password']

# get auth token
r = requests.get(server + "/auth/v1.0", headers={"X-Auth-User":tenant_user, "X-Auth-Key":tenant_pass })
auth = r.headers["X-Auth-Token"]
print("Auth Token: " + auth)

run_flag = True

i = 1
if (run_flag == True):
    print("Run: #" + str(i))
    i += 1
    # setup a session
    sess = requests.session()
    # get 10k objects that have prefix == 'tn/':
    resp = sess.get(server + '/v1/' + tenant + '/' + container + '/?prefix=tn/', headers={"X-Auth-Token":auth})
    # list of files to delete:
    to_delete = ''
    print (len(resp.text.splitlines()))
    if (len(resp.text.splitlines()) < (batch_size - 1000)):
        run_flag = False

    for line in resp.text.splitlines():
        # match specific pattern: tn/0010/471/443/UPLOAD/0477478957016tn.jpg
        tn_pattern1 = re.compile(r"tn/\d{4}/\d{3}/\d{3}/UPLOAD/\d{13}tn.jpg")
        # match specific pattern: tn/0507/274/125/0507274125016tn.jpg
        tn_pattern2 = re.compile(r"tn/\d{4}/\d{3}/\d{3}/\d{13}tn.jpg")
        #                         tn/0011/465/289/UPLOAD/6a319771-b994-4b0a-9975-8fa1fedfad14tn.jpg
        # match specific pattern: tn/0008/112/886/UPLOAD/720e1fd3-5bff-43ca-a685-d2ec35f09a6ctn.jpg
        tn_pattern3 = re.compile(r"tn/\d{4}/\d{3}/\d{3}/UPLOAD/[a-z0-9-]+tn.jpg")
        # if matched, add it to the deletion list
        if tn_pattern1.match(line):
            to_delete += container + '/' + line + '\n'
        elif tn_pattern2.match(line):
            to_delete += container + '/' + line + '\n'
        elif tn_pattern3.match(line):
            to_delete += container + '/' + line + '\n'
        else: 
            print(line + " - no match")

    # print(r.headers)
    if (len(to_delete) > (batch_size - 100) or run_flag == False):
        # code_to_test = """
        del_resp = sess.delete(server + '/v1/' + tenant + '/?bulk-delete', data=to_delete, headers={"X-Auth-Token":auth, "X-Storage-Url": server + '/v1/' + tenant, "Content-Type":"text/plain"})
        # """
        # elapsed_time = timeit.timeit(code_to_test)/100
        # print(elapsed_time)
        # print(del_resp.content) 
