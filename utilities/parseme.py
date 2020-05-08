#!/usr/local/bin/python

import requests

# quick script to check all the logins in a particular property file

props = open("/Users/carrigan/git/tnt32/file-migration/src/resources/openstack.properties","r")
for line in props:
    if line.startswith('#') or line.startswith('\n'):
        next
    else: 
        # print(line)
        name, data = line.strip().split("=")
        tenant, password, url = data.split(',')
        # url = server + '/v1/' + account
        http = requests.get(url, headers={"X-Storage-User":tenant, "X-Storage-Pass":password })
        if http.status_code == 200:
            print("good - name: '" + name + "' tenant: '" + tenant  + "' password: '" + password + "' url: " + url )
        else:
            print("bad  - name: '" + name + "' tenant: '" + tenant  + "' password: '" + password + "' url: " + url )
props.close()
