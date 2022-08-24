#!/usr/bin/python3

from csv import reader 
import json
import subprocess
import sys

image_vms={}
errors={}

count = 0
if len(sys.argv) > 1:
  count = int(sys.argv[1])

# get image list in json format
os_image_c = subprocess.run(["openstack", "image", "list", "-f", "json"], check=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)
image_json = json.loads(os_image_c.stdout)

# example output:
#   {
#     "ID": "610cb8b7-189f-79b8-a018-1a37c74717bb",
#     "Name": "ubuntu-16.04-server-cloudimg-amd64-disk1.img --DO NOT USE--",
#     "Status": "active"
#   },

# iterate over json of images, add to image_vms dict with key as image name, and value as empty list
for image in image_json:
  # print(f"image name: '{image['Name']}'")
  image_vms.update({image["Name"]: []})

# os_c = subprocess.run(["openstack", "server", "list", "-f", "csv", "-c", "ID", "-c", "Image"], shell=True, check=True, stdout=subprocess.PIPE, universal_newlines=True)
os_server_c = subprocess.run(["openstack", "server", "list", "-f", "json", "--all-projects"], check=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)
server_json = json.loads(os_server_c.stdout)
# print(server_json)

# example output:
#   {
#     "ID": "789c7854-220c-4542-885c-372bacf817cb",
#     "Name": "prd-zk01",
#     "Status": "ACTIVE",
#     "Networks": {
#       "Storage 914-PROD": [
#         "10.132.54.16"
#       ]
#     },
#     "Image": "Ubuntu 18.04 LDAP v1 (no swap) --DO NOT USE--",
#     "Flavor": "m.2-4-20-all"

for vm in server_json:
  # print(f"image name: '{vm['Image']}'")
  try:
    image_vms[vm["Image"]].append(vm["Name"])

  except KeyError:
    # print(f'No matching Image for this host: {vm["Name"]} uuid: {vm["ID"]}')
    errors[vm['ID']] = vm['Name']
    next

# print(image_vms)
print("===============================")
print("Images, counts of vms and hosts")
print("===============================")
test=dict(sorted(image_vms.items(), key=lambda item: len(item[1])))
for k in test:
   print(f"'{k}' used {len(image_vms[k])} times")
   if count > 0 and len(image_vms[k]) > 0 and len(image_vms[k]) <= count:
     print(f"    hosts: {image_vms[k]}")

if errors:
  print("\n===============================")
  print("\nHosts with no image (uid & short_name):")
  for uuid in errors:
    print(f'    {uuid} {errors[uuid]}')
