#!/usr/bin/env python3

import datetime
import boto3
import os
import argparse
import logging
from dateutil.parser import parse as parsedate
import time


bucket = 'snapfish-prod-media-raw-snapfish'

parser = argparse.ArgumentParser()
parser.add_argument("prefix", help=f"Pass a prefix/key to dedupe. Assumes the {bucket}")
args = parser.parse_args()
logging.basicConfig(filename='a.log',level=logging.INFO,format='%(asctime)s.%(msecs)03d - %(levelname)s: %(message)s', datefmt='%d-%b-%y %H:%M:%S')
#logging.info("File to import: " + args.filename)
prefix = args.prefix
# prefix = '1f5eff/40758720050-1004851833784070.jpg'

# session = boto3.Session(profile_name='migration')
# logging.warning(session.client('s3').get_caller_identity())

# s3 = boto3.resource('s3')
client = boto3.client('s3')
#versions = s3.Bucket(bucket).list_object_versions.filter(Prefix=key)
# response = s3.Bucket(bucket).object_versions.filter(Prefix=key)
# logging.warning(versions)

resp = client.list_object_versions(Prefix=prefix, Bucket=bucket)

logging.warning(f"Bucket: {bucket}")
logging.warning(f"Key: {prefix}\n")

prev_version = ''
prev_size = ''
# prev_date = ''
#   url_datetime = parsedate(arr_line[2]).astimezone()
#   now_datetime = datetime.datetime.fromtimestamp(time.time()).astimezone()
prev_date = datetime.datetime.fromtimestamp(time.time()).astimezone()

to_del = []

for obj in [*resp['Versions'], *resp.get('DeleteMarkers', [])]:
    logging.warning(f"Key: {obj['Key']}")
    logging.warning(f"VersionId: {obj['VersionId']}")
    logging.warning(f"LastModified: {obj['LastModified']}")
    logging.warning(f"IsLatest: {obj['IsLatest']}")
    logging.warning(f"Size: {obj['Size']}\n")

    if prev_version == '':
        logging.info(f"No previous version stored, storing {obj['VersionId']}\n")
        prev_version = obj['VersionId']
        prev_size = int(obj['Size'])
        prev_date = obj['LastModified']
        next
    else:
        if prev_size > int(obj['Size']):
            logging.warning(f"Saved version: {prev_version} is greater than or equal to {obj['VersionId']}, keeping {prev_version}")
            to_del.append(obj['VersionId'])
        # if sizes are equal, then keep whichever is newer:
        elif prev_size == int(obj['Size']): 
            logging.warning(f"Same size ({prev_size}B), comparing dates - {prev_date} vs {obj['LastModified']}")
            # if stored date is newer, delete current evaluation
            if prev_date >= obj['LastModified']:
                logging.warning(f"Same size, saved version is newer or same date, keeping {prev_version}")
                to_del.append(obj['VersionId'])
            # else, delete the stored version, and save the current evaluation to stored version
            else:
                to_del.append(prev_version)
                logging.warning(f"Same size, current evaluation is newer, keeping: {obj['VersionId']}")
                prev_version = obj['VersionId']
                prev_size = int(obj['Size'])
                prev_date = obj['LastModified']
        else:
            logging.warning(f"Current evaluation is larger, {obj['Size']} vs {prev_size}, keeping current version: {obj['VersionId']}")
            to_del.append(prev_version)
            prev_version = obj['VersionId']
            prev_size = int(obj['Size'])
            prev_date = obj['LastModified']
            # logging.warning(f"{prev_version} is greater than or equal to {obj['VersionId']}, keeping {prev_version}")

logging.warning(f"Will delete: {to_del}\n")
for versionId in to_del:
    logging.warning(f"Delete {versionId} of object {prefix} in bucket {bucket}")
logging.warning(f"################################################")


#for version in versions:
#    obj = version.head()
#    logging.warning(version)
    # logging.warning(obj.head('VersionId'), obj.head('ContentLength'), obj.head('LastModified'))

"""
$ aws s3api list-object-versions --bucket snapfish-prod-media-raw-snapfish --prefix 1f5eff/40758720050-1004851833784070.jpg
{
    "Versions": [
       {
          "ETag": "\"da223358500f6c4574afde9cc5ba3c8b\"",
          "Size": 3223319,
          "StorageClass": "STANDARD_IA",
          "Key": "1f5eff/40758720050-1004851833784070.jpg",
          "VersionId": "8m2R1xp.fhJpKieFDbf2Y4Qkk4PlZJ6c",
          "IsLatest": true,
          "LastModified": "2021-12-04T00:03:34.000Z"
       },
       {
          "ETag": "\"da223358500f6c4574afde9cc5ba3c8b\"",
          "Size": 3223319,
          "StorageClass": "STANDARD_IA",
          "Key": "1f5eff/40758720050-1004851833784070.jpg",
          "VersionId": "HYj13tDAz9Haq4jj8BFQMP1CDxWODkEk",
          "IsLatest": false,
          "LastModified": "2021-11-27T21:34:56.000Z"
       }
    ]
 }
"""
