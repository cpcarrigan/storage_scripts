#!/usr/bin/env python3

import boto3

bucket = 'snapfish-prod-media-raw-snapfish'
key = '1f5eff/40758720050-1004851833784070.jpg'
s3 = boto3.resource('s3')
versions = s3.Bucket(bucket).object_versions.filter(Prefix=key)

for version in versions:
    obj = version.get()
    print(obj.get('VersionId'), obj.get('ContentLength'), obj.get('LastModified'))
