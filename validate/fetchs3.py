#!/usr/bin/python3

""" Fetch files from S3 per Brent Williams """

import gzip
import os
import shutil
import boto3


#session = boto3.Session(
#  aws_access_key_id=settings.AWS_SERVER_PUBLIC_KEY,
#  aws_secret_access_key=settings.AWS_SERVER_SECRET_KEY,
#  region_name='US-EAST-1'
#)


# aws s3 ls s3://snapfish-prod-media-image-migration/assets_db_repairs/AUS/S1/1d7b4e/


#Initiate S3 Resource
# s3 = session.resource('s3')
s3r = boto3.resource('s3')
s3c = boto3.client('s3')

# Select Your S3 Bucket
my_bucket = s3r.Bucket('snapfish-prod-media-image-migration')

for obj in my_bucket.objects.all():
  # if obj.key.startswith('assets_db_repairs/AUS/S2') and obj.key.endswith('.csv'):
  if obj.key.startswith('assets_db_repairs/SFO') and obj.key.endswith('.csv'):
    print(f"Downloading: {obj.bucket_name}/{obj.key}")
    obj_path = obj.key.split('/')
    csv = obj_path[-1]

    # download the file:
    with open(csv, 'wb') as f_csv:
      s3c.download_fileobj(obj.bucket_name, obj.key, f_csv)

    # compress csv file
    with open(csv, 'rb') as f_in:
      with gzip.open(f"csv-ready/{csv}.gz", 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

    # mv the csv file in s3 to new name so we don't download it twice
    s3r.Object(obj.bucket_name, f"{obj.key}.downloaded").copy_from(CopySource=f"{obj.bucket_name}/{obj.key}")
    s3r.Object(obj.bucket_name, obj.key).delete()

    # delete csv file:
    os.remove(csv)
