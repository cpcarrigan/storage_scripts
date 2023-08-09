# README #

### What is this repository for? ###
This repo is for lightweight scripts that used for managing and manipulating 
object storage, openstack compute and 


### Scripts and what they do ###

* [bulk s3 deletions](batch_deletions/s3_bulk_delete.py) - takes a container name and a file with S3 objects to delete, batches them and deletes. 
See full documentation [here](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_objects.html)

* [dedupe versions](dedupe/dedupe_batch_mode.py) - takes a directory, reads in a list of objects, figures out which versions should be kept and which should be deleted, generates a list of objects with versionIds to delete.
* [VM image usage](pf9/image_usage_count.py) - pull list of images from openstack, count the number of times used, uses 'project.txt' as a prexisting list of projects to query against.
* [delete old TN files](unused_tn/tn_rm_batch.py) - delete unused thumbnail images based on name format from object storage (Openstack swift protocol).


