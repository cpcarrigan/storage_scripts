#!/bin/bash

cluster=$1

# hardcoding directories is not right ...
cd /home/ccarrigan/git/misc/storage_scripts/utilities
for tenant in cvs-ua-prod snapfish uass walgreens walgreens-ua
  do echo ${cluster} ${tenant}
    ./get_container_stats.py ${cluster} ${tenant} ../data/${cluster}-${tenant}-container-list
  done
