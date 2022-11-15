#!/bin/bash

set -o errexit
set -o nounset

min_ago=120

find work/active -type f -mmin +${min_ago} -exec mv '{}' work/ready \;
