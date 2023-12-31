#!/bin/bash

set -o errexit
set -o nounset

csvgz=$1
csv="${csvgz/.gz/}"

echo "about to uncompress ${csvgz}"
gzip --uncompress "${csvgz}"

echo "about to split ${csv}"
split  -a 3 -d -l 100000 "${csv}" "work/ready/${csv}-"

echo "about to compress ${csv}"
gzip "${csv}"

echo "move ${csvgz}  to done"
mv "${csvgz}" ./done/
