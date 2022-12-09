#!/usr/bin/env bash

set -o errexit
set -o nounset


fixgz=$1

echo "gz: ${fixgz}"

# gunzip
# strip out double quotes, swap bad hostnames
#
# hostname / storage mappings
# bucket = { 'esthreecos-ak.sf-cdn.com': 'other',
# 'images1.snapfish.com': 'other',
# 'images2.snapfish.com': 'other',
# 's1.sf-cdn.com': 's1',
# 's2.sf-cdn.com': 's2',
# 'swiftbuckets1.sf-cdn.com': 's1',
# 'swiftbuckets3.sf-cdn.com': 's1',
# 'swiftbuckets4.sf-cdn.com': 's2',
# 'swiftbuckets7.sf-cdn.com': 's2',
# 'swiftbuckets.sf-cdn.com': 'sb'}
#
# sort by column 3 -- sort -k 3 ?
# gzip again

#gunzip "${fixgz}"

# -fixcsv=`echo ${fixgz} | sed -e 's/.gz$//'`
fixcsv="${fixgz/.gz/}"

echo "csv: ${fixcsv}"

sed -i 's/"//g' "${fixcsv}"

sed -i 's/swiftbuckets1.sf-cdn.com/s1.sf-cdn.com/' "${fixcsv}"
sed -i 's/swiftbuckets3.sf-cdn.com/s1.sf-cdn.com/' "${fixcsv}"
sed -i 's/swiftbuckets4.sf-cdn.com/s2.sf-cdn.com/' "${fixcsv}"
sed -i 's/swiftbuckets7.sf-cdn.com/s2.sf-cdn.com/' "${fixcsv}"

# http://images2.snapfish.com/ldm70000/migration_365138/hr_0004_245_045_0004245045387.jpg
# https://s1.sf-cdn.com/v1/snapfish/migration_365138/hr_0004_245_045_0004245045387.jpg
sed -i 's/images2.snapfish.com\/ldm70000/s1.sf-cdn.com\/v1\/snapfish/' "${fixcsv}"

# http://images1.snapfish.com/dm70000/migration_36510/hr_0004_273_300_0004273300343.jpg
# http://s1.sf-cdn.com/v1/snapfish/migration_36510/hr_0004_273_300_0004273300343.jpg
sed -i 's/images1.snapfish.com\/dm70000/s1.sf-cdn.com\/v1\/snapfish/' "${fixcsv}"

# http://images2.snapfish.com/ldm80001/hires_2053/a5d76c3a-f3e0-4e0c-ba44-787a8d4281c7.jpg
# http://swiftbuckets.sf-cdn.com/v1/uass/hires_2053/a5d76c3a-f3e0-4e0c-ba44-787a8d4281c7.jpg
sed -i 's/images2.snapfish.com\/ldm80001/swiftbuckets.sf-cdn.com\/v1\/uass/' "${fixcsv}"

# http://images2.snapfish.com/ldm60002/encoding85/ee03ded5-2b4b-4f01-8e4c-edd71bdbd4ca/0469752425027.JPG
# http://swiftbuckets.sf-cdn.com/v1/encoding/encoding85/ee03ded5-2b4b-4f01-8e4c-edd71bdbd4ca/0469752425027.JPG
sed -i 's/images2.snapfish.com\/ldm60002/swiftbuckets.sf-cdn.com\/v1\/encoding/' "${fixcsv}"

# http://images2.snapfish.com/ldm60000/snapfish_uploads1555/hr/0624/882/469//0624882469011.jpg
# http://swiftbuckets.sf-cdn.com/v1/snapfish/snapfish_uploads1555/hr/0624/882/469/0624882469011.jpg
sed -i 's/images2.snapfish.com\/ldm60000/swiftbuckets.sf-cdn.com\/v1\/snapfish/' "${fixcsv}"

sort -k 3 -t, "${fixcsv}" >> "${fixcsv}.tmp"

mv "${fixcsv}.tmp" "${fixcsv}"
gzip "${fixcsv}"
