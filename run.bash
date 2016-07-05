#!/bin/bash -x
set -e

url=mongodb://localhost:27017/test
count=$(mongo $url --quiet --eval 'db.cmsProviderLocationsOriginal.count()')
limit=100000
thresh=10000
runCount=$(((count / limit) + (count % limit > 0)))
for i in `seq 1 $runCount`;
do
  skip=$(((i - 1) * limit))
  npm run start -- --url=$url --skip=$skip --limit=$limit --thresh=$thresh
done
