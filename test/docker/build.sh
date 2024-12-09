#/bin/bash

cp -r ../../raft .
cp -r ../../simulate .
docker-compose build
rm -r raft
rm -r simulate
rm -r ./output