#/bin/bash

cp -r ../../raft .
docker-compose build
rm -r raft