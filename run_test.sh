#!/usr/bin/env bash

cd test
echo "===> Changing directory to \"./test\""

docker-compose up -d --build zookeeper kafka

docker-compose up --exit-code-from go-logsink-test
rc=$?
if [[ $rc != 0 ]]
  docker ps -a
  then exit $rc
fi
