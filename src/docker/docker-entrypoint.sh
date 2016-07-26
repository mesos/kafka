#!/bin/bash
set -e -x

if [ $1 == "scheduler" ]; then
  trap 'echo trap; kill -TERM $PID' TERM INT
  /usr/bin/java -jar /opt/kafka-mesos/kafka-mesos-*.jar "$@" &
  PID=$!
  wait $PID
  trap - TERM INT
  wait $PID
  EXIT_STATUS=$?
else
  exec "$@"
fi
