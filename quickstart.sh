#!/bin/bash

#
# This script deploys Mesos Kafka on a minimesos cluster and adds a single broker
#

command -v minimesos >/dev/null 2>&1 || { echo "minimesos is not found. You can install it by running 'curl -sSL https://minimesos.org/install | sh'. For more info see https://www.minimesos.org" >&2; exit 1; }

cp kafka-mesos.sh /tmp
cp kafka-mesos*.jar /tmp

if [ "$1" == "" ]; then
  echo "Usage: quickstart.sh <ipaddress>"
  exit 1
fi;

IP=$1

echo "Launching minimesos cluster..."
minimesos up

echo "Exporting minimesos environment variables..."
$(minimesos)

MASTER_HOST_PORT=$(echo $MINIMESOS_MASTER | cut -d/ -f3)

echo "Creating kafka-mesos.properties file in /tmp..."
cat << EOF > /tmp/kafka-mesos.properties
storage=file:kafka-mesos.json
master=$MINIMESOS_ZOOKEEPER/mesos
zk=$MASTER_HOST_PORT
api=http://$IP:7000
EOF

echo "Installing Mesos Kafka..."
ZK_HOST_PORT=$(echo $MINIMESOS_ZOOKEEPER | cut -d/ -f3)
docker run -d -p 7000:7000 `whoami`/kafka-mesos --master=$MINIMESOS_ZOOKEEPER/mesos --api=http://$IP:7000 --zk=$ZK_HOST_PORT 2>&1 > /dev/null

echo "Wait until Mesos Kafka API is available..."
while ! nc -z $IP 7000; do   
  sleep 0.1 # wait for 1/10 of the second before check again
done
echo "Mesos Kafka API is available..."

echo "Adding broker 0..."
cd /tmp
./kafka-mesos.sh broker add 0
cd - 2>&1 > /dev/null

echo "Starting broker 0..."
cd /tmp
./kafka-mesos.sh broker start 0
cd - 2>&1 > /dev/null

minimesos state > state.json
BROKER_IP=$(cat state.json | jq -r ".frameworks[0].tasks[] | select(.name=\"broker-0\").statuses[0].container_status.network_infos[0].ip_address")
echo "broker-0 IP is $BROKER_IP"
