#!/bin/bash
set -ue

base_dir=../..
tmp_dir=.docker
mkdir -p $tmp_dir

kafka_version=0.8.2.2
scala_version=2.10
docker_tag=`whoami`/kafka-mesos

print_help() {
    echo "Usage: build-image.sh [options]"
    echo "Options:"
    echo " -t <docker-tag-name> - Docker tag name. Default $docker_tag"
    echo " -k <kafka-version>   - Kafka version. Default $kafka_version"
    echo " -h                   - Print this help"
}

# parse args
while getopts ":t:k:h" opt; do
  case $opt in
    t)
      docker_tag=$OPTARG
      ;;
    k)
      kafka_version=$OPTARG
      ;;
    h)
      print_help
      exit 0
      ;;
    \?)
      echo "Invalid option: -$OPTARG"
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument"
      exit 1
  esac
done

# download kafka
kafka_file=kafka_$scala_version-$kafka_version.tgz
wget -nc -P $tmp_dir https://archive.apache.org/dist/kafka/$kafka_version/$kafka_file
find $tmp_dir -type f -name 'kafka*.tgz' -not -name "$kafka_file" -delete

# build kafka-mesos-jar
find $tmp_dir -type f -name 'kafka-mesos*.jar' -delete
cd $base_dir && ./gradlew jar && cd -
cp $base_dir/kafka-mesos*.jar $tmp_dir
cp $base_dir/kafka-mesos*.sh $tmp_dir

# build docker image
sudo="sudo"
if docker info &> /dev/null; then sudo=""; fi # skip sudo if possible
$sudo docker build -t $docker_tag .
