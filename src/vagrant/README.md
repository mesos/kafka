[![Build Status](https://travis-ci.org/mesosphere/kafka.svg?branch=master)](https://travis-ci.org/mesosphere/kafka)

Kafka Mesos Framework
======================

Install gradle http://gradle.org/installation

    git clone https://github.com/mesos/kafka
    cd kafka
    ./gradlew jar
    wget https://archive.apache.org/dist/kafka/0.8.1.1/kafka_2.9.2-0.8.1.1.tgz
    MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so java -jar kafka-mesos-*.jar scheduler

