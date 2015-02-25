[![Build Status](https://travis-ci.org/mesosphere/kafka.svg?branch=master)](https://travis-ci.org/mesosphere/kafka)

Kafka Mesos Framework
======================

Install gradle http://gradle.org/installation

    git clone https://github.com/mesos/kafka
    cd kafka
    ./gradlew jar
    wget https://archive.apache.org/dist/kafka/0.8.1.1/kafka_2.9.2-0.8.1.1.tgz
    MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so java -jar kafka-0.2.jar scheduler

This is an *ALPHA* version. More features will be continued to be added until we cut a stable beta build to release candidate against.

* smart broker.id assignment.

* preservation of broker placement (through constraints and/or new features).

* ability to-do configuration changes.

* rolling restarts (for things like configuration changes).

* scaling up/down utilizing new re-balance decommission.

* smart partition assignmnet via constraints visa vi roles, resources and attributes.

