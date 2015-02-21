[![Build Status](https://travis-ci.org/mesosphere/kafka.svg?branch=master)](https://travis-ci.org/mesosphere/kafka)

Kafka Mesos Framework
======================

Use Vagrant to get up and running.

1) Install Vagrant [http://www.vagrantup.com/](http://www.vagrantup.com/)  
2) Install Virtual Box [https://www.virtualbox.org/](https://www.virtualbox.org/)  

Install gradle http://gradle.org/installation

    git clone https://github.com/mesos/kafka
    cd kafka
    ./gradlew jar
    wget https://archive.apache.org/dist/kafka/0.8.1.1/kafka_2.9.2-0.8.1.1.tgz
    MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so java -jar kafka-mesos-sandbox-0.1.jar scheduler

