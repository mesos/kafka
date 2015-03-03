Kafka Mesos Framework
======================

This is an *ALPHA* version. More features will be continued to be added until we cut a stable beta build to release candidate against.

Table of content:

[Installation](#installation)    
[Starting and using 1 broker](#starting-and-using-1-broker)    

[Typical Operations](#typical-operations)    
* [Changing the location of data stored](#changing-the-location-of-data-stored)    
* [Starting 3 brokers](#starting-3-brokers)    
* [Flushing scheduler state](#flusing-scheduler-state)


[Navigating the CLI](#navigating-the-cli)    
* [Adding brokers to the cluster](#adding-brokers-to-the-cluster)    
* [Updating broker configurations](#updating-the-broker-configurations)    
* [Starting brokers](#starting-brokers-in-the-cluster-)    
* [Stopping brokers](#stopping-brokers-in-the-cluster)    
* [Removing brokers](#removing-brokers-from-the-cluster)    

[Project Goals](#project-goals)    

Installation
-------------

Install gradle http://gradle.org/installation

    # git clone https://github.com/mesos/kafka
    # cd kafka
    # ./gradlew jar
    # wget https://archive.apache.org/dist/kafka/0.8.1.1/kafka_2.9.2-0.8.1.1.tgz
    # MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so ./kafka-mesos.sh scheduler

Now, in another terminal window (or startup the scheduler on marathon or such).

    # export MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so

Starting and using 1 broker
---------------------------

First lets start up and use 1 broker with the default settings. Further in the readme you can see how to change these from the defaults.

```
# ./kafka-mesos.sh add 0
Broker added

broker:
  id: 0
  started: false
  resources: cpus:1.00, mem:128
```

You now have a cluster with 1 broker that is not started. 

```
# ./kafka-mesos.sh status
Cluster status received

cluster:
  brokers:
    id: 0
    started: false
    resources: cpus:1.00, mem:128

```
Now lets startup the broker.

```
# ./kafka-mesos.sh start 0
Broker 0 started
```

Now, we don't know where the broker is and we need that for producers and consumers to connect to the cluster.

```
# ./kafka-mesos.sh status
Cluster status received

cluster:
  brokers:
    id: 0
    started: true
    resources: cpus:1.00, mem:128
    task: 
      id: Broker-0
      running: true
      endpoint: 172.16.25.62:31000
```

Great!!! Now lets produce and consume from the cluster. Lets use [kafkacat](https://github.com/edenhill/kafkacat) a nice third party c library command line tool for Kafka.

```
# echo "test"|kafkacat -P -b "172.16.25.62:31000" -t testTopic -p 0
```

And lets read it back.

```
# kafkacat -C -b "172.16.25.62:31000" -t testTopic -p 0 -e
test
```

This is an alpha version.    

Typical Operations
===================

Changing the location of data stored
-------------------------------------

```
# ./kafka-mesos.sh stop 0
Broker 0 stopped
# ./kafka-mesos.sh update 0 --options log.dirs=/mnt/array1/broker0
Broker updated

broker:
  id: 0
  started: false
  resources: cpus:1.00, mem:128
  options: log.dirs=/mnt/array1/broker0

# ./kafka-mesos.sh start 0
Broker 0 started
```

Starting 3 brokers
-------------------------

```
#./kafka-mesos.sh add 0..2 --heap 1024 --mem 2048
#./kafka-mesos.sh start 0
#./kafka-mesos.sh start 1
#./kafka-mesos.sh start 2
```

Flushing scheduler state
-------------------------

Currently state is held in a local json file where the scheduler runs.
This will eventually be a plugable interface so you can store it some place else also for HA.

    # rm -f kafka-mesos.json


Navigating the CLI
==================

Adding brokers to the cluster
-------------------------------

```
# ./kafka-mesos.sh help add
Add broker
Usage: add <broker-id-expression>

Expression examples:
  0      - broker 0
  0,1    - brokers 0,1
  0..2   - brokers 0,1,2
  0,1..2 - brokers 0,1,2
  "*"    - any broker

Option           Description                          
------           -----------                          
--attributes     slave attributes (rack:1;role:master)
--cpus <Double>  cpu amount                           
--host           slave hostname                       
--mem <Long>     mem amount                           
--options        kafka options (a=1;b=2) 
```

Updating the broker configurations
-----------------------------------

```
# ./kafka-mesos.sh help update
Update broker
Usage: update <broker-id-expression>

Expression examples:
  0      - broker 0
  0,1    - brokers 0,1
  0..2   - brokers 0,1,2
  0,1..2 - brokers 0,1,2
  "*"    - any broker

Option           Description                          
------           -----------                          
--attributes     slave attributes (rack:1;role:master)
--cpus <Double>  cpu amount                           
--host           slave hostname                       
--mem <Long>     mem amount                           
--options        kafka options (a=1;b=2)              

Note: use "" arg to unset the option
```

Starting brokers in the cluster 
-------------------------------

```
# ./kafka-mesos.sh help start
Start broker
Usage: start <broker-id-expression>

Expression examples:
  0      - broker 0
  0,1    - brokers 0,1
  0..2   - brokers 0,1,2
  0,1..2 - brokers 0,1,2
  "*"    - any broker

Option               Description                           
------               -----------                           
--timeout <Integer>  timeout in seconds. 0 - for no timeout
                       (default: 30) 
```

Stopping brokers in the cluster
-------------------------------

```
# ./kafka-mesos.sh help stop
Stop broker
Usage: stop <broker-id-expression>

Expression examples:
  0      - broker 0
  0,1    - brokers 0,1
  0..2   - brokers 0,1,2
  0,1..2 - brokers 0,1,2
  "*"    - any broker

Option               Description                           
------               -----------                           
--timeout <Integer>  timeout in seconds. 0 - for no timeout
                       (default: 30)   

```

Removing brokers from the cluster
----------------------------------

```
# ./kafka-mesos.sh help remove
Remove broker
Usage: remove <broker-id-expression>

Expression examples:
  0      - broker 0
  0,1    - brokers 0,1
  0..2   - brokers 0,1,2
  0,1..2 - brokers 0,1,2
  "*"    - any broker

```

Project Goals
==============

* smart broker.id assignment.

* preservation of broker placement (through constraints and/or new features).

* ability to-do configuration changes.

* rolling restarts (for things like configuration changes).

* scaling the cluster up and down with automatic, programmatic and manual options.

* smart partition assignmnet via constraints visa vi roles, resources and attributes.


