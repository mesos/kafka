Kafka Mesos Framework
======================

This is an *ALPHA* version. More features will be continued to be added until we cut a stable beta build to release candidate against.

[Installation](#installation)    
[Configuration](#configuration)    
[Run the scheduler](#run-the-scheduler)    
[Starting and using 1 broker](#starting-and-using-1-broker)    

[Typical Operations](#typical-operations)
* [Changing the location of data stored](#changing-the-location-of-data-stored)
* [Starting 3 brokers](#starting-3-brokers)
* [Flushing scheduler state](#flusing-scheduler-state)
* [Failed Broker Recovery](#failed-broker-recovery)


[Navigating the CLI](#navigating-the-cli)
* [Adding brokers to the cluster](#adding-brokers-to-the-cluster)
* [Updating broker configurations](#updating-the-broker-configurations)
* [Starting brokers](#starting-brokers-in-the-cluster-)
* [Stopping brokers](#stopping-brokers-in-the-cluster)
* [Removing brokers](#removing-brokers-from-the-cluster)

[Using the REST API](#using-the-rest-api)    

[Project Goals](#project-goals)

Installation
-------------

Install gradle http://gradle.org/installation

Clone and build the project

    # git clone https://github.com/mesos/kafka
    # cd kafka
    # ./gradlew jar
    # wget https://archive.apache.org/dist/kafka/0.8.1.1/kafka_2.9.2-0.8.1.1.tgz

Configuration
-------------

Edit `kafka-mesos.properties` and make the right settings for your environment.
Before running `./kafka-mesos.sh`, set the location of libmesos:

    # export MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so

Run the scheduler
-----------------

Start the Kafka scheduler using this command:

    # MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so ./kafka-mesos.sh scheduler

You can use Marathon to launch the scheduler process so it gets restarted if it crashes.

Starting and using 1 broker
---------------------------

First lets start up and use 1 broker with the default settings. Further in the readme you can see how to change these from the defaults.

```
# ./kafka-mesos.sh add 0
Broker added

broker:
  id: 0
  active: false
  state: stopped
  resources: cpus:1.00, mem:128, heap:128
  failover: delay:10s, maxDelay:60s
```

You now have a cluster with 1 broker that is not started.

```
# ./kafka-mesos.sh status
Cluster status received

cluster:
  brokers:
    id: 0
    active: false
    state: stopped
    resources: cpus:1.00, mem:128, heap:128
    failover: delay:10s, maxDelay:60s
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
    active: true
    state: running
    resources: cpus:1.00, mem:128, heap:128
    failover: delay:10s, maxDelay:60s
    task:
      id: broker-0-d2d94520-2f3e-4779-b276-771b4843043c
      running: true
      endpoint: 172.16.25.62:31000
      attributes: rack=r1
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
  active: false
  state: stopped
  resources: cpus:1.00, mem:128, heap:128
  options: log.dirs=/mnt/array1/broker0
  failover: delay:10s, maxDelay:60s

# ./kafka-mesos.sh start 0
Broker 0 started
```

Starting 3 brokers
-------------------------

```
#./kafka-mesos.sh add 0..2 --heap 1024 --mem 2048
Brokers added

brokers:
  id: 0
  active: false
  state: stopped
  resources: cpus:1.00, mem:2048, heap:1024
  failover: delay:10s, maxDelay:60s

  id: 1
  active: false
  state: stopped
  resources: cpus:1.00, mem:2048, heap:1024
  failover: delay:10s, maxDelay:60s

  id: 2
  active: false
  state: stopped
  resources: cpus:1.00, mem:2048, heap:1024
  failover: delay:10s, maxDelay:60s

#./kafka-mesos.sh start 0
Broker 0 started
#./kafka-mesos.sh start 1
Broker 1 started
#./kafka-mesos.sh start 2
Broker 2 started
```

Flushing scheduler state
-------------------------

Currently state is held in a local json file where the scheduler runs.
This will eventually be a plugable interface so you can store it some place else also for HA.

    # rm -f kafka-mesos.json

Failed Broker Recovery
------------------------
When the broker fails, kafka mesos scheduler assumes that the failure is recoverable. Scheduler will try
to restart broker after waiting failoverDelay (i.e. 30s, 2m) on any matched slave. Initially waiting
delay is equal to failoverDelay setting. After each serial failure it doubles until it reaches failoverMaxDelay value.

If failoverMaxTries is defined and serial failure count exceeds it, broker will be deactivated.

Following failover settings exists:
```
--failoverDelay    - initial failover delay to wait after failure, required
--failoverMaxDelay - max failover delay, required
--failoverMaxTries - max failover tries to deactivate broker, optional
```

Navigating the CLI
==================

Adding brokers to the cluster
-------------------------------

```
# ./kafka-mesos.sh help add
Add broker
Usage: add <id-expr> [options]

Option              Description
------              -----------
--constraints       constraints (hostname=like:master,
                      rack=like:1.*). See below.
--cpus <Double>     cpu amount (0.5, 1, 2)
--failoverDelay     failover delay (10s, 5m, 3h)
--failoverMaxDelay  max failover delay. See failoverDelay.
--failoverMaxTries  max failover tries
--heap <Long>       heap amount in Mb
--mem <Long>        mem amount in Mb
--options           kafka options (log.dirs=/tmp/kafka/$id,
                      num.io.threads=16)
id-expr examples:
  0      - broker 0
  0,1    - brokers 0,1
  0..2   - brokers 0,1,2
  0,1..2 - brokers 0,1,2
  *      - any broker

constraint examples:
  like:master     - value equals 'master'
  unlike:master   - value not equals 'master'
  like:slave.*    - value starts with 'slave'
  unique          - all values are unique
  cluster         - all values are the same
  cluster:master  - value equals 'master'
  groupBy         - all values are the same
  groupBy:3       - all values are within 3 different groups
```

Updating broker configurations
-----------------------------------

```
# ./kafka-mesos.sh help update
Update broker
Usage: update <id-expr> [options]

Option              Description
------              -----------
--constraints       constraints (hostname=like:master,
                      rack=like:1.*). See below.
--cpus <Double>     cpu amount (0.5, 1, 2)
--failoverDelay     failover delay (10s, 5m, 3h)
--failoverMaxDelay  max failover delay. See failoverDelay.
--failoverMaxTries  max failover tries
--heap <Long>       heap amount in Mb
--mem <Long>        mem amount in Mb
--options           kafka options (log.dirs=/tmp/kafka/$id,
                      num.io.threads=16)
id-expr examples:
  0      - broker 0
  0,1    - brokers 0,1
  0..2   - brokers 0,1,2
  0,1..2 - brokers 0,1,2
  *      - any broker

constraint examples:
  like:master     - value equals 'master'
  unlike:master   - value not equals 'master'
  like:slave.*    - value starts with 'slave'
  unique          - all values are unique
  cluster         - all values are the same
  cluster:master  - value equals 'master'
  groupBy         - all values are the same
  groupBy:3       - all values are within 3 different groups

Note: use "" arg to unset the option
```

Starting brokers in the cluster
-------------------------------

```
# ./kafka-mesos.sh help start
Start broker
Usage: start <id-expr> [options]

Option     Description
------     -----------
--timeout  timeout (30s, 1m, 1h). 0s - no timeout

id-expr examples:
  0      - broker 0
  0,1    - brokers 0,1
  0..2   - brokers 0,1,2
  0,1..2 - brokers 0,1,2
  *      - any broker
```

Stopping brokers in the cluster
-------------------------------

```
# ./kafka-mesos.sh help stop
Stop broker
Usage: stop <id-expr> [options]

Option     Description
------     -----------
--timeout  timeout (30s, 1m, 1h). 0s - no timeout
--force    forcibly stop

id-expr examples:
  0      - broker 0
  0,1    - brokers 0,1
  0..2   - brokers 0,1,2
  0,1..2 - brokers 0,1,2
  *      - any broker
```

Removing brokers from the cluster
----------------------------------

```
# ./kafka-mesos.sh help remove
Remove broker
Usage: remove <id-expr>

id-expr examples:
  0      - broker 0
  0,1    - brokers 0,1
  0..2   - brokers 0,1,2
  0,1..2 - brokers 0,1,2
  *      - any broker
```

Rebalancing brokers in the cluster
----------------------------------
```
# ./kafka-mesos.sh help rebalance
Rebalance
Usage: rebalance <id-expr>|status [options]

Option     Description
------     -----------
--timeout  timeout (30s, 1m, 1h). 0s - no timeout
--topics   <topic-expr>. Default - *. See below.

topic-expr examples:
  t0        - topic t0 with default RF (replication-factor)
  t0,t1     - topics t0, t1 with default RF
  t0:3      - topic t0 with RF=3
  t0,t1:2   - topic t0 with default RF, topic t1 with RF=2
  *         - all topics with default RF
  *:2       - all topics with RF=2
  t0:1,*:2  - all topics with RF=2 except topic t0 with RF=1

id-expr examples:
  0      - broker 0
  0,1    - brokers 0,1
  0..2   - brokers 0,1,2
  0,1..2 - brokers 0,1,2
  *      - any broker
```

Using the REST API
========================

The scheduler REST API fully exposes all features of the CLI using following request format:
```
/api/brokers/<cli command>/id={broker.id}&<setting>=<value>
```

Adding a broker

```
# curl "http://localhost:7000/api/brokers/add?id=0&cpus=8&mem=43008"
{"brokers" : [{"id" : "0", "mem" : 43008, "cpus" : 8.0, "heap" : 128, "failover" : {"delay" : "10s", "maxDelay" : "60s"}, "active" : false}]}
```

Starting a broker

```
# curl "http://localhost:7000/api/brokers/start?id=0"
{"success" : true, "ids" : "0"}
```

Stopping a broker

```
# curl"http://localhost:7000/api/brokers/stop?id=0"
{"success" : true, "ids" : "0"}
```

Status

```
# curl "http://localhost:7000/api/brokers/status?id=0"
{"brokers" : [{"id" : "0", "mem" : 128, "cpus" : 0.1, "heap" : 128, "failover" : {"delay" : "10s", "maxDelay" : "60s", "failures" : 5, "failureTime" : 1426651240585}, "active" : true}, {"id" : "5", "mem" : 128, "cpus" : 0.5, "heap" : 128, "failover" : {"delay" : "10s", "maxDelay" : "60s"}, "active" : false}, {"id" : "8", "mem" : 43008, "cpus" : 8.0, "heap" : 128, "failover" : {"delay" : "10s", "maxDelay" : "60s"}, "active" : true}]}
```

Project Goals
==============

* smart broker.id assignment.

* preservation of broker placement (through constraints and/or new features).

* ability to-do configuration changes.

* rolling restarts (for things like configuration changes).

* scaling the cluster up and down with automatic, programmatic and manual options.

* smart partition assignment via constraints visa vi roles, resources and attributes.
