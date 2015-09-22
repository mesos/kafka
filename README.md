Kafka Mesos Framework
======================

For issues https://github.com/mesos/kafka/issues

[Installation](#installation)    
* [Environment Configuration](#environment-configuration)
* [Scheduler Configuration](#scheduler-configuration)
* [Run the scheduler](#run-the-scheduler)
* [Starting and using 1 broker](#starting-and-using-1-broker)

[Typical Operations](#typical-operations)
* [Run the scheduler with Docker](https://github.com/mesos/kafka/tree/master/src/docker#intro)   
* [Run the scheduler on Marathon](https://github.com/mesos/kafka/tree/master/src/docker#running-image-in-marathon)  
* [Changing the location where data is stored](#changing-the-location-where-data-is-stored)
* [Starting 3 brokers](#starting-3-brokers)
* [High Availability Scheduler State](#high-availability-scheduler-state)
* [Failed Broker Recovery](#failed-broker-recovery)
* [Passing multiple options](#passing-multiple-options)


[Navigating the CLI](#navigating-the-cli)
* [Adding brokers to the cluster](#adding-brokers-to-the-cluster)
* [Updating broker configurations](#updating-broker-configurations)
* [Starting brokers](#starting-brokers-in-the-cluster)
* [Stopping brokers](#stopping-brokers-in-the-cluster)
* [Removing brokers](#removing-brokers-from-the-cluster)
* [Rebalancing brokers in the cluster](#rebalancing-topics)
* [Listing topics](#listing-topics)
* [Adding topic](#adding-topic)
* [Updating topic](#updating-topic)

[Using the REST API](#using-the-rest-api)    

[Project Goals](#project-goals)

Installation
-------------

Install OpenJDK 7 (or higher) http://openjdk.java.net/install/

Install gradle http://gradle.org/installation

Clone and build the project

    # git clone https://github.com/mesos/kafka
    # cd kafka
    # ./gradlew jar
    # wget https://archive.apache.org/dist/kafka/0.8.2.1/kafka_2.10-0.8.2.1.tgz

Environment Configuration
--------------------------

Before running `./kafka-mesos.sh`, set the location of libmesos:

    # export MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so

If the host running scheduler has several IP addresses you may also need to

    # export LIBPROCESS_IP=<IP_ACCESSIBLE_FROM_MASTER>

Scheduler Configuration
----------------------

The scheduler is configured through the command line or `kafka-mesos.properties` file.

The following options are available:
```
# ./kafka-mesos.sh help scheduler
Start scheduler 
Usage: scheduler [options] [config.properties]

Option               Description
------               -----------
--api                Api url. Example: http://master:7000
--bind-address       Scheduler bind address (master, 0.0.0.0, 192.168.50.*, if:eth1). Default - all
--debug <Boolean>    Debug mode. Default - false
--framework-name     Framework name. Default - kafka
--framework-role     Framework role. Default - *
--framework-timeout  Framework timeout (30s, 1m, 1h). Default - 30d
--jre                JRE zip-file (jre-7-openjdk.zip). Default - none.
--log                Log file to use. Default - stdout.
--master             Master connection settings. Examples:
                      - master:5050
                      - master:5050,master2:5050
                      - zk://master:2181/mesos
                      - zk://username:password@master:2181
                      - zk://master:2181,master2:2181/mesos
--principal          Principal (username) used to register framework. Default - none
--secret             Secret (password) used to register framework. Default - none
--storage            Storage for cluster state. Examples:
                      - file:kafka-mesos.json
                      - zk:/kafka-mesos
                     Default - file:kafka-mesos.json
--user               Mesos user to run tasks. Default - none
--zk                 Kafka zookeeper.connect. Examples:
                      - master:2181
                      - master:2181,master2:2181
```

Additionally you can create `kafka-mesos.properties` containing values for CLI options of scheduler.

Example of `kafka-mesos.properties`:
```
storage=file:kafka-mesos.json
master=zk://master:2181/mesos
zk=master:2181
api=http://master:7000
```

Now if running scheduler via `./kafka-mesos.sh scheduler` (no options specified) the scheduler will read values for options
from the above file. You could also specify alternative config file by using `config` argument of the scheduler.

Run the scheduler
-----------------

Start the Kafka scheduler using this command:

    # ./kafka-mesos.sh scheduler

Note: you can also use Marathon to launch the scheduler process so it gets restarted if it crashes.

Starting and using 1 broker
---------------------------

First let's start up and use 1 broker with the default settings. Further in the readme you can see how to change these from the defaults.

```
# ./kafka-mesos.sh broker add 0
broker added:
  id: 0
  active: false
  state: stopped
  resources: cpus:1.00, mem:2048, heap:1024, port:auto
  failover: delay:1m, max-delay:10m
  stickiness: period:10m
```

You now have a cluster with 1 broker that is not started.

```
# ./kafka-mesos.sh broker list
broker:
  id: 0
  active: false
  state: stopped
  resources: cpus:1.00, mem:2048, heap:1024, port:auto
  failover: delay:1m, max-delay:10m
  stickiness: period:10m
```
Now let's start the broker.

```
# ./kafka-mesos.sh broker start 0
broker started:
  id: 0
  active: true
  state: running
  resources: cpus:1.00, mem:2048, heap:1024, port:auto
  failover: delay:1m, max-delay:10m
  stickiness: period:10m, hostname:slave0
  task:
    id: broker-0-d2d94520-2f3e-4779-b276-771b4843043c
    running: true
    endpoint: 172.16.25.62:31000
    attributes: rack=r1
```

Great! Now let's produce and consume from the cluster. Let's use [kafkacat](https://github.com/edenhill/kafkacat), a nice third party c library command line tool for Kafka.

```
# echo "test"|kafkacat -P -b "172.16.25.62:31000" -t testTopic -p 0
```

And let's read it back.

```
# kafkacat -C -b "172.16.25.62:31000" -t testTopic -p 0 -e
test
```

This is a beta version.

Typical Operations
===================

Changing the location where data is stored
------------------------------------------

```
# ./kafka-mesos.sh broker stop 0
broker stopped:
  id: 0
  active: false
  state: stopped
  resources: cpus:1.00, mem:2048, heap:1024, port:auto
  failover: delay:1m, max-delay:10m
  stickiness: period:10m, hostname:slave0, expires:2015-07-10 15:51:43+03

# ./kafka-mesos.sh broker update 0 --options log.dirs=/mnt/array1/broker0
broker updated:
  id: 0
  active: false
  state: stopped
  resources: cpus:1.00, mem:2048, heap:1024, port:auto
  options: log.dirs=/mnt/array1/broker0
  failover: delay:1m, max-delay:10m
  stickiness: period:10m, hostname:slave0, expires:2015-07-10 15:51:43+03

# ./kafka-mesos.sh broker start 0
broker started:
  id: 0
  active: true
  state: running
  resources: cpus:1.00, mem:2048, heap:1024, port:auto
  failover: delay:1m, max-delay:10m
  stickiness: period:10m, hostname:slave0
  task:
    id: broker-0-d2d94520-2f3e-4779-b276-771b4843043c
    running: true
    endpoint: 172.16.25.62:31000
    attributes: rack=r1
```

Starting 3 brokers
-------------------------

```
#./kafka-mesos.sh broker add 0..2 --heap 1024 --mem 2048
brokers added:
  id: 0
  active: false
  state: stopped
  resources: cpus:1.00, mem:2048, heap:1024, port:auto
  failover: delay:1m, max-delay:10m
  stickiness: period:10m

  id: 1
  active: false
  state: stopped
  resources: cpus:1.00, mem:2048, heap:1024, port:auto
  failover: delay:1m, max-delay:10m
  stickiness: period:10m

  id: 2
  active: false
  state: stopped
  resources: cpus:1.00, mem:2048, heap:1024, port:auto
  failover: delay:1m, max-delay:10m
  stickiness: period:10m

#./kafka-mesos.sh broker start 0..2
brokers started:
  id: 0
  active: true
  state: running
  resources: cpus:1.00, mem:2048, heap:1024, port:auto
  failover: delay:1m, max-delay:10m
  stickiness: period:10m, hostname:slave0
  task:
    id: broker-0-d2d94520-2f3e-4779-b276-771b4843043c
    running: true
    endpoint: 172.16.25.62:31000
    attributes: rack=r1

  id: 1
  active: true
  state: running
  ...
```

High Availability Scheduler State
-------------------------
The scheduler supports storing the cluster state in Zookeeper. It currently shares a znode within the mesos ensemble. To turn this on in properties 

```
clusterStorage=zk:/kafka-mesos
```

Failed Broker Recovery
------------------------
When a broker fails, kafka mesos scheduler assumes that the failure is recoverable. The scheduler will try
to restart the broker after waiting failover-delay (i.e. 30s, 2m). The initial waiting delay is equal to failover-delay setting.
After each consecutive failure this delay is doubled until it reaches failover-max-delay value.

If failover-max-tries is defined and the consecutive failure count exceeds it, the broker will be deactivated.

The following failover settings exists:
```
--failover-delay     - initial failover delay to wait after failure, required
--failover-max-delay - max failover delay, required
--failover-max-tries - max failover tries to deactivate broker, optional
```

Broker Placement Stickiness
---------------------------
If a broker is started within a stickiness-period interval from it's stop time, the scheduler will place it on the same node
it was on during the last successful start. This applies both to failover and manual restarts.

The following stickiness settings exists:
```
--stickiness-period  - period of time during which broker would be restarted on the same node
```

Passing multiple options
-----------------------
A common use case is to supply multiple `log.dirs`, or provide other options. To do this you may use comma escaping like this:

```
./kafka-mesos.sh broker update 0 --options log.dirs=/mnt/array1/broker0\\,/mnt/array2/broker0,num.io.threads=16
broker updated:
  id: 0
  active: false
  state: stopped
  resources: cpus:1.00, mem:2048, heap:1024, port:auto
  options: log.dirs=/mnt/array1/broker0\,/mnt/array2/broker0,num.io.threads=16
  failover: delay:1m, max-delay:10m
  stickiness: period:10m, hostname:slave0, expires:2015-07-29 11:54:39Z
```


Navigating the CLI
==================

Adding brokers to the cluster
-------------------------------

```
# ./kafka-mesos.sh help broker add
Add broker
Usage: broker add <broker-expr> [options]

Option                Description
------                -----------
--bind-address        broker bind address (broker0, 192.168.50.*, if:eth1). Default - auto
--constraints         constraints (hostname=like:master,rack=like:1.*). See below.
--cpus <Double>       cpu amount (0.5, 1, 2)
--failover-delay      failover delay (10s, 5m, 3h)
--failover-max-delay  max failover delay. See failoverDelay.
--failover-max-tries  max failover tries. Default - none
--heap <Long>         heap amount in Mb
--jvm-options         jvm options string (-Xms128m -XX:PermSize=48m)
--log4j-options       log4j options or file. Examples:
                       log4j.logger.kafka=DEBUG\, kafkaAppender
                       file:log4j.properties
--mem <Long>          mem amount in Mb
--options             options or file. Examples:
                       log.dirs=/tmp/kafka/$id,num.io.threads=16
                       file:server.properties
--port                port or range (31092, 31090..31100). Default - auto
--stickiness-period   stickiness period to preserve same node for broker (5m, 10m, 1h)

Generic Options
Option  Description
------  -----------
--api   Api url. Example: http://master:7000

broker-expr examples:
  0      - broker 0
  0,1    - brokers 0,1
  0..2   - brokers 0,1,2
  0,1..2 - brokers 0,1,2
  *      - any broker
attribute filtering:
  *[rack=r1]           - any broker having rack=r1
  *[hostname=slave*]   - any broker on host with name starting with 'slave'
  0..4[rack=r1,dc=dc1] - any broker having rack=r1 and dc=dc1

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
# ./kafka-mesos.sh help broker update
Update broker
Usage: broker update <broker-expr> [options]

Option                Description
------                -----------
--bind-address        broker bind address (broker0, 192.168.50.*, if:eth1). Default - auto
--constraints         constraints (hostname=like:master,rack=like:1.*). See below.
--cpus <Double>       cpu amount (0.5, 1, 2)
--failover-delay      failover delay (10s, 5m, 3h)
--failover-max-delay  max failover delay. See failoverDelay.
--failover-max-tries  max failover tries. Default - none
--heap <Long>         heap amount in Mb
--jvm-options         jvm options string (-Xms128m -XX:PermSize=48m)
--log4j-options       log4j options or file. Examples:
                       log4j.logger.kafka=DEBUG\, kafkaAppender
                       file:log4j.properties
--mem <Long>          mem amount in Mb
--options             options or file. Examples:
                       log.dirs=/tmp/kafka/$id,num.io.threads=16
                       file:server.properties
--port                port or range (31092, 31090..31100). Default - auto
--stickiness-period   stickiness period to preserve same node for broker (5m, 10m, 1h)

Generic Options
Option  Description
------  -----------
--api   Api url. Example: http://master:7000

broker-expr examples:
  0      - broker 0
  0,1    - brokers 0,1
  0..2   - brokers 0,1,2
  0,1..2 - brokers 0,1,2
  *      - any broker
attribute filtering:
  *[rack=r1]           - any broker having rack=r1
  *[hostname=slave*]   - any broker on host with name starting with 'slave'
  0..4[rack=r1,dc=dc1] - any broker having rack=r1 and dc=dc1

constraint examples:
  like:master     - value equals 'master'
  unlike:master   - value not equals 'master'
  like:slave.*    - value starts with 'slave'
  unique          - all values are unique
  cluster         - all values are the same
  cluster:master  - value equals 'master'
  groupBy         - all values are the same
  groupBy:3       - all values are within 3 different groups

Note: use "" arg to unset an option
```

Starting brokers in the cluster
-------------------------------

```
# ./kafka-mesos.sh help broker start
Start broker
Usage: broker start <broker-expr> [options]

Option     Description
------     -----------
--timeout  timeout (30s, 1m, 1h). 0s - no timeout

Generic Options
Option  Description
------  -----------
--api   Api url. Example: http://master:7000

broker-expr examples:
  0      - broker 0
  0,1    - brokers 0,1
  0..2   - brokers 0,1,2
  0,1..2 - brokers 0,1,2
  *      - any broker
attribute filtering:
  *[rack=r1]           - any broker having rack=r1
  *[hostname=slave*]   - any broker on host with name starting with 'slave'
  0..4[rack=r1,dc=dc1] - any broker having rack=r1 and dc=dc1
```

Stopping brokers in the cluster
-------------------------------

```
# ./kafka-mesos.sh help broker stop
Stop broker
Usage: broker stop <broker-expr> [options]

Option     Description
------     -----------
--force    forcibly stop
--timeout  timeout (30s, 1m, 1h). 0s - no timeout

Generic Options
Option  Description
------  -----------
--api   Api url. Example: http://master:7000

broker-expr examples:
  0      - broker 0
  0,1    - brokers 0,1
  0..2   - brokers 0,1,2
  0,1..2 - brokers 0,1,2
  *      - any broker
attribute filtering:
  *[rack=r1]           - any broker having rack=r1
  *[hostname=slave*]   - any broker on host with name starting with 'slave'
  0..4[rack=r1,dc=dc1] - any broker having rack=r1 and dc=dc1
```

Removing brokers from the cluster
----------------------------------

```
# ./kafka-mesos.sh help broker remove
Remove broker
Usage: broker remove <broker-expr> [options]

Generic Options
Option  Description
------  -----------
--api   Api url. Example: http://master:7000

broker-expr examples:
  0      - broker 0
  0,1    - brokers 0,1
  0..2   - brokers 0,1,2
  0,1..2 - brokers 0,1,2
  *      - any broker
attribute filtering:
  *[rack=r1]           - any broker having rack=r1
  *[hostname=slave*]   - any broker on host with name starting with 'slave'
  0..4[rack=r1,dc=dc1] - any broker having rack=r1 and dc=dc1
```

Listing Topics
--------------
```
#./kafka-mesos.sh help topic list
List topics
Usage: topic list [<topic-expr>]

Generic Options
Option  Description
------  -----------
--api   Api url. Example: http://master:7000

topic-expr examples:
  t0        - topic t0
  t0,t1     - topics t0, t1
  *         - any topic
  t*        - topics starting with 't'
```

Adding Topic
------------
```
#./kafka-mesos.sh help topic add
Add topic
Usage: topic add <topic-expr> [options]

Option                  Description
------                  -----------
--broker                <broker-expr>. Default - *. See below.
--options               topic options. Example: flush.ms=60000,retention.ms=6000000
--partitions <Integer>  partitions count. Default - 1
--replicas <Integer>    replicas count. Default - 1

Generic Options
Option  Description
------  -----------
--api   Api url. Example: http://master:7000

topic-expr examples:
  t0        - topic t0
  t0,t1     - topics t0, t1
  *         - any topic
  t*        - topics starting with 't'

broker-expr examples:
  0      - broker 0
  0,1    - brokers 0,1
  0..2   - brokers 0,1,2
  0,1..2 - brokers 0,1,2
  *      - any broker
attribute filtering:
  *[rack=r1]           - any broker having rack=r1
  *[hostname=slave*]   - any broker on host with name starting with 'slave'
  0..4[rack=r1,dc=dc1] - any broker having rack=r1 and dc=dc1
```

Updating Topic
--------------
```
#./kafka-mesos.sh help topic update
Update topic
Usage: topic update <topic-expr> [options]

Option     Description
------     -----------
--options  topic options. Example: flush.ms=60000,retention.ms=6000000

Generic Options
Option  Description
------  -----------
--api   Api url. Example: http://master:7000

topic-expr examples:
  t0        - topic t0
  t0,t1     - topics t0, t1
  *         - any topic
  t*        - topics starting with 't'
```

Rebalancing topics
----------------------------------
```
#./kafka-mesos.sh help topic rebalance
Rebalance topics
Usage: topic rebalance <topic-expr>|status [options]

Option                Description
------                -----------
--broker              <broker-expr>. Default - *. See below.
--replicas <Integer>  replicas count. Default - 1
--timeout             timeout (30s, 1m, 1h). 0s - no timeout

Generic Options
Option  Description
------  -----------
--api   Api url. Example: http://master:7000

topic-expr examples:
  t0        - topic t0
  t0,t1     - topics t0, t1
  *         - any topic
  t*        - topics starting with 't'

broker-expr examples:
  0      - broker 0
  0,1    - brokers 0,1
  0..2   - brokers 0,1,2
  0,1..2 - brokers 0,1,2
  *      - any broker
attribute filtering:
  *[rack=r1]           - any broker having rack=r1
  *[hostname=slave*]   - any broker on host with name starting with 'slave'
  0..4[rack=r1,dc=dc1] - any broker having rack=r1 and dc=dc1
```

Using the REST API
========================

The scheduler REST API fully exposes all of the features of the CLI with the following request format:
```
/api/broker/<cli command>/broker={broker-expr}&<setting>=<value>
/api/topic/<cli command>/topic={topic-expr}&<setting>=<value>
```

Listing brokers

```
# curl "http://localhost:7000/api/broker/list"
{"brokers" : [{"id" : "0", "mem" : 128, "cpus" : 0.1, "heap" : 128, "failover" : {"delay" : "10s", "maxDelay" : "60s", "failures" : 5, "failureTime" : 1426651240585}, "active" : true}, {"id" : "5", "mem" : 128, "cpus" : 0.5, "heap" : 128, "failover" : {"delay" : "10s", "maxDelay" : "60s"}, "active" : false}, {"id" : "8", "mem" : 43008, "cpus" : 8.0, "heap" : 128, "failover" : {"delay" : "10s", "maxDelay" : "60s"}, "active" : true}]}
```

Adding a broker

```
# curl "http://localhost:7000/api/broker/add?broker=0&cpus=8&mem=43008"
{"brokers" : [{"id" : "0", "mem" : 43008, "cpus" : 8.0, "heap" : 128, "failover" : {"delay" : "10s", "maxDelay" : "60s"}, "active" : false}]}
```

Starting a broker

```
# curl "http://localhost:7000/api/broker/start?broker=0"
{"success" : true, "ids" : "0"}
```

Stopping a broker

```
# curl "http://localhost:7000/api/broker/stop?broker=0"
{"success" : true, "ids" : "0"}
```

Listing topics
```
# curl "http://localhost:7000/api/topic/list"
{"topics" : [{"name" : "t", "partitions" : {"0" : "0, 1"}, "options" : {"flush.ms": "1000"}}]}
```

Adding topic
```
# curl "http://localhost:7000/api/topic/add?topic=t"
{"topic" : {"name" : "t", "partitions" : {"0" : "1"}, "options" : {}}}
```

Updating topic
```
# curl "http://localhost:7000/api/topic/update?topic=t&options=flush.ms%3D1000"
{"topic" : {"name" : "t", "partitions" : {"0" : "0, 1"}, "options" : {"flush.ms" : "1000"}}}
```

Project Goals
==============

* smart broker.id assignment.

* preservation of broker placement (through constraints and/or new features).

* ability to-do configuration changes.

* rolling restarts (for things like configuration changes).

* scaling the cluster up and down with automatic, programmatic and manual options.

* smart partition assignment via constraints visa vi roles, resources and attributes.
