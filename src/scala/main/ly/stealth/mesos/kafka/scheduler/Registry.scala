/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ly.stealth.mesos.kafka.scheduler

import ly.stealth.mesos.kafka.scheduler.http.api._
import ly.stealth.mesos.kafka.scheduler.http.{HttpServerComponent, HttpServerComponentImpl}
import ly.stealth.mesos.kafka.scheduler.mesos._
import ly.stealth.mesos.kafka.{ClockComponent, Cluster, WallClockComponent}

trait HttpApiComponent
  extends BrokerApiComponent
    with TopicApiComponent
    with PartitionApiComponent
    with QuotaApiComponent

trait Registry
  extends ClusterComponent
  with TaskReconcilerComponent
  with SchedulerComponent
  with OfferManagerComponent
  with HttpServerComponent
  with KafkaDistributionComponent
  with MesosTaskFactoryComponent
  with BrokerLogManagerComponent
  with SchedulerDriverComponent
  with BrokerLifecycleManagerComponent
  with ClockComponent
  with BrokerTaskManagerComponent
  with HttpApiComponent
  with EventLoopComponent
{

}

class ProductionRegistry
  extends Registry
    with ClusterComponent
    with OfferManagerComponentImpl
    with TaskReconcilerComponentImpl
    with HttpServerComponentImpl
    with SchedulerComponentImpl
    with KafkaDistributionComponentImpl
    with MesosTaskFactoryComponentImpl
    with BrokerLogManagerComponentImpl
    with BrokerLifecycleManagerComponentImpl
    with WallClockComponent
    with BrokerTaskManagerComponentImpl
    with BrokerApiComponentImpl
    with TopicApiComponentImpl
    with PartitionApiComponentImpl
    with QuotaApiComponentImpl
    with DefaultEventLoopComponent
{
  val cluster = Cluster.load()
}