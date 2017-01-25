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
package ly.stealth.mesos.kafka

import ly.stealth.mesos.kafka.Broker.Metrics
import scala.collection.{Map, Seq}

case class BrokerStatusResponse(brokers: Seq[Broker])
case class BrokerStartResponse(brokers: Seq[Broker], status: String, message: Option[String] = None)
case class BrokerRemoveResponse(ids: Seq[String])

case class FrameworkMessage(metrics: Option[Metrics] = None, log: Option[LogResponse] = None)
case class ListTopicsResponse(topics: Seq[Topic])

case class HttpLogResponse(status: String, content: String)
case class RebalanceStartResponse(status: String, state: String, error: Option[String])
case class HttpErrorResponse(code: Int, error: String)


case class Partition(
  id: Int = 0,
  replicas: Seq[Int] = null,
  isr: Seq[Int] = null,
  leader: Int = 0,
  expectedLeader: Int = 0)

case class Topic(
  name: String = null,
  partitions: Map[Int, Seq[Int]] = Map(),
  options: Map[String, String] = Map()
) {
  def partitionsState: String = {
    var s: String = ""
    for ((partition, brokers) <- partitions.toSeq.sortBy(_._1)) {
      if (!s.isEmpty) s += ", "
      s += partition + ":[" + brokers.mkString(",") + "]"
    }
    s
  }
}