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
package ly.stealth.mesos.kafka.interface

import ly.stealth.mesos.kafka.interface.impl.AdminUtils
import java.util.Properties
import scala.collection.Map

case class FeatureSupport(
  quotas: Boolean,
  genericEntityConfigs: Boolean
)

trait AdminUtilsProxy {
  def fetchAllTopicConfigs(): Map[String, Properties]

  def createOrUpdateTopicPartitionAssignmentPathInZK(topic: String,
    partitionReplicaAssignment: Map[Int, Seq[Int]],
    config: Properties = new Properties,
    update: Boolean = false)

  def changeTopicConfig(topic: String, configs: Properties)

  def fetchEntityConfig(entityType: String, entity: String): Properties

  def changeClientIdConfig(clientId: String, configs: Properties)

  def fetchAllEntityConfigs(entityType: String): Map[String, Properties]

  def assignReplicasToBrokers(ids: Seq[Int], nPartitions: Int,
    replicationFactor: Int,
    fixedStartIndex: Int = -1,
    startPartitionId: Int = -1): Map[Int, Seq[Int]]

  val features: FeatureSupport
}

object AdminUtilsProxy {
  def apply(zkUrl: String) = new AdminUtils(zkUrl)
}
