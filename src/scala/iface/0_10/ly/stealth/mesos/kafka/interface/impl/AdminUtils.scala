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
package ly.stealth.mesos.kafka.interface.impl

import kafka.utils.{ZkUtils => KafkaZkUtils}
import kafka.admin.{BrokerMetadata, AdminUtils => KafkaAdminUtils}
import java.util.Properties
import ly.stealth.mesos.kafka.interface.{AdminUtilsProxy, FeatureSupport}
import scala.collection.Map


class AdminUtils(zkUrl: String) extends AdminUtilsProxy {
  private val DEFAULT_TIMEOUT_MS = 30000
  private val zkUtils = KafkaZkUtils(zkUrl, DEFAULT_TIMEOUT_MS, DEFAULT_TIMEOUT_MS, isZkSecurityEnabled = false)

  override def fetchAllTopicConfigs(): Map[String, Properties] = KafkaAdminUtils.fetchAllTopicConfigs(zkUtils)

  override def createOrUpdateTopicPartitionAssignmentPathInZK(
    topic: String,
    partitionReplicaAssignment: Map[Int, Seq[Int]],
    config: Properties,
    update: Boolean
  ): Unit = KafkaAdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, partitionReplicaAssignment, config, update)

  override def changeTopicConfig(
    topic: String,
    configs: Properties
  ): Unit = KafkaAdminUtils.changeTopicConfig(zkUtils, topic, configs)

  override def deleteTopic(topicToDelete: String): Unit =
    KafkaAdminUtils.deleteTopic(zkUtils, topicToDelete)

  override def fetchEntityConfig(
    entityType: String,
    entity: String
  ): Properties = KafkaAdminUtils.fetchEntityConfig(zkUtils, entityType, entity)

  override def changeClientIdConfig(
    clientId: String,
    configs: Properties
  ): Unit = KafkaAdminUtils.changeClientIdConfig(zkUtils, clientId, configs)

  override def fetchAllEntityConfigs(entityType: String): Map[String, Properties]
  = KafkaAdminUtils.fetchAllEntityConfigs(zkUtils, entityType)

  override def assignReplicasToBrokers(
    ids: Seq[Int],
    nPartitions: Int,
    replicationFactor: Int,
    fixedStartIndex: Int,
    startPartitionId: Int
  ): Map[Int, Seq[Int]] = {
    val md = ids.map(BrokerMetadata(_, None))
    KafkaAdminUtils.assignReplicasToBrokers(md, nPartitions, replicationFactor, fixedStartIndex, startPartitionId)
  }

  override val features: FeatureSupport = FeatureSupport(quotas = true, genericEntityConfigs = true)
}
